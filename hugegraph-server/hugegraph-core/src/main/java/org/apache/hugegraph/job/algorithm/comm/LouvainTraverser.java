/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.job.algorithm.comm;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.iterator.ListIterator;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm.AlgoTraverser;
import org.apache.hugegraph.job.algorithm.Consumers;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class LouvainTraverser extends AlgoTraverser {

    public static final String C_PASS = "c_pass-";
    public static final String C_KIN = "c_kin";
    public static final String C_WEIGHT = "c_weight";
    public static final String C_MEMBERS = "c_members";

    private static final long LIMIT = AbstractAlgorithm.MAX_QUERY_LIMIT;
    private static final int MAX_COMM_SIZE = 100000; // 10w

    private static final Logger LOG = Log.logger(LouvainTraverser.class);

    private final GraphTraversalSource g;
    private final String sourceLabel;
    private final String sourceCLabel;
    private final long degree;
    private final boolean skipIsolated;

    private final Cache cache;

    private long m;
    private String passLabel;

    public LouvainTraverser(UserJob<Object> job, int workers, long degree,
                            String sourceLabel, String sourceCLabel,
                            boolean skipIsolated) {
        super(job, LouvainAlgorithm.ALGO_NAME, workers);
        this.g = this.graph().traversal();
        this.sourceLabel = sourceLabel;
        this.sourceCLabel = sourceCLabel;
        this.degree = degree;
        this.skipIsolated = skipIsolated;
        this.m = 1L;
        this.passLabel = "";

        this.cache = new Cache();
    }

    private void defineSchemaOfPk() {
        String label = this.labelOfPassN(0);
        if (this.graph().existsVertexLabel(label) ||
            this.graph().existsEdgeLabel(label)) {
            throw new IllegalArgumentException(
                    "Please clear historical results before proceeding");
        }

        SchemaManager schema = this.graph().schema();
        schema.propertyKey(C_KIN).asInt()
              .ifNotExist().create();
        schema.propertyKey(C_MEMBERS).valueSet().asText()
              .ifNotExist().create();
        schema.propertyKey(C_WEIGHT).asFloat()
              .ifNotExist().create();

        this.m = this.g.E().count().next();
    }

    private void defineSchemaOfPassN(int pass) {
        this.passLabel = labelOfPassN(pass);

        SchemaManager schema = this.graph().schema();
        try {
            schema.vertexLabel(this.passLabel).useCustomizeStringId()
                  .properties(C_KIN, C_MEMBERS, C_WEIGHT)
                  .create();
            schema.edgeLabel(this.passLabel)
                  .sourceLabel(this.passLabel)
                  .targetLabel(this.passLabel)
                  .properties(C_WEIGHT)
                  .create();
        } catch (ExistedException e) {
            throw new IllegalArgumentException(
                    "Please clear historical results before proceeding", e);
        }
    }

    private List<String> cpassEdgeLabels() {
        List<String> names = new ArrayList<>();
        for (SchemaLabel label : this.graph().schema().getEdgeLabels()) {
            String name = label.name();
            if (name.startsWith(C_PASS)) {
                names.add(name);
            }
        }
        return names;
    }

    private List<String> cpassVertexLabels() {
        List<String> names = new ArrayList<>();
        for (SchemaLabel label : this.graph().schema().getVertexLabels()) {
            String name = label.name();
            if (name.startsWith(C_PASS)) {
                names.add(name);
            }
        }
        return names;
    }

    private String labelOfPassN(int n) {
        return C_PASS + n;
    }

    private float weightOfEdge(Edge e) {
        if (e.label().startsWith(C_PASS)) {
            assert e.property(C_WEIGHT).isPresent();
            return e.value(C_WEIGHT);
        } else if (e.property(C_WEIGHT).isPresent()) {
            return e.value(C_WEIGHT);
        }
        return 1f;
    }

    private float weightOfEdges(List<Edge> edges) {
        float weight = 0f;
        for (Edge edge : edges) {
            weight += weightOfEdge(edge);
        }
        return weight;
    }

    private Vertex newCommunityNode(Id cid, float cweight,
                                    int kin, List<String> members) {
        assert !members.isEmpty() : members;
        /*
         * cweight: members size(all pass) of the community, just for debug
         * kin: edges weight in the community
         * members: members id of the community of last pass
         */
        return this.graph().addVertex(T.label, this.passLabel,
                                      T.id, cid, C_WEIGHT, cweight,
                                      C_KIN, kin, C_MEMBERS, members);
    }

    private Vertex makeCommunityNode(Id cid) {
        VertexLabel vl = this.graph().vertexLabel(this.passLabel);
        return new HugeVertex(this.graph(), cid, vl);
    }

    private Edge newCommunityEdge(Vertex source, Vertex target, float weight) {
        return source.addEdge(this.passLabel, target, C_WEIGHT, weight);
    }

    private void insertNewCommunity(int pass, Id cid, float cweight,
                                    int kin, List<String> members,
                                    Map<Id, MutableFloat> cedges) {
        // create backend vertex if it's the first time
        Id vid = this.cache.genId(pass, cid);
        Vertex node = this.newCommunityNode(vid, cweight, kin, members);
        commitIfNeeded();
        // update backend vertex edges
        for (Map.Entry<Id, MutableFloat> e : cedges.entrySet()) {
            float weight = e.getValue().floatValue();
            vid = this.cache.genId(pass, e.getKey());
            Vertex targetV = this.makeCommunityNode(vid);
            this.newCommunityEdge(node, targetV, weight);
            commitIfNeeded();
        }
        LOG.debug("Add new comm: {} kin={} size={}", node, kin, members.size());
    }

    private boolean needSkipVertex(int pass, Vertex v) {
        // skip the old intermediate data when first pass
        String label = v.label();
        if (label.startsWith(C_PASS)) {
            if (pass == 0) {
                return true;
            }
            String lastPassLabel = labelOfPassN(pass - 1);
            if (!label.equals(lastPassLabel)) {
                return true;
            }
        }
        // skip the vertex with unmatched clabel
        return this.sourceCLabel != null && !match(v, this.sourceCLabel);
    }

    private Iterator<Vertex> sourceVertices(int pass) {
        if (pass > 0) {
            // all vertices of merged community
            String lastPassLabel = labelOfPassN(pass - 1);
            return this.vertices(lastPassLabel, LIMIT);
        } else {
            assert pass == 0;
            // all vertices at the first time
            return this.vertices(this.sourceLabel, LIMIT);
        }
    }

    private List<Edge> neighbors(Id vid) {
        Iterator<Edge> nbs = this.edgesOfVertex(vid, Directions.BOTH,
                                                (Id) null, this.degree);
        @SuppressWarnings("resource")
        ListIterator<Edge> list = new ListIterator<>(LIMIT, nbs);
        return (List<Edge>) list.list();
    }

    private float weightOfVertex(Vertex v, List<Edge> edges) {
        // degree/weight of vertex
        Float value = this.cache.vertexWeight((Id) v.id());
        if (value != null) {
            return value;
        }
        if (edges == null) {
            edges = neighbors((Id) v.id());
        }
        float weight = weightOfEdges(edges);
        this.cache.vertexWeight((Id) v.id(), weight);
        return weight;
    }

    private int kinOfVertex(Vertex v) {
        if (v.label().startsWith(C_PASS) && v.property(C_KIN).isPresent()) {
            return v.value(C_KIN);
        }
        return 0;
    }

    private float cweightOfVertex(Vertex v) {
        if (v.label().startsWith(C_PASS) && v.property(C_WEIGHT).isPresent()) {
            return v.value(C_WEIGHT);
        }
        return 1f;
    }

    private Community communityOfVertex(Vertex v, List<Edge> nbs) {
        Id vid = (Id) v.id();
        Community c = this.cache.vertex2Community(vid);
        // ensure source vertex exist in cache
        if (c == null) {
            c = this.wrapCommunity(v, nbs);
            assert c != null;
        }
        return c;
    }

    // 1: wrap original vertex as community node
    // 2: add original vertices to community node,
    //    and save as community vertex when merge()
    // 3: wrap community vertex as community node,
    //    and repeat step 2 and step 3.
    private Community wrapCommunity(Vertex v, List<Edge> nbs) {
        Id vid = (Id) v.id();
        Community comm = this.cache.vertex2Community(vid);
        if (comm != null) {
            return comm;
        }

        comm = new Community(vid);
        comm.add(this, v, nbs);
        comm = this.cache.vertex2CommunityIfAbsent(vid, comm);
        return comm;
    }

    private Collection<Pair<Community, MutableInt>> nbCommunities(int pass, List<Edge> edges) {
        // comms is a map of cid:[community,weight]
        Map<Id, Pair<Community, MutableInt>> comms = new HashMap<>();
        for (Edge edge : edges) {
            Vertex otherV = ((HugeEdge) edge).otherVertex();
            if (needSkipVertex(pass, otherV)) {
                // skip the old intermediate data, or filter clabel
                continue;
            }
            // will traverse the neighbors of otherV
            Community c = this.wrapCommunity(otherV, null);
            if (!comms.containsKey(c.cid)) {
                comms.put(c.cid, Pair.of(c, new MutableInt(0)));
            }
            // calc weight between source vertex and neighbor community
            comms.get(c.cid).getRight().add(2 * weightOfEdge(edge));
        }
        return comms.values();
    }

    private void doMoveCommunity(Vertex v, List<Edge> nbs, Community newC) {
        Id vid = (Id) v.id();

        // update community of v (return the origin one)
        Community oldC = this.cache.vertex2Community(vid, newC);

        // remove v from old community. should synchronized (vid)?
        if (oldC != null) {
            oldC.remove(this, v, nbs);
        }

        // add v to new community
        newC.add(this, v, nbs);
        LOG.debug("Move {} to community: {}", v, newC);
    }

    private boolean moveCommunity(Vertex v, int pass) {
        // move vertex to neighbor community if needed
        List<Edge> nbs = neighbors((Id) v.id());
        if (this.skipIsolated && pass == 0 && nbs.isEmpty()) {
            return false;
        }
        Community c = communityOfVertex(v, nbs);
        double ki = kinOfVertex(v) + weightOfVertex(v, nbs);
        // update community of v if △Q changed
        double maxDeltaQ = 0d;
        Community bestComm = null;
        // list all neighbor communities of v
        for (Pair<Community, MutableInt> nbc : nbCommunities(pass, nbs)) {
            // △Q = (Ki_in - Ki * Etot / m) / 2m
            Community otherC = nbc.getLeft();
            if (otherC.size() >= MAX_COMM_SIZE) {
                LOG.info("Skip community {} for {} due to its size >= {}",
                         otherC.cid, v, MAX_COMM_SIZE);
                continue;
            }
            // weight between c and otherC
            double kiin = nbc.getRight().floatValue();
            // weight of otherC
            double tot = otherC.kin() + otherC.kout();
            if (c.equals(otherC)) {
                assert c == otherC;
                if (tot < ki) {
                    /*
                     * expect tot >= ki, but multi-threads may
                     * cause tot < ki due to concurrent update otherC
                     */
                    LOG.warn("Changing vertex: {}(ki={}, kiin={}, pass={}), otherC: {}",
                             v, ki, kiin, pass, otherC);
                }
                tot -= ki;
                // assert tot >= 0d : otherC + ", tot=" + tot + ", ki=" + ki;
                // expect tot >= 0, but may be something wrong?
                if (tot < 0d) {
                    tot = 0d;
                }
            }
            double deltaQ = kiin - ki * tot / this.m;
            if (deltaQ > maxDeltaQ) {
                // TODO: cache otherC for neighbors the same community
                maxDeltaQ = deltaQ;
                bestComm = otherC;
            }
        }
        if (maxDeltaQ > 0d && !c.equals(bestComm)) {
            // move v to the community of maxQ neighbor
            doMoveCommunity(v, nbs, bestComm);
            return true;
        }
        return false;
    }

    private double moveCommunities(int pass) {
        LOG.info("Detect community for pass {}", pass);
        Iterator<Vertex> vertices = this.sourceVertices(pass);

        // shuffle
        //r = r.order().by(shuffle);

        long total = 0L;
        AtomicLong moved = new AtomicLong(0L);

        Consumers<Vertex> consumers = new Consumers<>(this.executor, v -> {
            // called by multi-threads
            if (this.moveCommunity(v, pass)) {
                moved.incrementAndGet();
            }
        });

        consumers.start("louvain-move-pass-" + pass);
        try {
            while (vertices.hasNext()) {
                this.updateProgress(++this.progress);
                Vertex v = vertices.next();
                if (needSkipVertex(pass, v)) {
                    // skip the old intermediate data, or filter clabel
                    continue;
                }
                total++;
                consumers.provide(v);
            }
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            consumers.await();
        }

        // maybe always shocking when set degree limited
        return total == 0L ? 0d : moved.doubleValue() / total;
    }

    private void mergeCommunities(int pass) {
        LOG.info("Merge community for pass {}", pass);
        // merge each community as a vertex
        Collection<Pair<Community, Set<Id>>> comms = this.cache.communities();
        assert this.skipIsolated || this.allMembersExist(comms, pass - 1);
        this.cache.resetVertexWeight();

        Consumers<Pair<Community, Set<Id>>> consumers = new Consumers<>(
                this.executor, pair -> {
            // called by multi-threads
            this.mergeCommunity(pass, pair.getLeft(), pair.getRight());
        }, () -> {
            // commit when finished
            this.graph().tx().commit();
        });

        consumers.start("louvain-merge-pass-" + pass);
        try {
            for (Pair<Community, Set<Id>> pair : comms) {
                Community c = pair.getLeft();
                if (c.empty()) {
                    continue;
                }
                this.progress += pair.getRight().size();
                this.updateProgress(this.progress);
                //this.mergeCommunity(pass, pair.getLeft(), pair.getRight());
                consumers.provide(pair);
            }
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            consumers.await();
        }

        this.graph().tx().commit();
        assert this.skipIsolated || this.allMembersExist(pass);

        // reset communities
        this.cache.reset();
    }

    private void mergeCommunity(int pass, Community c, Set<Id> cvertices) {
        // update kin and edges between communities
        int kin = c.kin();
        int membersSize = cvertices.size();
        assert !cvertices.isEmpty();
        assert membersSize == c.size();
        List<String> members = new ArrayList<>(membersSize);
        Map<Id, MutableFloat> cedges = new HashMap<>(membersSize);
        for (Id v : cvertices) {
            members.add(v.toString());
            // collect edges between this community and other communities
            List<Edge> neighbors = neighbors(v);
            for (Edge edge : neighbors) {
                Vertex otherV = ((HugeEdge) edge).otherVertex();
                if (cvertices.contains(otherV.id())) {
                    // inner edges of this community, will be calc twice
                    // due to both e-in and e-out are in vertices,
                    kin += (int) weightOfEdge(edge);
                    continue;
                }
                assert this.cache.vertex2Community(otherV.id()) != null;
                Id otherCid = communityOfVertex(otherV, null).cid;
                if (otherCid.compareTo(c.cid) < 0) {
                    // skip if it should be collected by otherC
                    continue;
                }
                if (!cedges.containsKey(otherCid)) {
                    cedges.putIfAbsent(otherCid, new MutableFloat(0f));
                }
                // update edge weight
                cedges.get(otherCid).add(weightOfEdge(edge));
            }
        }

        // insert new community vertex and edges into storage
        this.insertNewCommunity(pass, c.cid, c.weight(), kin, members, cedges);
    }

    private boolean allMembersExist(Collection<Pair<Community, Set<Id>>> comms,
                                    int lastPass) {
        String lastLabel = labelOfPassN(lastPass);
        GraphTraversal<Vertex, Object> t = lastPass < 0 ? this.g.V().id() :
                                           this.g.V().hasLabel(lastLabel).id();
        Set<Object> all = this.execute(t, t::toSet);
        for (Pair<Community, Set<Id>> comm : comms) {
            all.removeAll(comm.getRight());
        }
        if (!all.isEmpty()) {
            LOG.warn("Lost members of last pass: {}", all);
        }
        return all.isEmpty();
    }

    private boolean allMembersExist(int pass) {
        String label = labelOfPassN(pass);
        int lastPass = pass - 1;
        Number expected;
        if (lastPass < 0) {
            expected = tryNext(this.g.V().count()).longValue() -
                       tryNext(this.g.V().hasLabel(label).count()).longValue();
        } else {
            expected = tryNext(this.g.V().hasLabel(labelOfPassN(lastPass))
                                     .values(C_WEIGHT).sum());
        }
        Number actual = tryNext(this.g.V().hasLabel(label)
                                      .values(C_WEIGHT).sum());
        boolean allExist = actual.floatValue() == expected.floatValue();
        assert allExist : actual + "!=" + expected;
        return allExist;
    }

    public Object louvain(int maxTimes, int stableTimes, double precision) {
        assert maxTimes > 0;
        assert precision > 0d;

        this.defineSchemaOfPk();

        /*
         * iterate until it has stabilized or
         * the maximum number of times is reached
         */
        int times = maxTimes;
        int movedTimes = 0;
        double movedPercent = 0d;
        double lastMovedPercent;

        for (int i = 0; i < maxTimes; i++) {
            boolean finished = true;
            lastMovedPercent = 1d;
            int tinyChanges = 0;
            while ((movedPercent = this.moveCommunities(i)) > 0d) {
                movedTimes++;
                finished = false;
                if (lastMovedPercent - movedPercent < precision) {
                    tinyChanges++;
                }
                if (i == 0 && movedPercent < precision) {
                    // stop the first round of iterations early
                    break;
                }
                if (tinyChanges >= stableTimes) {
                    // maybe always shaking and falling into an dead loop
                    break;
                }
                lastMovedPercent = movedPercent;
            }
            if (finished) {
                times = i;
                break;
            } else {
                this.defineSchemaOfPassN(i);
                this.mergeCommunities(i);
            }
        }

        Map<String, Object> results = InsertionOrderUtil.newMap();
        results.putAll(ImmutableMap.of("pass_times", times,
                                       "phase1_times", movedTimes,
                                       "last_precision", movedPercent,
                                       "times", maxTimes));
        Number communities = 0L;
        Number modularity = -1L;
        String commLabel = this.passLabel;
        if (!commLabel.isEmpty()) {
            communities = tryNext(this.g.V().hasLabel(commLabel).count());
            modularity = this.modularity(commLabel);
        }
        results.putAll(ImmutableMap.of("communities", communities,
                                       "modularity", modularity));
        return results;
    }

    public double modularity(int pass) {
        // community vertex label of one pass
        String label = labelOfPassN(pass);
        return this.modularity(label);
    }

    private double modularity(String label) {
        // label: community vertex label of one pass
        Number kin = tryNext(this.g.V().hasLabel(label).values(C_KIN).sum());
        Number weight = tryNext(this.g.E().hasLabel(label).values(C_WEIGHT).sum());
        double m = kin.intValue() + weight.floatValue() * 2.0d;
        double q = 0.0d;
        Iterator<Vertex> comms = this.vertices(label, LIMIT);
        while (comms.hasNext()) {
            Vertex comm = comms.next();
            int cin = comm.value(C_KIN);
            Number cout = tryNext(this.g.V(comm).bothE().values(C_WEIGHT).sum());
            double cdegree = cin + cout.floatValue();
            // Q = ∑(I/M - ((2I+O)/2M)^2)
            q += cin / m - Math.pow(cdegree / m, 2);
        }
        return q;
    }

    public Collection<Object> showCommunity(String community) {
        final String C_PASS0 = labelOfPassN(0);
        Collection<Object> comms = Collections.singletonList(community);
        boolean reachPass0 = false;
        while (!comms.isEmpty() && !reachPass0) {
            Iterator<Vertex> subComms = this.vertices(comms.iterator());
            comms = new HashSet<>();
            while (subComms.hasNext()) {
                this.updateProgress(++this.progress);
                Vertex sub = subComms.next();
                if (sub.property(C_MEMBERS).isPresent()) {
                    Set<Object> members = sub.value(C_MEMBERS);
                    reachPass0 = sub.label().equals(C_PASS0);
                    comms.addAll(members);
                }
            }
        }
        return comms;
    }

    public long exportCommunity(int pass, boolean vertexFirst) {
        String exportFile = String.format("%s/louvain-%s.txt",
                                          LouvainAlgorithm.EXPORT_PATH,
                                          this.jobId());
        String label = labelOfPassN(pass);
        GraphTraversal<Vertex, Vertex> t = this.g.V().hasLabel(label);
        this.execute(t, () -> {
            try (OutputStream os = Files.newOutputStream(Paths.get(exportFile));
                 BufferedOutputStream bos = new BufferedOutputStream(os)) {
                while (t.hasNext()) {
                    String comm = t.next().id().toString();
                    Collection<Object> members = this.showCommunity(comm);
                    if (vertexFirst) {
                        for (Object member : members) {
                            bos.write(StringEncoding.encode(member.toString()));
                            bos.write(StringEncoding.encode("\t"));
                            bos.write(StringEncoding.encode(comm));
                            bos.write(StringEncoding.encode("\n"));
                        }
                    } else {
                        bos.write(StringEncoding.encode(comm));
                        bos.write(StringEncoding.encode(": "));
                        bos.write(StringEncoding.encode(members.toString()));
                        bos.write(StringEncoding.encode("\n"));
                    }
                }
            }
            return null;
        });

        return this.progress;
    }

    public long clearPass(int pass) {
        GraphTraversal<Edge, Edge> te = this.g.E();
        if (pass < 0) {
            // drop edges of all pass
            List<String> els = this.cpassEdgeLabels();
            if (!els.isEmpty()) {
                String first = els.remove(0);
                te = te.hasLabel(first, els.toArray(new String[0]));
                this.drop(te);
            }
            // drop schema
            for (String label : this.cpassEdgeLabels()) {
                this.graph().schema().edgeLabel(label).remove();
            }
        } else {
            // drop edges of pass N
            String label = labelOfPassN(pass);
            if (this.graph().existsEdgeLabel(label)) {
                te = te.hasLabel(label);
                this.drop(te);
                // drop schema
                this.graph().schema().edgeLabel(label).remove();
            }
        }

        GraphTraversal<Vertex, Vertex> tv = this.g.V();
        if (pass < 0) {
            // drop vertices of all pass
            List<String> vls = this.cpassVertexLabels();
            if (!vls.isEmpty()) {
                String first = vls.remove(0);
                tv = tv.hasLabel(first, vls.toArray(new String[0]));
                this.drop(tv);
            }
            // drop schema
            for (String label : this.cpassVertexLabels()) {
                this.graph().schema().vertexLabel(label).remove();
            }
        } else {
            // drop vertices of pass N
            String label = labelOfPassN(pass);
            if (this.graph().existsVertexLabel(label)) {
                tv = tv.hasLabel(label);
                this.drop(tv);
                // drop schema
                this.graph().schema().vertexLabel(label).remove();
            }
        }

        return this.progress;
    }

    private static class Community {

        // community id (stored as a backend vertex)
        private final Id cid;
        // community members size of last pass [just for skip large community]
        private int size = 0;
        // community members size of origin vertex [just for debug members lost]
        private float weight = 0f;
        /*
         * weight of all edges in community(2X), sum of kin of new members
         *  [each is from the last pass, stored in backend vertex]
         */
        private int kin = 0;
        /*
         * weight of all edges between communities, sum of kout of new members
         * [each is last pass, calculated in real time by neighbors]
         */
        private float kout = 0f;

        public Community(Id cid) {
            this.cid = cid;
        }

        public boolean empty() {
            return this.size <= 0;
        }

        public int size() {
            return this.size;
        }

        public float weight() {
            return this.weight;
        }

        public synchronized void add(LouvainTraverser t,
                                     Vertex v, List<Edge> nbs) {
            this.size++;
            this.weight += t.cweightOfVertex(v);
            this.kin += t.kinOfVertex(v);
            this.kout += t.weightOfVertex(v, nbs);
        }

        public synchronized void remove(LouvainTraverser t,
                                        Vertex v, List<Edge> nbs) {
            this.size--;
            this.weight -= t.cweightOfVertex(v);
            this.kin -= t.kinOfVertex(v);
            this.kout -= t.weightOfVertex(v, nbs);
        }

        public synchronized int kin() {
            return this.kin;
        }

        public synchronized float kout() {
            return this.kout;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Community)) {
                return false;
            }
            Community other = (Community) object;
            return Objects.equals(this.cid, other.cid);
        }

        @Override
        public String toString() {
            return String.format("[%s](size=%s weight=%s kin=%s kout=%s)",
                                 this.cid, this.size, this.weight,
                                 this.kin, this.kout);
        }
    }

    private static class Cache {

        private final Map<Id, Float> vertexWeightCache;
        private final Map<Id, Community> vertex2Community;
        private final Map<Id, Integer> genIds;

        public Cache() {
            this.vertexWeightCache = new ConcurrentHashMap<>();
            this.vertex2Community = new ConcurrentHashMap<>();
            this.genIds = new ConcurrentHashMap<>();
        }

        public Community vertex2Community(Object id) {
            assert id instanceof Id;
            return this.vertex2Community.get(id);
        }

        public Community vertex2Community(Id id, Community c) {
            return this.vertex2Community.put(id, c);
        }

        public Community vertex2CommunityIfAbsent(Id id, Community c) {
            Community old = this.vertex2Community.putIfAbsent(id, c);
            if (old != null) {
                c = old;
            }
            return c;
        }

        public Float vertexWeight(Id id) {
            return this.vertexWeightCache.get(id);
        }

        public void vertexWeight(Id id, float weight) {
            this.vertexWeightCache.put(id, weight);
        }

        public void reset() {
            this.vertexWeightCache.clear();
            this.vertex2Community.clear();
            this.genIds.clear();
        }

        public void resetVertexWeight() {
            this.vertexWeightCache.clear();
        }

        public Id genId(int pass, Id cid) {
            synchronized (this.genIds) {
                if (!this.genIds.containsKey(cid)) {
                    this.genIds.putIfAbsent(cid, this.genIds.size() + 1);
                }
                String id = pass + "~" + this.genIds.get(cid);
                return IdGenerator.of(id);
            }
        }

        @SuppressWarnings("unused")
        public Id genId2(int pass, Id cid) {
            // gen id for merge-community vertex
            String id = cid.toString();
            if (pass == 0) {
                // concat pass with cid
                id = pass + "~" + id;
            } else {
                // replace last pass with current pass
                String lastPass = String.valueOf(pass - 1);
                assert id.startsWith(lastPass);
                id = id.substring(lastPass.length());
                id = pass + id;
            }
            return IdGenerator.of(id);
        }

        public Collection<Pair<Community, Set<Id>>> communities() {
            // TODO: get communities from backend store instead of ram
            Map<Id, Pair<Community, Set<Id>>> comms = new HashMap<>();
            for (Entry<Id, Community> e : this.vertex2Community.entrySet()) {
                Community c = e.getValue();
                if (c.empty()) {
                    continue;
                }
                Pair<Community, Set<Id>> pair = comms.computeIfAbsent(c.cid, k -> {
                    return Pair.of(c, new HashSet<>());
                });
                // collect members joined to the community [current pass]
                pair.getRight().add(e.getKey());
            }
            return comms.values();
        }
    }
}
