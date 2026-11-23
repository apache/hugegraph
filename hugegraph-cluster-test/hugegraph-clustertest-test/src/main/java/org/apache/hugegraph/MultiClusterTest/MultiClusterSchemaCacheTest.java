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

package org.apache.hugegraph.MultiClusterTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.SimpleClusterTest.BaseSimpleTest.RestClient;
import org.apache.hugegraph.ct.env.AbstractEnv;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;

/**
 * Schema changes made through one Server must reach the schema caches of the
 * other Servers (apache/hugegraph#3235). Every test changes the schema through
 * one Server and then asks the other two, which have the old schema cached.
 * <p>
 * Not covered here, because the code does not provide it: two Servers changing
 * the same schema element at the same moment, and a write that is in flight on
 * another Server at the instant an index is created.
 */
public class MultiClusterSchemaCacheTest extends BaseMultiClusterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PKS = URL_PREFIX + "/schema/propertykeys";
    private static final String VLS = URL_PREFIX + "/schema/vertexlabels";
    private static final String ILS = URL_PREFIX + "/schema/indexlabels";
    private static final String VERTICES = URL_PREFIX + "/graph/vertices";
    private static final String TASKS = URL_PREFIX + "/tasks";
    private static final long VISIBLE_SECONDS = 30L;

    @BeforeClass
    public static void waitForGraphOnEveryServer() throws Exception {
        for (RestClient client : clients) {
            client.target().property(ClientProperties.CONNECT_TIMEOUT, 5_000)
                  .property(ClientProperties.READ_TIMEOUT, 15_000);
        }
        waitFor("the graph to open on every Server", 120L, () -> {
            for (RestClient client : clients) {
                if (status(client.get(URL_PREFIX)) != 200) {
                    return false;
                }
            }
            return true;
        });
    }

    @Test
    public void testUpdateReachesEveryServer() throws Exception {
        String pk = "sc_upd_note";
        createPropertyKey(0, pk, "TEXT");
        for (RestClient client : clients) {
            json(client.get(PKS + "/" + pk));
        }

        ok(put(clients.get(0), PKS + "/" + pk, "append",
               "{\"name\": \"" + pk + "\", \"user_data\": {\"marker\": \"set\"}}"));

        waitFor("the appended user_data on every Server", VISIBLE_SECONDS, () -> {
            for (RestClient client : clients) {
                if (!json(client.get(PKS + "/" + pk)).path("user_data").has("marker")) {
                    return false;
                }
            }
            return true;
        });
    }

    @Test
    public void testAppendThroughAnotherServerKeepsTheFirstAppend() throws Exception {
        String a = "sc_app_a";
        String p1 = "sc_app_p1";
        String p2 = "sc_app_p2";
        String vl = "sc_app_person";
        for (String pk : new String[]{a, p1, p2}) {
            createPropertyKey(0, pk, "TEXT");
        }
        createVertexLabel(0, vl, a);
        for (RestClient client : clients) {
            json(client.get(VLS + "/" + vl));
        }

        ok(put(clients.get(0), VLS + "/" + vl, "append", appendBody(vl, p1)));
        waitFor(p1 + " on every Server", VISIBLE_SECONDS,
                () -> everyServerHasProperties(vl, a, p1));

        // Server 1 writes the whole label back with its own change added
        ok(put(clients.get(1), VLS + "/" + vl, "append", appendBody(vl, p2)));
        waitFor(p1 + " and " + p2 + " on every Server", VISIBLE_SECONDS,
                () -> everyServerHasProperties(vl, a, p1, p2));
    }

    @Test
    public void testIndexCreatedThroughOneServerWorksOnTheOthers() throws Exception {
        int indexes = 4;
        String name = "sc_idx_name";
        String vl = "sc_idx_person";
        List<String> props = new ArrayList<>();
        createPropertyKey(0, name, "TEXT");
        for (int k = 0; k < indexes; k++) {
            props.add("sc_idx_c" + k);
            createPropertyKey(0, props.get(k), "INT");
        }
        createVertexLabel(0, vl, name, props.toArray(new String[0]));
        for (int i = 0; i < 20; i++) {
            StringBuilder body = new StringBuilder("{\"label\": \"" + vl +
                                                   "\", \"properties\": {\"" + name +
                                                   "\": \"v" + i + "\"");
            for (String prop : props) {
                body.append(", \"").append(prop).append("\": ").append(i);
            }
            ok(clients.get(0).post(VERTICES, body.append("}}").toString()));
        }

        for (int k = 0; k < indexes; k++) {
            String prop = props.get(k);
            String il = "sc_idx_by_c" + k;
            for (RestClient client : clients) {
                json(client.get(VLS + "/" + vl));
                json(client.get(ILS));
            }

            // The other Servers serve reads of this label while the index is created
            AtomicBoolean stop = new AtomicBoolean(false);
            ExecutorService readers = Executors.newFixedThreadPool(4);
            List<Future<?>> reads = new ArrayList<>();
            for (int reader = 0; reader < 4; reader++) {
                RestClient client = clients.get(1 + reader % 2);
                reads.add(readers.submit(() -> {
                    while (!stop.get()) {
                        Response r = client.target().path(VERTICES)
                                           .queryParam("label", vl)
                                           .queryParam("limit", 3).request().get();
                        ok(r);
                    }
                }));
            }
            try {
                JsonNode created = json(clients.get(0).post(ILS,
                        "{\"name\": \"" + il + "\", \"base_type\": \"VERTEX_LABEL\", " +
                        "\"base_value\": \"" + vl + "\", \"index_type\": \"SECONDARY\", " +
                        "\"fields\": [\"" + prop + "\"]}"));
                String task = created.path("task_id").asText();
                waitFor("the rebuild task of " + il, 120L, () ->
                        "success".equals(json(clients.get(0).get(TASKS + "/" + task))
                                                 .path("task_status").asText()));
            } finally {
                stop.set(true);
                readers.shutdown();
                Assert.assertTrue(readers.awaitTermination(60L, TimeUnit.SECONDS));
            }
            // A reader that stopped on an error left the index created with fewer reads
            for (Future<?> read : reads) {
                read.get();
            }

            waitFor(il + " created and listed in its label on every Server",
                    VISIBLE_SECONDS, () -> {
                        for (RestClient client : clients) {
                            JsonNode index = json(client.get(ILS + "/" + il));
                            if (!"CREATED".equals(index.path("status").asText())) {
                                return false;
                            }
                            JsonNode label = json(client.get(VLS + "/" + vl));
                            if (!contains(label.path("index_labels"), il)) {
                                return false;
                            }
                        }
                        return true;
                    });

            // A vertex written through any Server is found through the index on any Server
            for (int writer = 0; writer < clients.size(); writer++) {
                ok(clients.get(writer).post(VERTICES,
                        "{\"label\": \"" + vl + "\", \"properties\": {\"" + name +
                        "\": \"w" + k + "_" + writer + "\", \"" + prop + "\": " +
                        value(k, writer) + "}}"));
            }
            int trial = k;
            waitFor("vertices written through every Server in index " + il,
                    VISIBLE_SECONDS, () -> {
                        for (RestClient reader : clients) {
                            for (int writer = 0; writer < clients.size(); writer++) {
                                Response r = reader.target().path(VERTICES)
                                                   .queryParam("label", vl)
                                                   .queryParam("properties", "{condition}")
                                                   .resolveTemplate("condition",
                                                                    "{\"" + prop + "\": " +
                                                                    value(trial, writer) + "}")
                                                   .request().get();
                                if (r.getStatus() != 200) {
                                    r.readEntity(String.class);
                                    return false;
                                }
                                if (json(r).path("vertices").size() != 1) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    });
        }
    }

    @Test
    public void testRemoveReachesEveryServerWithSyncDeletion() throws Exception {
        /*
         * task.sync_deletion used to switch the clear event of a removal off.
         * This checks the removal end to end, but it is not the regression
         * test for that: the Servers also publish clear events while they set
         * up a new graph, which can clear the caches here on their own. The
         * unit test of CachedSchemaTransactionV2 pins the publish itself.
         */
        String graph = "schemacachesync";
        String prefix = "graphspaces/DEFAULT/graphs/" + graph;
        String pks = prefix + "/schema/propertykeys";
        if (status(clients.get(0).get(prefix)) != 200) {
            ok(clients.get(0).post(prefix,
                                   "{\"backend\": \"hstore\", \"serializer\": \"binary\", " +
                                   "\"store\": \"" + graph + "\", " +
                                   "\"task.sync_deletion\": \"true\"}"));
        }
        waitFor("the second graph on every Server", 120L, () -> {
            for (RestClient client : clients) {
                if (status(client.get(prefix)) != 200) {
                    return false;
                }
            }
            return true;
        });

        String pk = "sc_rm_note";
        ok(clients.get(0).post(pks, propertyKeyBody(pk, "TEXT")));
        waitFor(pk + " listed on every Server", VISIBLE_SECONDS,
                () -> everyServerLists(pks, pk, true));

        Response removed = clients.get(0).target().path(pks + "/" + pk).request().delete();
        ok(removed);
        waitFor(pk + " gone from every Server", VISIBLE_SECONDS,
                () -> everyServerLists(pks, pk, false));
    }

    @Test
    public void testChangeDuringPDRestartReachesEveryServer() throws Exception {
        AbstractEnv cluster = (AbstractEnv) env;
        int pdNodes = cluster.getPDRestAddrs().size();
        // One of the PD nodes is the leader: restarting it restarts every watch
        for (int node = 0; node < pdNodes; node++) {
            String pk = "sc_gap_" + node;
            for (RestClient client : clients) {
                json(client.get(PKS));
            }

            cluster.restartPDNode(node);
            // One change as early as PD takes it, while the watches may still be down
            waitFor("an add right after restarting PD node " + node, 180L, () -> {
                Response r = clients.get(0).post(PKS, propertyKeyBody(pk, "TEXT"));
                String content = r.readEntity(String.class);
                return r.getStatus() / 100 == 2 ||
                       (r.getStatus() == 400 && content.contains("exist"));
            });
            cluster.awaitPDNode(node);

            waitFor(pk + " listed on every Server after restarting PD node " + node,
                    90L, () -> everyServerLists(PKS, pk, true));
        }
    }

    // ------------------------------------------------------------ helpers

    private static int value(int trial, int writer) {
        return 100000 + 10 * trial + writer;
    }

    private static void createPropertyKey(int server, String name, String dataType) {
        ok(clients.get(server).post(PKS, propertyKeyBody(name, dataType)));
    }

    private static String propertyKeyBody(String name, String dataType) {
        return "{\"name\": \"" + name + "\", \"data_type\": \"" + dataType +
               "\", \"cardinality\": \"SINGLE\"}";
    }

    private static void createVertexLabel(int server, String name, String first,
                                          String... others) {
        StringBuilder props = new StringBuilder("\"" + first + "\"");
        for (String other : others) {
            props.append(", \"").append(other).append("\"");
        }
        ok(clients.get(server).post(VLS,
                                    "{\"name\": \"" + name + "\", \"id_strategy\": " +
                                    "\"AUTOMATIC\", \"properties\": [" + props +
                                    "], \"nullable_keys\": [" + props + "]}"));
    }

    private static String appendBody(String label, String property) {
        return "{\"name\": \"" + label + "\", \"properties\": [\"" + property +
               "\"], \"nullable_keys\": [\"" + property + "\"]}";
    }

    private static boolean everyServerHasProperties(String label, String... properties)
            throws Exception {
        for (RestClient client : clients) {
            JsonNode found = json(client.get(VLS + "/" + label)).path("properties");
            for (String property : properties) {
                if (!contains(found, property)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean everyServerLists(String path, String name, boolean listed)
            throws Exception {
        for (RestClient client : clients) {
            Response r = client.get(path);
            if (r.getStatus() != 200) {
                r.readEntity(String.class);
                return false;
            }
            boolean found = false;
            for (JsonNode pk : json(r).path("propertykeys")) {
                found |= name.equals(pk.path("name").asText());
            }
            if (found != listed) {
                return false;
            }
        }
        return true;
    }

    private static boolean contains(JsonNode array, String text) {
        for (JsonNode item : array) {
            if (text.equals(item.asText())) {
                return true;
            }
        }
        return false;
    }

    private static Response put(RestClient client, String path, String action, String body) {
        return client.target().path(path).queryParam("action", action)
                     .request().put(Entity.json(body));
    }

    private static int status(Response response) {
        response.readEntity(String.class);
        return response.getStatus();
    }

    private static void ok(Response response) {
        String content = response.readEntity(String.class);
        Assert.assertEquals("Response with status " + response.getStatus() +
                            " and content " + content, 2, response.getStatus() / 100);
    }

    private static JsonNode json(Response response) throws Exception {
        String content = response.readEntity(String.class);
        Assert.assertEquals("Response with status " + response.getStatus() +
                            " and content " + content, 2, response.getStatus() / 100);
        return MAPPER.readTree(content);
    }

    private static void waitFor(String what, long seconds, Callable<Boolean> condition)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.call()) {
                    return;
                }
                last = null;
            } catch (Exception | AssertionError e) {
                // A Server may answer with an error while PD restarts
                last = e;
            }
            Thread.sleep(200L);
        }
        try {
            env.dumpClusterStatus();
        } catch (Throwable ignored) {
            // The failure below is the one to report
        }
        throw new AssertionError("Timed out after " + seconds + " s waiting for " + what, last);
    }
}
