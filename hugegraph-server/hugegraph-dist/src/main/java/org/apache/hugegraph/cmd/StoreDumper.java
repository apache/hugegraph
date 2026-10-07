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

package org.apache.hugegraph.cmd;

import java.util.Iterator;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

public class StoreDumper {

    private final HugeGraph graph;

    private static final Logger LOG = Log.logger(StoreDumper.class);

    public StoreDumper(String conf) {
        this.graph = HugeFactory.open(conf);
    }

    public void dump(HugeType table, long offset, long limit) {
        BackendStore store = this.backendStore(table);

        Query query = new Query(table);
        Iterator<BackendEntry> rs = store.query(query);
        for (long i = 0; i < offset && rs.hasNext(); i++) {
            rs.next();
        }

        LOG.info("Dump table {} (offset {} limit {}):", table, offset, limit);

        for (long i = 0; i < limit && rs.hasNext(); i++) {
            BackendEntry entry = rs.next();
            LOG.info("{}", entry);
        }

        CloseableIterator.closeIterator(rs);
    }

    private BackendStore backendStore(HugeType table) {
        String m = table.isSchema() ? "schemaTransaction" : "graphTransaction";
        Object tx = Whitebox.invoke(this, "graph", m);
        return Whitebox.invoke(tx.getClass(), "store", tx);
    }

    public void close() throws Exception {
        this.graph.close();
    }

    public static void main(String[] args) throws Exception {
        E.checkArgument(args.length >= 1,
                        "StoreDumper need a config file.");

        String conf = args[0];
        RegisterUtil.registerBackends();

        HugeType table = HugeType.valueOf(arg(args, 1, "VERTEX").toUpperCase());
        long offset = Long.parseLong(arg(args, 2, "0"));
        long limit = Long.parseLong(arg(args, 3, "20"));

        StoreDumper dumper = new StoreDumper(conf);
        dumper.dump(table, offset, limit);
        dumper.close();

        // Stop daemon thread
        HugeFactory.shutdown(30L, true);
    }

    private static String arg(String[] args, int index, String deflt) {
        if (index < args.length) {
            return args[index];
        }
        return deflt;
    }
}
