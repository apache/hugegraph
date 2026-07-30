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

package org.apache.hugegraph.rocksdb.access;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * {@link SessionOperatorImpl#get(String, byte[])} read-miss / cloud-hydration behaviour.
 *
 * <p>The {@code get} path releases the column-family handle lock BEFORE invoking the
 * {@link RocksdbChangedListener#onReadMiss} hook (hydration may perform slow S3 I/O), then
 * re-acquires the lock for a single retry read. These tests pin that contract:
 *
 * <ul>
 *   <li>A hit returns the value without ever consulting a read-miss listener.</li>
 *   <li>A miss with no listener (or a listener that reports {@code false}) returns {@code null}
 *       without a retry read.</li>
 *   <li>A miss whose listener hydrates the key and reports {@code true} triggers the retry read
 *       and returns the hydrated value.</li>
 *   <li>A miss whose listener reports {@code true} but hydrates nothing still returns {@code null}
 *       from the retry read.</li>
 * </ul>
 */
public class SessionOperatorReadMissTest {

    private static final String TABLE = "t";

    @BeforeClass
    public static void init() {
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");
        RocksDBFactory.getInstance().setHugeConfig(new HugeConfig(new MapConfiguration(configMap)));
    }

    @Test
    public void testGetHitDoesNotConsultReadMissListener() throws Exception {
        AtomicInteger readMissCalls = new AtomicInteger(0);
        RocksdbChangedListener counter = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                readMissCalls.incrementAndGet();
                return false;
            }
        };
        withSession("hit", counter, session -> {
            SessionOperator op = session.sessionOp();
            op.prepare();
            op.put(TABLE, bytes("k"), bytes("v"));
            op.commit();

            byte[] value = session.sessionOp().get(TABLE, bytes("k"));

            assertArrayEquals(bytes("v"), value);
            assertEquals("read-miss hook must not fire on a hit", 0, readMissCalls.get());
        });
    }

    @Test
    public void testGetMissWithoutHydrationReturnsNull() throws Exception {
        AtomicInteger readMissCalls = new AtomicInteger(0);
        RocksdbChangedListener noHydration = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                readMissCalls.incrementAndGet();
                return false;
            }
        };
        withSession("miss-nohydrate", noHydration, session -> {
            session.checkTable(TABLE);

            byte[] value = session.sessionOp().get(TABLE, bytes("absent"));

            assertNull("a miss that is not hydrated must return null", value);
            assertEquals("read-miss hook must fire exactly once on a miss",
                         1, readMissCalls.get());
        });
    }

    @Test
    public void testGetMissTriggersHydrationThenReturnsHydratedValue() throws Exception {
        RocksdbChangedListener hydrator = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                // Simulate cloud hydration: the CF handle lock has already been released, so the
                // listener may safely re-enter the session to write the missing key.
                SessionOperator op = session.sessionOp();
                op.prepare();
                op.put(table, key, bytes("hydrated"));
                op.commit();
                return true;
            }
        };
        withSession("miss-hydrate", hydrator, session -> {
            session.checkTable(TABLE);

            byte[] value = session.sessionOp().get(TABLE, bytes("cold"));

            assertArrayEquals("retry read after hydration must observe the hydrated value",
                              bytes("hydrated"), value);
        });
    }

    @Test
    public void testGetMissHydrationReportedButNoDataReturnsNull() throws Exception {
        RocksdbChangedListener liar = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                // Reports success but writes nothing — the retry read must still return null.
                return true;
            }
        };
        withSession("miss-emptyhydrate", liar, session -> {
            session.checkTable(TABLE);

            byte[] value = session.sessionOp().get(TABLE, bytes("cold"));

            assertNull("retry read must return null when hydration produced no data", value);
        });
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private interface SessionTask {
        void run(RocksDBSession session) throws Exception;
    }

    private static void withSession(String dbName, RocksdbChangedListener listener,
                                    SessionTask task) throws Exception {
        Path parent = Files.createTempDirectory("hg-readmiss-");
        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(listener);
        try {
            RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
            task.run(session);
        } finally {
            factory.removeRocksdbChangedListener(listener);
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteRecursively(java.io.File dir) {
        if (dir == null || !dir.exists()) {
            return;
        }
        java.io.File[] children = dir.listFiles();
        if (children != null) {
            for (java.io.File c : children) {
                if (c.isDirectory()) {
                    deleteRecursively(c);
                } else {
                    c.delete();
                }
            }
        }
        dir.delete();
    }
}
