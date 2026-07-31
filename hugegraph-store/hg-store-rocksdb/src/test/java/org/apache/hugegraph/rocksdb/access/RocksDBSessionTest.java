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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.junit.BeforeClass;

public class RocksDBSessionTest {

    private final String graphName = "testDummy";

    @BeforeClass
    public static void init() {
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");

        HugeConfig hConfig = new HugeConfig(new MapConfiguration(configMap));
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        rFactory.setHugeConfig(hConfig);
    }

    private static byte[] intToBytesForPartId(int v) {
        short s = (short) v;
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort(s);
        return buffer.array();
    }

    public static byte[] byteCompose(byte[] b1, byte[] b2) {
        byte[] b3 = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, b3, 0, b1.length);
        System.arraycopy(b2, 0, b3, b1.length, b2.length);
        return b3;
    }

    private static byte[] keyAppendPartId(int partId, byte[] key) {
        byte[] partBytes = intToBytesForPartId(partId);
        byte[] targetKey = byteCompose(partBytes, key);
        return targetKey;
    }

    private static byte[] getPartStartKey(int partId) {
        return intToBytesForPartId(partId);
    }

    private static byte[] getPartEndKey(int partId) {
        return intToBytesForPartId(partId + 1);
    }

    //    @Test
    public void put() {
        System.out.println("RocksDBSessionTest::put test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t1";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "a1".getBytes(), "f1".getBytes());
        sessionOp.commit();

        String vRet = new String(rSession.sessionOp().get(tName, "a1".getBytes()));
        assertEquals(vRet, "f1");
    }

    // @Test
    public void selectTable() {

        System.out.println("RocksDBSessionTest::selectTable test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t2";
        rSession.checkTable(tName);

        assertTrue(rSession.tableIsExist(tName));

    }

    // @Test
    public void batchPut() {
        System.out.println("RocksDBSessionTest::batchPut test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t3";
        String tName2 = "t4";

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "a1".getBytes(), "f1".getBytes());
        sessionOp.put(tName, "a2".getBytes(), "f2".getBytes());
        sessionOp.put(tName, "a3".getBytes(), "f3".getBytes());
        sessionOp.put(tName, "a4".getBytes(), "f4".getBytes());
        sessionOp.put(tName, "a5".getBytes(), "f5".getBytes());
        sessionOp.put(tName, "a6".getBytes(), "f6".getBytes());
        sessionOp.commit();

        sessionOp.put(tName2, "m1".getBytes(), "k1".getBytes());
        sessionOp.put(tName2, "m2".getBytes(), "k2".getBytes());
        sessionOp.put(tName2, "m3".getBytes(), "k3".getBytes());
        sessionOp.put(tName, "a7".getBytes(), "f7".getBytes());
        sessionOp.commit();

        String vRet1 = new String(rSession.sessionOp().get(tName, "a1".getBytes()));
        String vRet2 = new String(rSession.sessionOp().get(tName2, "m1".getBytes()));
        String vRet3 = new String(rSession.sessionOp().get(tName, "a7".getBytes()));

        assertEquals(vRet1, "f1");
        assertEquals(vRet2, "k1");
        assertEquals(vRet3, "f7");

    }

    // @Test
    public void get() {
        System.out.println("RocksDBSessionTest::get test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t5";
        rSession.checkTable(tName);
        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "hash".getBytes(), "youareok".getBytes());
        sessionOp.commit();

        String vRet = new String(rSession.sessionOp().get(tName, "hash".getBytes()));
        assertEquals(vRet, "youareok");
    }

    // @Test
    public void createTables() {
        System.out.println("RocksDBSessionTest::createTables test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        rSession.createTables("fly1", "fly2", "fly3");

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put("fly1", "cat1".getBytes(), "hit1".getBytes());
        sessionOp.put("fly2", "cat2".getBytes(), "hit2".getBytes());
        sessionOp.put("fly3", "cat3".getBytes(), "hit3".getBytes());
        sessionOp.commit();

        assertTrue(rSession.tableIsExist("fly1"));
        assertTrue(rSession.tableIsExist("fly2"));
        assertTrue(rSession.tableIsExist("fly3"));

    }

    // @Test
    public void dropTables() {
        System.out.println("RocksDBSessionTest::dropTables test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        rSession.createTables("dummy1");

        assertTrue(rSession.tableIsExist("dummy1"));

        rSession.dropTables("dummy1");

        assertFalse(rSession.tableIsExist("dummy1"));

    }

    // @Test
    public void scan() {
        System.out.println("RocksDBSessionTest::scan test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        String graph1 = "tBatch";
        rFactory.createGraphDB("./tmp", graphName);
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graph1);

        String tName = "t6";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "box1".getBytes(), "gift1".getBytes());
        sessionOp.put(tName, "box2".getBytes(), "gift2".getBytes());
        sessionOp.put(tName, "box3".getBytes(), "gift3".getBytes());
        sessionOp.put(tName, "box4".getBytes(), "gift4".getBytes());
        sessionOp.put(tName, "box5".getBytes(), "gift5".getBytes());
        sessionOp.put(tName, "box6".getBytes(), "gift6".getBytes());
        sessionOp.put(tName, "box7".getBytes(), "gift7".getBytes());
        sessionOp.put(tName, "box8".getBytes(), "gift8".getBytes());
        sessionOp.commit();

        //scan table
        ScanIterator it = sessionOp.scan(tName);
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + " : " + new String(col.value));
        }

        long c = rSession.getApproximateDataSize();
        System.out.println(c);

        long c1 = RocksDBFactory.getInstance().getTotalSize();
        System.out.println(c1);
    }

    // @Test
    public void testScan() {
        System.out.println("RocksDBSessionTest::testScan test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t7";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "box1".getBytes(), "gift1".getBytes());
        sessionOp.put(tName, "box2".getBytes(), "gift2".getBytes());
        sessionOp.put(tName, "box3".getBytes(), "gift3".getBytes());
        sessionOp.put(tName, "room1".getBytes(), "killer1".getBytes());
        sessionOp.put(tName, "box4".getBytes(), "gift4".getBytes());
        sessionOp.put(tName, "box5".getBytes(), "gift5".getBytes());
        sessionOp.put(tName, "room2".getBytes(), "killer2".getBytes());
        sessionOp.put(tName, "box6".getBytes(), "gift6".getBytes());
        sessionOp.put(tName, "boat1".getBytes(), "girl1".getBytes());
        sessionOp.put(tName, "box7".getBytes(), "gift7".getBytes());
        sessionOp.put(tName, "box8".getBytes(), "gift8".getBytes());
        sessionOp.commit();

        //prefix scan
        ScanIterator it = sessionOp.scan(tName, "bo".getBytes());
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + " : " + new String(col.value));
        }

    }

    // @Test
    public void testScan1() {
        System.out.println("RocksDBSessionTest::testScan1 test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t8";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "box1".getBytes(), "gift1".getBytes());
        sessionOp.put(tName, "box2".getBytes(), "gift2".getBytes());
        sessionOp.put(tName, "box3".getBytes(), "gift3".getBytes());
        sessionOp.put(tName, "room1".getBytes(), "killer1".getBytes());
        sessionOp.put(tName, "box4".getBytes(), "gift4".getBytes());
        sessionOp.put(tName, "box5".getBytes(), "gift5".getBytes());
        sessionOp.put(tName, "room2".getBytes(), "killer2".getBytes());
        sessionOp.put(tName, "box6".getBytes(), "gift6".getBytes());
        sessionOp.put(tName, "boat1".getBytes(), "girl1".getBytes());
        sessionOp.put(tName, "box7".getBytes(), "gift7".getBytes());
        sessionOp.put(tName, "box8".getBytes(), "gift8".getBytes());
        sessionOp.commit();

        //range scan
        ScanIterator it = sessionOp.scan(tName, "box2".getBytes(), "box5".getBytes(),
                                         ScanIterator.Trait.SCAN_GTE_BEGIN |
                                         ScanIterator.Trait.SCAN_LTE_END);
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + " : " + new String(col.value));
        }

    }

    // @Test
    public void testCount() {
        System.out.println("RocksDBSessionTest::testCount test");
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);
        String tName = "c1";
        rSession.checkTable(tName);

        int part1 = 1;
        int part2 = 2;
        int part3 = 3;
        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.prepare();
        sessionOp.put(tName, keyAppendPartId(part1, "k1".getBytes()), "v1".getBytes());
        sessionOp.put(tName, keyAppendPartId(part1, "k2".getBytes()), "v2".getBytes());
        sessionOp.put(tName, keyAppendPartId(part1, "k3".getBytes()), "v3".getBytes());
        sessionOp.put(tName, intToBytesForPartId(part2), "v".getBytes());
        sessionOp.put(tName, keyAppendPartId(part2, "k4".getBytes()), "v4".getBytes());
        sessionOp.put(tName, keyAppendPartId(part2, "k5".getBytes()), "v5".getBytes());
        sessionOp.put(tName, keyAppendPartId(part2, "k6".getBytes()), "v6".getBytes());
        sessionOp.put(tName, intToBytesForPartId(part3), "v".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k8".getBytes()), "v8".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k9".getBytes()), "v9".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k10".getBytes()), "v10".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k11".getBytes()), "v11".getBytes());
        sessionOp.commit();

//        assertEquals(3, rSession.sessionOp().keyCount(tName, getPartStartKey(part1),
//        getPartEndKey(part1)));
//        assertEquals(4, rSession.sessionOp().keyCount(tName, getPartStartKey(part2),
//        getPartEndKey(part2)));
//        assertEquals(5, rSession.sessionOp().keyCount(tName, getPartStartKey(part3),
//        getPartEndKey(part3)));

        assertEquals(12, rSession.sessionOp().keyCount("\0".getBytes(StandardCharsets.UTF_8),
                                                       "\255".getBytes(StandardCharsets.UTF_8),
                                                       tName));
        assertEquals(12, rSession.sessionOp().estimatedKeyCount(tName));
    }

    // @Test
    public void merge() {
        System.out.println("RocksDBSessionTest::merge test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "room1";
        rSession.checkTable(tName);
        byte[] x2 = longToByteArray(19);
        System.out.println(x2);
        SessionOperator sessionOp = rSession.sessionOp();
        byte[] x1 = sessionOp.get(tName, "p3".getBytes());
        String str1 = new String(x1);
        if (x1 != null) {
            long curValue = longFromByteArray(x1);
            System.out.println("current value:" + curValue);
            sessionOp.merge(tName, "p3".getBytes(), longToByteArray(10));
            sessionOp.commit();
        }

        byte[] value = rSession.sessionOp().get(tName, "p3".getBytes());
        final long longValue = longFromByteArray(value);
        System.out.println("after merge value: " + longValue);

        sessionOp.put(tName, "p3".getBytes(), "19".getBytes());
        sessionOp.commit();

        ScanIterator it = sessionOp.scan(tName, "p".getBytes());
        System.out.println("after put ------>");
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + ":" + new String(col.value));
        }

    }

    // @Test
    public void truncate() {
        System.out.println("RocksDBSessionTest::truncate test");
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", "gg1");
        rSession.checkTable("f1");
        SessionOperator op = rSession.sessionOp();
        op.put("f1", "m1".getBytes(), "n1".getBytes());
        op.put("f1", "m2".getBytes(), "n2".getBytes());
        op.put("f1", "m3".getBytes(), "n3".getBytes());
        op.commit();
        rSession.checkTable("f2");

        RocksDBSession rocksDBSession2 = rFactory.createGraphDB("./tmp", "gg2");
        rocksDBSession2.checkTable("txt");

        rSession.truncate();
        op = rSession.sessionOp();
        op.put("f1", "beijing".getBytes(), "renkou".getBytes());
        op.commit();

        rFactory.releaseAllGraphDB();
    }

    // @Test
    public void batchPut2() {
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", "gg1");
        rSession.checkTable("f1");
        SessionOperator op = rSession.sessionOp();
        try {
            op.put("f1", "m2".getBytes(), "xx2".getBytes());
            op.put("f1", "m1".getBytes(), "xx1".getBytes());
            op.put("f1", "m3".getBytes(), "xx3".getBytes());
            op.put("f1", "m4".getBytes(), "xx5".getBytes());
            op.commit();
            op.deleteRange("f1", "m5".getBytes(), "m2".getBytes());

            op.put("f2", new byte[]{1, 1}, new byte[]{1, -3});
            op.put("f2", new byte[]{1, -2}, new byte[]{2, 0});
            op.put("f2", new byte[]{1, 10}, new byte[]{1, 5});
            op.put("f2", new byte[]{1, 9}, new byte[]{1, 3});
            op.commit();

            op.deleteRange("f2", new byte[]{1, 1}, new byte[]{1, -1});
            op.commit();

            ScanIterator it = op.scan("f2");
            while (it.hasNext()) {
                RocksDBSession.BackendColumn col = it.next();
                System.out.println(Arrays.toString(col.name) + "=>" + Arrays.toString(col.value));
            }

        } catch (DBStoreException e) {
            System.out.println(e);
        }

    }

    // @Test
    public void doStuff() {
        byte[] stuff = new byte[]{1, 3, 1, -3};
        System.out.println(Arrays.toString(stuff));
    }

    private byte[] longToByteArray(long l) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(l);
        return buf.array();
    }

    private long longFromByteArray(byte[] a) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(a);
        buf.flip();
        return buf.getLong();
    }

}
