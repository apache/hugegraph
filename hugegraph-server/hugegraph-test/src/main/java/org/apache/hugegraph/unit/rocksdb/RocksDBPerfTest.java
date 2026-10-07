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

package org.apache.hugegraph.unit.rocksdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import org.junit.Test;

public class RocksDBPerfTest extends BaseRocksDBUnitTest {

    private static final int TIMES = 10000 * 1000;

    @Test
    public void testSeekExistKey() {
        put("exist", "value");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            Iterator<BackendColumn> iter = session.scan(TABLE, getBytes("exist"));
            while (iter.hasNext()) {
                iter.next();
            }
        }
    }

    @Test
    public void testSeekNonExistKey() {
        put("exist", "value");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            Iterator<BackendColumn> iter = session.scan(TABLE, getBytes("non-exist"));
            while (iter.hasNext()) {
                iter.next();
            }
        }
    }

    @Test
    public void testGetExistKey() {
        put("exist", "value");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            byte[] value = session.get(TABLE, getBytes("exist"));
            assert value.length == "value".length();
        }
    }

    @Test
    public void testGetNonExistKey() {
        put("exist", "value");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            byte[] value = session.get(TABLE, getBytes("non-exist"));
            assert value == null;
        }
    }

    @Test
    public void testPut() {
        for (int i = 0; i < TIMES; i++) {
            put("person-" + i, "value-" + i);
        }
    }

    @Test
    public void testGet3Keys() {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            session.get(TABLE, getBytes("person:1gname"));
            session.get(TABLE, getBytes("person:1gage"));
            session.get(TABLE, getBytes("person:1gcity"));
        }
    }

    @Test
    public void testMultiGet3Keys() {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Session session = this.rocks.session();
        BackendColumnIterator iter;
        for (int i = 0; i < TIMES; i++) {
            List<byte[]> keys = Arrays.asList(getBytes("person:1gname"),
                                              getBytes("person:1gage"),
                                              getBytes("person:1gcity"));
            iter = session.get(TABLE, keys);
            iter.next();
            iter.next();
            iter.next();
        }
    }

    @Test
    public void testGet1KeyWithMultiValues() {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        put("person:2all", "name=Lisa,age=20,city=Beijing");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            session.get(TABLE, getBytes("person:2all"));
        }
    }

    @Test
    public void testScanByPrefix() {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            Iterator<BackendColumn> iter = session.scan(TABLE, getBytes("person:1"));
            while (iter.hasNext()) {
                iter.next();
            }
        }
    }

    @Test
    public void testGet3KeysWithData() {
        testPut();
        testGet3Keys();
    }

    @Test
    public void testMultiGet3KeysWithData() {
        testPut();
        testMultiGet3Keys();
    }

    @Test
    public void testGet1KeyWithData() {
        testPut();
        testGet1KeyWithMultiValues();
    }

    @Test
    public void testScanByPrefixWithData() {
        testPut();
        testScanByPrefix();
    }

    @Test
    public void testUpdate() {
        Session session = this.rocks.session();

        Random r = new Random();
        Map<Integer, Integer> comms = new HashMap<>();
        byte[] empty = new byte[0];

        int n = 1000;
        for (int i = 0; i < n; i++) {
            int value = i;
            comms.put(i, value);
            String key = String.format("index:%3d:%d", i, value);
            session.put(TABLE, getBytes(key), empty);
        }
        session.commit();

        int updateTimes = 300; // 30w
        for (int j = 0; j < updateTimes; j++) {
            for (int i = 0; i < n; i++) {
                int value = comms.get(i);
                String old = String.format("index:%3d:%d", i, value);
                session.delete(TABLE, getBytes(old));

                value = r.nextInt(n); // TODO: aggregate
                value = i + 1;
                comms.put(i, value);
                String key = String.format("index:%3d:%d", i, value);
                session.put(TABLE, getBytes(key), empty);
            }
            session.commit();
        }
    }

    @Test
    public void testScanByPrefixAfterUpdate() {
        Session session = this.rocks.session();

        this.testUpdate();

        int n = 1000;
        int queryTimes = 300; // 30w
        for (int j = 0; j < queryTimes; j++) {
            for (int i = 0; i < n; i++) {
                String key = String.format("index:%3d", i);
                Iterator<BackendColumn> iter = session.scan(TABLE, getBytes(key));
                while (iter.hasNext()) {
                    iter.next();
                }
            }
        }
    }
}
