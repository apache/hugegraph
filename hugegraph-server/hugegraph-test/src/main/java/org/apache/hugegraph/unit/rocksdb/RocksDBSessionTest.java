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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Assume;
import org.junit.Test;
import org.rocksdb.RocksDBException;

public class RocksDBSessionTest extends BaseRocksDBUnitTest {

    @Test
    public void testPutAndGet() {
        String value = getString(this.rocks.session().get(TABLE, getBytes("person:1gname")));
        Assert.assertNull(value);

        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().put(TABLE, getBytes("person:1gage"), getBytes(19));
        this.rocks.session().put(TABLE, getBytes("person:1gcity"), getBytes("Beijing"));
        this.commit();

        value = getString(this.rocks.session().get(TABLE, getBytes("person:1gname")));
        Assert.assertEquals("James", value);

        long age = getLong(this.rocks.session().get(TABLE, getBytes("person:1gage")));
        Assert.assertEquals(19, age);

        value = getString(this.rocks.session().get(TABLE, getBytes("person:1gcity")));
        Assert.assertEquals("Beijing", value);
    }

    @Test
    public void testPutAndMultiGet() {
        BackendColumnIterator values =
                this.rocks.session()
                          .get(TABLE, Collections.singletonList(getBytes("person:1gname")));
        Assert.assertFalse(values.hasNext());

        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().put(TABLE, getBytes("person:1gage"), getBytes(19));
        this.rocks.session().put(TABLE, getBytes("person:1gcity"), getBytes("Beijing"));
        this.commit();

        values = this.rocks.session().get(TABLE, Arrays.asList(getBytes("person:1gname"),
                                                               getBytes("person:1gage")));
        Assert.assertTrue(values.hasNext());
        Assert.assertEquals("James", getString(values.next().value));
        Assert.assertEquals(19, getLong(values.next().value));
        Assert.assertFalse(values.hasNext());

        values = this.rocks.session().get(TABLE, Arrays.asList(
                getBytes("person:1gname"),
                getBytes("person:1gage"),
                getBytes("person:1gcity")));
        Assert.assertTrue(values.hasNext());
        Assert.assertEquals("James", getString(values.next().value));
        Assert.assertEquals(19, getLong(values.next().value));
        Assert.assertEquals("Beijing", getString(values.next().value));
        Assert.assertFalse(values.hasNext());

        values = this.rocks.session().get(TABLE, Arrays.asList(
                getBytes("person:1gname"),
                getBytes("person:1gage-non-exist"),
                getBytes("person:1gcity")));
        Assert.assertTrue(values.hasNext());
        Assert.assertEquals("James", getString(values.next().value));
        Assert.assertEquals("Beijing", getString(values.next().value));
        Assert.assertFalse(values.hasNext());

        values = this.rocks.session().get(TABLE, Arrays.asList(
                getBytes("person:1gname"),
                getBytes("person:1gage-non-exist"),
                getBytes("person:1gcity"),
                getBytes("person:1gname")));
        Assert.assertTrue(values.hasNext());
        Assert.assertEquals("James", getString(values.next().value));
        Assert.assertEquals("Beijing", getString(values.next().value));
        Assert.assertEquals("James", getString(values.next().value));
        Assert.assertFalse(values.hasNext());
    }

    @Test
    public void testPutAndGetWithMultiTables() throws RocksDBException {
        final String TABLE2 = "test-table2";

        this.rocks.createTable(TABLE2);
        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().put(TABLE2, getBytes("person:1gname"), getBytes("James2"));
        this.commit();

        String value = getString(this.rocks.session().get(TABLE, getBytes("person:1gname")));
        Assert.assertEquals("James", value);

        String value2 = getString(this.rocks.session().get(TABLE2, getBytes("person:1gname")));
        Assert.assertEquals("James2", value2);
    }

    @Test
    public void testMergeWithCounter() {
        this.rocks.session().put(TABLE, getBytes("person:1gage"), getBytes(19));
        this.commit();

        this.rocks.session().merge(TABLE, getBytes("person:1gage"), getBytes(1));
        this.commit();

        byte[] value = this.rocks.session().get(TABLE, getBytes("person:1gage"));
        Assert.assertEquals(20L, getLong(value));

        this.rocks.session().merge(TABLE, getBytes("person:1gage"), getBytes(123456789000L));
        this.commit();

        value = this.rocks.session().get(TABLE, getBytes("person:1gage"));
        Assert.assertEquals(123456789020L, getLong(value));

        this.rocks.session().put(TABLE, getBytes("person:1gage"), getBytes(250));
        this.commit();

        this.rocks.session().merge(TABLE, getBytes("person:1gage"), getBytes(10));
        this.commit();

        value = this.rocks.session().get(TABLE, getBytes("person:1gage"));
        Assert.assertEquals(260L, getLong(value));
    }

    @Test
    public void testMergeWithStringList() throws RocksDBException {
        Assume.assumeTrue("Not support string append now", false);

        this.rocks.session().put(TABLE, getBytes("person:1gphoneno"), getBytes("12306"));
        this.commit();

        this.rocks.session().merge(TABLE, getBytes("person:1gphoneno"), getBytes("12315"));
        this.commit();

        Assert.assertEquals("12306,12315", get("person:1gphoneno"));
    }

    @Test
    public void testScanByAll() {
        put("person:1gname", "James");
        put("person:2gname", "Lisa");

        Map<String, String> results = new HashMap<>();
        Session session = this.rocks.session();
        Iterator<BackendColumn> iter = session.scan(TABLE);
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }
        Assert.assertEquals(2, results.size());

        // add some keys then scan again
        put("person:3gname", "Tom");
        put("person:4gname", "Mike");

        results = new HashMap<>();
        iter = session.scan(TABLE);
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }
        Assert.assertEquals(4, results.size());

        // delete some keys then scan again
        this.rocks.session().delete(TABLE, getBytes("person:2gname"));
        this.rocks.session().commit();
        BaseUnitTest.runWithThreads(1, () -> {
            this.rocks.session().delete(TABLE, getBytes("person:3gname"));
            this.rocks.session().commit();
            this.rocks.close();
        });

        results = new HashMap<>();
        iter = session.scan(TABLE);
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }
        Assert.assertEquals(2, results.size());

        // delete some keys by prefix then scan again
        this.rocks.session().deletePrefix(TABLE, getBytes("person:1"));
        this.rocks.session().commit();

        results = new HashMap<>();
        iter = session.scan(TABLE);
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testScanByPrefix() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Map<String, String> results = new HashMap<>();
        Session session = this.rocks.session();
        Iterator<BackendColumn> iter = session.scan(TABLE, getBytes("person:1"));
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }

        Assert.assertEquals(3, results.size());
        Assert.assertEquals("James", results.get("person:1gname"));
        Assert.assertEquals("19", results.get("person:1gage"));
        Assert.assertEquals("Beijing", results.get("person:1gcity"));

        Assert.assertEquals("Lisa", get("person:2gname"));
    }

    @Test
    public void testScanByRange() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        put("person:3gname", "Hebe");
        put("person:3gage", "21");
        put("person:3gcity", "Taipei");

        Map<String, String> results = new HashMap<>();
        Session session = this.rocks.session();
        Iterator<BackendColumn> iter = session.scan(TABLE,
                                                    getBytes("person:1"),
                                                    getBytes("person:3"));
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }

        Assert.assertEquals(6, results.size());
        Assert.assertEquals("James", results.get("person:1gname"));
        Assert.assertEquals("19", results.get("person:1gage"));
        Assert.assertEquals("Beijing", results.get("person:1gcity"));

        Assert.assertEquals("Lisa", results.get("person:2gname"));
        Assert.assertEquals("20", results.get("person:2gage"));
        Assert.assertEquals("Beijing", results.get("person:2gcity"));

        Assert.assertEquals("Hebe", get("person:3gname"));
    }

    @Test
    public void testScanByRangeWithBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = getBytes("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 2};
        byte[] value12 = getBytes("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = getBytes("value-2-1");
        session.put(TABLE, key21, value21);

        byte[] key22 = new byte[]{2, 2};
        byte[] value22 = getBytes("value-2-2");
        session.put(TABLE, key22, value22);

        byte[] key23 = new byte[]{2, 3};
        byte[] value23 = getBytes("value-2-3");
        session.put(TABLE, key23, value23);

        this.commit();

        Map<ByteBuffer, byte[]> results = new HashMap<>();
        Iterator<BackendColumn> iter = session.scan(TABLE,
                                                    new byte[]{1, 0},
                                                    new byte[]{2, 3});
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(ByteBuffer.wrap(col.name), col.value);
        }

        Assert.assertEquals(4, results.size());
        Assert.assertArrayEquals(value11, results.get(ByteBuffer.wrap(key11)));
        Assert.assertArrayEquals(value12, results.get(ByteBuffer.wrap(key12)));
        Assert.assertArrayEquals(value21, results.get(ByteBuffer.wrap(key21)));
        Assert.assertArrayEquals(value22, results.get(ByteBuffer.wrap(key22)));

        Assert.assertArrayEquals(value23, session.get(TABLE, key23));
    }

    @Test
    public void testScanByRangeWithSignedBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = getBytes("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 2};
        byte[] value12 = getBytes("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key13 = new byte[]{1, -3};
        byte[] value13 = getBytes("value-1-3");
        session.put(TABLE, key13, value13);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = getBytes("value-2-1");
        session.put(TABLE, key21, value21);

        this.commit();

        Iterator<BackendColumn> iter;

        iter = session.scan(TABLE, new byte[]{1, -1}, new byte[]{1, 3});
        Assert.assertFalse(iter.hasNext());

        iter = session.scan(TABLE, new byte[]{1, 1}, new byte[]{1, -1});
        Map<ByteBuffer, byte[]> results = new HashMap<>();
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(ByteBuffer.wrap(col.name), col.value);
        }

        Assert.assertEquals(3, results.size());
        Assert.assertArrayEquals(value11, results.get(ByteBuffer.wrap(key11)));
        Assert.assertArrayEquals(value12, results.get(ByteBuffer.wrap(key12)));
        Assert.assertArrayEquals(value13, results.get(ByteBuffer.wrap(key13)));

        Assert.assertArrayEquals(value21, session.get(TABLE, key21));
    }

    @Test
    public void testUpdate() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        put("person:1gage", "20");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("20", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testDeleteByKey() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().delete(TABLE, getBytes("person:1gage"));
        this.commit();

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertNull(get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testDeleteByKeyButNotExist() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().delete(TABLE, getBytes("person:1"));
        this.commit();

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testDeleteByPrefix() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().deletePrefix(TABLE, getBytes("person:1"));
        this.commit();

        Assert.assertNull(get("person:1gname"));
        Assert.assertNull(get("person:1gage"));
        Assert.assertNull(get("person:1gcity"));

        Assert.assertEquals("Lisa", get("person:2gname"));
    }

    @Test
    public void testDeleteByRange() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        put("person:3gname", "Hebe");
        put("person:3gage", "21");
        put("person:3gcity", "Taipei");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("Lisa", get("person:2gname"));
        Assert.assertEquals("Hebe", get("person:3gname"));

        this.rocks.session().deleteRange(TABLE, getBytes("person:1"), getBytes("person:3"));
        this.commit();

        Assert.assertNull(get("person:1gname"));
        Assert.assertNull(get("person:1gage"));
        Assert.assertNull(get("person:1gcity"));

        Assert.assertNull(get("person:2gname"));
        Assert.assertNull(get("person:2gage"));
        Assert.assertNull(get("person:2gcity"));

        Assert.assertEquals("Hebe", get("person:3gname"));
        Assert.assertEquals("21", get("person:3gage"));
        Assert.assertEquals("Taipei", get("person:3gcity"));
    }

    @Test
    public void testDeleteByRangeWithBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = getBytes("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 2};
        byte[] value12 = getBytes("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = getBytes("value-2-1");
        session.put(TABLE, key21, value21);

        session.deleteRange(TABLE, key11, new byte[]{1, 3});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(value21, session.get(TABLE, key21));
    }

    @Test
    public void testDeleteByRangeWithSignedBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = getBytes("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, -2};
        byte[] value12 = getBytes("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = getBytes("value-2-1");
        session.put(TABLE, key21, value21);

        this.commit();

        // TODO: enable after fixed rocksdb issue #8239
        /*
        session.deleteRange(TABLE, new byte[]{1, -3}, new byte[]{1, 3});
        Assert.assertThrows(BackendException.class, () -> {
            this.commit();
        }, e -> {
            Assert.assertContains("end key comes before start key",
                                  e.getCause().getMessage());
        });
        */

        Assert.assertArrayEquals(value11, session.get(TABLE, key11));
        Assert.assertArrayEquals(value12, session.get(TABLE, key12));
        Assert.assertArrayEquals(value21, session.get(TABLE, key21));

        session.deleteRange(TABLE, new byte[]{1, 1}, new byte[]{1, -1});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(value21, session.get(TABLE, key21));
    }

    @Test
    public void testDeleteByRangeWithMinMaxByteValue() {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 0};
        byte[] value11 = getBytes("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 127};
        byte[] value12 = getBytes("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key13 = new byte[]{1, (byte) 0x80}; // 128
        byte[] value13 = getBytes("value-1-3");
        session.put(TABLE, key13, value13);

        byte[] key14 = new byte[]{1, (byte) 0xff}; // 255
        byte[] value14 = getBytes("value-1-4");
        session.put(TABLE, key14, value14);

        byte[] key20 = new byte[]{2, 0};
        byte[] value20 = getBytes("value-2-0");
        session.put(TABLE, key20, value20);

        session.deleteRange(TABLE,
                            new byte[]{1, 0}, new byte[]{1, (byte) 0xff});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(null, session.get(TABLE, key13));
        Assert.assertArrayEquals(value14, session.get(TABLE, key14));
        Assert.assertArrayEquals(value20, session.get(TABLE, key20));

        session.deleteRange(TABLE,
                            new byte[]{1, (byte) 0xff}, new byte[]{2, 0});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(null, session.get(TABLE, key13));
        Assert.assertArrayEquals(null, session.get(TABLE, key14));
        Assert.assertArrayEquals(value20, session.get(TABLE, key20));
    }

    @Test
    public void testDeleteSingle() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gname", "James2");

        Assert.assertEquals("James2", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));

        // deleteSingle after put once
        this.rocks.session().deleteSingle(TABLE, getBytes("person:1gage"));
        this.commit();

        Assert.assertEquals("James2", get("person:1gname"));
        Assert.assertNull(get("person:1gage"));

        // deleteSingle after put twice
        this.rocks.session().deleteSingle(TABLE, getBytes("person:1gname"));
        this.commit();

        // NOTE: maybe return "James" here
        Assert.assertNull(get("person:1gname"));
        Assert.assertTrue(null == get("person:1gname") ||
                          "James".equals(get("person:1gname")));
        Assert.assertNull(get("person:1gage"));
    }

    @Test
    public void testCompact() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().delete(TABLE, getBytes("person:1gage"));
        this.commit();

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertNull(get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().compactRange(TABLE);

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertNull(get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testProperty() {
        int count = new Random().nextInt(2048);
        for (int i = 0; i < count; i++) {
            put("key-" + i, "value" + i);
        }
        this.commit();

        String property = "rocksdb.estimate-num-keys";
        String numKeys = this.rocks.session().property(TABLE, property);
        Assert.assertEquals(String.valueOf(count), numKeys);
    }
}
