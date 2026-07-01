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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBTables;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

public class RocksDBTableQueryByIdsTest extends BaseRocksDBUnitTest {

    private static final String DATABASE = "db";

    private TestVertexTable vertexTable;
    private TestEdgeTable edgeOutTable;
    private TestEdgeTable edgeInTable;

    @Override
    @Before
    public void setup() throws RocksDBException {
        super.setup();
        this.vertexTable = new TestVertexTable(DATABASE);
        this.edgeOutTable = new TestEdgeTable(true, DATABASE);
        this.edgeInTable = new TestEdgeTable(false, DATABASE);
        this.rocks.createTable(this.vertexTable.table());
        this.rocks.createTable(this.edgeOutTable.table());
        this.rocks.createTable(this.edgeInTable.table());
    }

    @Test
    public void testVertexQueryByIdsWithAllExistingIds() {
        Id id1 = IdGenerator.of("v1");
        Id id2 = IdGenerator.of("v2");
        Id id3 = IdGenerator.of("v3");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.rocks.session().put(this.vertexTable.table(), id2.asBytes(), getBytes("value2"));
        this.rocks.session().put(this.vertexTable.table(), id3.asBytes(), getBytes("value3"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2, id3);
        BackendColumnIterator iter = this.vertexTable.queryByIds(this.rocks.session(), ids);

        Map<String, String> results = toResultMap(iter);

        Assert.assertEquals(3, results.size());
        Assert.assertEquals("value1", results.get("v1"));
        Assert.assertEquals("value2", results.get("v2"));
        Assert.assertEquals("value3", results.get("v3"));
    }

    @Test
    public void testVertexQueryByIdsWithExistingAndMissingIdsMixed() {
        Id id1 = IdGenerator.of("v1");
        Id id2 = IdGenerator.of("v2");
        Id id3 = IdGenerator.of("v3");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.rocks.session().put(this.vertexTable.table(), id3.asBytes(), getBytes("value3"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2, id3);
        BackendColumnIterator iter = this.vertexTable.queryByIds(this.rocks.session(), ids);

        Map<String, String> results = toResultMap(iter);

        Assert.assertEquals(2, results.size());
        Assert.assertEquals("value1", results.get("v1"));
        Assert.assertEquals("value3", results.get("v3"));
        Assert.assertFalse(results.containsKey("v2"));
    }

    @Test
    public void testVertexQueryByIdsDuplicateIds() {
        Id id1 = IdGenerator.of("v1");
        Id id2 = IdGenerator.of("v2");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.rocks.session().put(this.vertexTable.table(), id2.asBytes(), getBytes("value2"));
        this.commit();

        // [id1, id2, id1] — non-consecutive duplicates must be preserved
        List<Id> ids = Arrays.asList(id1, id2, id1);
        BackendColumnIterator iter = this.vertexTable.queryByIds(this.rocks.session(), ids);

        List<String> names = toColumnNames(iter);

        Assert.assertEquals(3, names.size());
        Assert.assertEquals("v1", names.get(0));
        Assert.assertEquals("v2", names.get(1));
        Assert.assertEquals("v1", names.get(2));
    }

    @Test
    public void testEdgeOutQueryByIdsWithAllExistingIds() {
        Id id1 = IdGenerator.of("e1");
        Id id2 = IdGenerator.of("e2");

        this.rocks.session().put(this.edgeOutTable.table(), id1.asBytes(), getBytes("edge-value1"));
        this.rocks.session().put(this.edgeOutTable.table(), id2.asBytes(), getBytes("edge-value2"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2);
        BackendColumnIterator iter = this.edgeOutTable.queryByIds(this.rocks.session(), ids);

        Map<String, String> results = toResultMap(iter);

        Assert.assertEquals(2, results.size());
        Assert.assertEquals("edge-value1", results.get("e1"));
        Assert.assertEquals("edge-value2", results.get("e2"));
    }

    @Test
    public void testEdgeInQueryByIdsWithAllExistingIds() {
        Id id1 = IdGenerator.of("e1");
        Id id2 = IdGenerator.of("e2");

        this.rocks.session().put(this.edgeInTable.table(), id1.asBytes(), getBytes("edge-value1"));
        this.rocks.session().put(this.edgeInTable.table(), id2.asBytes(), getBytes("edge-value2"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2);
        BackendColumnIterator iter = this.edgeInTable.queryByIds(this.rocks.session(), ids);

        Map<String, String> results = toResultMap(iter);

        Assert.assertEquals(2, results.size());
        Assert.assertEquals("edge-value1", results.get("e1"));
        Assert.assertEquals("edge-value2", results.get("e2"));
    }

    @Test
    public void testVertexQueryByIdsFailsWhenHasChanges() {
        Id id1 = IdGenerator.of("v1");
        Id id2 = IdGenerator.of("v2");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.rocks.session().put(this.vertexTable.table(), id2.asBytes(), getBytes("value2"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2);
        RocksDBSessions.Session mockSession = new DelegatingSession(this.rocks.session()) {
            @Override
            public boolean hasChanges() {
                return true;
            }
        };

        Assert.assertThrows(IllegalStateException.class, () -> {
            this.vertexTable.queryByIds(mockSession, ids);
        }, e -> Assert.assertContains("Can't queryByIds()", e.getMessage()));
    }

    @Test
    public void testPublicQueryMultiIdsFails() {
        Id id1 = IdGenerator.of("v1");
        Id id2 = IdGenerator.of("v2");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.rocks.session().put(this.vertexTable.table(), id2.asBytes(), getBytes("value2"));
        this.commit();

        Set<Id> idSet = new LinkedHashSet<>(Arrays.asList(id1, id2));
        IdQuery query = new IdQuery(HugeType.VERTEX, idSet);

        RocksDBSessions.Session mockSession = new DelegatingSession(this.rocks.session()) {
            @Override
            public boolean hasChanges() {
                return true;
            }
        };

        Assert.assertThrows(IllegalStateException.class, () -> {
            this.vertexTable.query(mockSession, query);
        }, e -> Assert.assertContains("Can't queryByIds()", e.getMessage()));
    }

    @Test
    public void testPublicQuerySingleIdFails() {
        Id id1 = IdGenerator.of("v1");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.commit();

        Set<Id> idSet = new LinkedHashSet<>(Arrays.asList(id1));
        IdQuery query = new IdQuery(HugeType.VERTEX, idSet);

        RocksDBSessions.Session mockSession = new DelegatingSession(this.rocks.session()) {
            @Override
            public boolean hasChanges() {
                return true;
            }
        };

        Assert.assertThrows(IllegalStateException.class, () -> {
            this.vertexTable.query(mockSession, query);
        }, e -> Assert.assertContains("Can't queryByIds()", e.getMessage()));
    }

    private Map<String, String> toResultMap(BackendColumnIterator iter) {
        Map<String, String> results = new HashMap<>();
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }
        return results;
    }

    private List<String> toColumnNames(BackendColumnIterator iter) {
        List<String> names = new ArrayList<>();
        while (iter.hasNext()) {
            names.add(getString(iter.next().name));
        }
        return names;
    }

    /**
     * A session wrapper that delegates all operations to an underlying session.
     * Subclasses can override specific methods for mocking purposes.
     */
    private static class DelegatingSession extends RocksDBSessions.Session {

        private final RocksDBSessions.Session delegate;

        DelegatingSession(RocksDBSessions.Session delegate) {
            this.delegate = delegate;
        }

        @Override
        public String dataPath() {
            return this.delegate.dataPath();
        }

        @Override
        public String walPath() {
            return this.delegate.walPath();
        }

        @Override
        public String property(String table, String property) {
            return this.delegate.property(table, property);
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            return this.delegate.keyRange(table);
        }

        @Override
        public void compactRange(String table) {
            this.delegate.compactRange(table);
        }

        @Override
        public void put(String table, byte[] key, byte[] value) {
            this.delegate.put(table, key, value);
        }

        @Override
        public void merge(String table, byte[] key, byte[] value) {
            this.delegate.merge(table, key, value);
        }

        @Override
        public void increase(String table, byte[] key, byte[] value) {
            this.delegate.increase(table, key, value);
        }

        @Override
        public void delete(String table, byte[] key) {
            this.delegate.delete(table, key);
        }

        @Override
        public void deleteSingle(String table, byte[] key) {
            this.delegate.deleteSingle(table, key);
        }

        @Override
        public void deletePrefix(String table, byte[] key) {
            this.delegate.deletePrefix(table, key);
        }

        @Override
        public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) {
            this.delegate.deleteRange(table, keyFrom, keyTo);
        }

        @Override
        public byte[] get(String table, byte[] key) {
            return this.delegate.get(table, key);
        }

        @Override
        public BackendColumnIterator get(String table, List<byte[]> keys) {
            return this.delegate.get(table, keys);
        }

        @Override
        public BackendColumnIterator scan(String table) {
            return this.delegate.scan(table);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            return this.delegate.scan(table, prefix);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            return this.delegate.scan(table, keyFrom, keyTo, scanType);
        }

        @Override
        public Object commit() {
            return this.delegate.commit();
        }

        @Override
        public void rollback() {
            this.delegate.rollback();
        }

        @Override
        public boolean hasChanges() {
            return this.delegate.hasChanges();
        }

        @Override
        public void open() {
            this.delegate.open();
        }

        @Override
        public void close() {
            this.delegate.close();
        }
    }

    /**
     * Subclass that exposes the protected queryByIds for testing.
     */
    private static class TestVertexTable extends RocksDBTables.Vertex {

        public TestVertexTable(String database) {
            super(database);
        }

        @Override
        public BackendColumnIterator queryByIds(RocksDBSessions.Session session,
                                                Collection<Id> ids) {
            return super.queryByIds(session, ids);
        }
    }

    /**
     * Subclass that exposes the protected queryByIds for testing.
     */
    private static class TestEdgeTable extends RocksDBTables.Edge {

        public TestEdgeTable(boolean out, String database) {
            super(out, database);
        }

        @Override
        public BackendColumnIterator queryByIds(RocksDBSessions.Session session,
                                                Collection<Id> ids) {
            return super.queryByIds(session, ids);
        }
    }
}
