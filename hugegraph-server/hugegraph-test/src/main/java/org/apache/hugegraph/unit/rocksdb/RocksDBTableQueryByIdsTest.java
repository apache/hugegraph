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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBTables;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

public class RocksDBTableQueryByIdsTest extends BaseRocksDBUnitTest {

    private static final String DATABASE = "db";

    private TestVertexTable vertexTable;
    private TestEdgeTable edgeTable;

    @Override
    @Before
    public void setup() throws RocksDBException {
        super.setup();
        this.vertexTable = new TestVertexTable(DATABASE);
        this.edgeTable = new TestEdgeTable(DATABASE);
        this.rocks.createTable(this.vertexTable.table());
        this.rocks.createTable(this.edgeTable.table());
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
    public void testVertexQueryByIdsWithDuplicateIds() {
        Id id1 = IdGenerator.of("v1");
        Id id2 = IdGenerator.of("v2");

        this.rocks.session().put(this.vertexTable.table(), id1.asBytes(), getBytes("value1"));
        this.rocks.session().put(this.vertexTable.table(), id2.asBytes(), getBytes("value2"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2, id1);
        BackendColumnIterator iter = this.vertexTable.queryByIds(this.rocks.session(), ids);

        Map<String, Integer> countMap = new HashMap<>();
        Map<String, String> results = new HashMap<>();
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            String key = getString(col.name);
            results.put(key, getString(col.value));
            countMap.put(key, countMap.getOrDefault(key, 0) + 1);
        }

        Assert.assertEquals(2, results.size());
        Assert.assertEquals("value1", results.get("v1"));
        Assert.assertEquals("value2", results.get("v2"));
        // Verify duplicate ids produce duplicate results
        Assert.assertEquals(Integer.valueOf(2), countMap.get("v1"));
        Assert.assertEquals(Integer.valueOf(1), countMap.get("v2"));
    }

    @Test
    public void testEdgeQueryByIdsWithAllExistingIds() {
        Id id1 = IdGenerator.of("e1");
        Id id2 = IdGenerator.of("e2");

        this.rocks.session().put(this.edgeTable.table(), id1.asBytes(), getBytes("edge-value1"));
        this.rocks.session().put(this.edgeTable.table(), id2.asBytes(), getBytes("edge-value2"));
        this.commit();

        List<Id> ids = Arrays.asList(id1, id2);
        BackendColumnIterator iter = this.edgeTable.queryByIds(this.rocks.session(), ids);

        Map<String, String> results = toResultMap(iter);

        Assert.assertEquals(2, results.size());
        Assert.assertEquals("edge-value1", results.get("e1"));
        Assert.assertEquals("edge-value2", results.get("e2"));
    }

    /**
     * NOTE: Testing the fallback path (session.hasChanges() == true) is not
     * feasible here because both the optimized multi-get path and the fallback
     * scan-based path ultimately delegate to session.get() / session.scan(),
     * which have a pre-existing assertion `assert !this.hasChanges()` in
     * RocksDBStdSessions. This assertion is disabled in production but fires
     * during unit tests when assertions are enabled. The dispatch logic itself
     * is covered by the implementation in RocksDBTables.Vertex/Edge.queryByIds().
     */

    private Map<String, String> toResultMap(BackendColumnIterator iter) {
        Map<String, String> results = new HashMap<>();
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(getString(col.name), getString(col.value));
        }
        return results;
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

        public TestEdgeTable(String database) {
            super(true, database);
        }

        @Override
        public BackendColumnIterator queryByIds(RocksDBSessions.Session session,
                                                Collection<Id> ids) {
            return super.queryByIds(session, ids);
        }
    }
}
