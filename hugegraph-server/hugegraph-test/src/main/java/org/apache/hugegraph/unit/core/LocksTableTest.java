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

package org.apache.hugegraph.unit.core;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.concurrent.LockManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.LockUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class LocksTableTest extends BaseUnitTest {

    private LockUtil.LocksTable locksTable;
    private static String GRAPH = "graph";

    @BeforeClass
    public static void setup() {
        genLockGroup(GRAPH, "group");
        genLockGroup(GRAPH, "group1");
    }

    @Before
    public void initLockTable() {
        this.locksTable = new LockUtil.LocksTable(GRAPH);
    }

    @AfterClass
    public static void teardown() {
        destroyLockGroup(GRAPH, "group");
        destroyLockGroup(GRAPH, "group1");
    }

    @Test
    public void testLocksTable() {
        this.locksTable.lockReads("group", IdGenerator.of(1));
        LockUtil.Locks locks = Whitebox.getInternalState(this.locksTable,
                                                         "locks");
        Map<String, Set<Id>> table = Whitebox.getInternalState(this.locksTable,
                                                               "table");

        Assert.assertEquals(1, table.size());
        Assert.assertTrue(table.containsKey("group"));
        Set<Id> ids = table.get("group");
        Assert.assertEquals(1, ids.size());
        Assert.assertTrue(ids.contains(IdGenerator.of(1)));

        List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
        Assert.assertEquals(1, lockList.size());

        this.locksTable.unlock();

        Assert.assertEquals(0, table.size());
        Assert.assertEquals(0, lockList.size());
    }

    @Test
    public void testLocksTableWithLockKey() {
        genLockGroup(GRAPH, LockUtil.KEY_LOCK);
        this.locksTable.lockKey("group", IdGenerator.of(1));
        LockUtil.Locks locks = Whitebox.getInternalState(this.locksTable,
                                                         "locks");
        Map<String, Set<Id>> table = Whitebox.getInternalState(this.locksTable,
                                                               "table");

        Assert.assertEquals(1, table.size());
        Assert.assertTrue(table.containsKey("group"));
        Set<Id> ids = table.get("group");
        Assert.assertEquals(1, ids.size());
        Assert.assertTrue(ids.contains(IdGenerator.of(1)));

        List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
        Assert.assertEquals(1, lockList.size());

        this.locksTable.unlock();

        Assert.assertEquals(0, table.size());
        Assert.assertEquals(0, lockList.size());

        Id id1 = IdGenerator.of(1);
        Id id2 = IdGenerator.of(2);
        Id id3 = IdGenerator.of(3);
        Id id4 = IdGenerator.of(4);
        this.locksTable.lockKeys("group",
                                 ImmutableSet.of(id1, id2, id3, id4));
        this.locksTable.lockKeys("group1",
                                 ImmutableSet.of(id1, id2));
        locks = Whitebox.getInternalState(this.locksTable, "locks");
        table = Whitebox.getInternalState(this.locksTable, "table");

        Assert.assertEquals(2, table.size());
        Assert.assertTrue(table.containsKey("group"));
        Assert.assertTrue(table.containsKey("group1"));
        ids = table.get("group");
        Assert.assertEquals(4, ids.size());
        Set<Id> expect = ImmutableSet.of(id1, id2, id3, id4);
        Assert.assertEquals(expect, ids);
        ids = table.get("group1");
        Assert.assertEquals(2, ids.size());
        expect = ImmutableSet.of(id1, id2);
        Assert.assertEquals(expect, ids);

        lockList = Whitebox.getInternalState(locks, "lockList");
        Assert.assertEquals(6, lockList.size());

        this.locksTable.unlock();

        Assert.assertEquals(0, table.size());
        Assert.assertEquals(0, lockList.size());
        destroyLockGroup(GRAPH, LockUtil.KEY_LOCK);
    }

    @Test
    public void testLocksTableSameGroupDifferentLocks() {
        Id id1 = IdGenerator.of(1);
        Id id2 = IdGenerator.of(2);
        Id id3 = IdGenerator.of(3);
        Id id4 = IdGenerator.of(4);
        this.locksTable.lockReads("group", id1);
        this.locksTable.lockReads("group", id2);
        this.locksTable.lockReads("group", id3);
        this.locksTable.lockReads("group", id4);
        this.locksTable.lockReads("group", id1);
        this.locksTable.lockReads("group", id3);
        this.locksTable.lockReads("group", id1);
        LockUtil.Locks locks = Whitebox.getInternalState(this.locksTable,
                                                         "locks");
        Map<String, Set<Id>> table = Whitebox.getInternalState(this.locksTable,
                                                               "table");

        Assert.assertEquals(1, table.size());
        Assert.assertTrue(table.containsKey("group"));
        Set<Id> ids = table.get("group");
        Assert.assertEquals(4, ids.size());
        Set<Id> expect = ImmutableSet.of(id1, id2, id3, id4);
        Assert.assertEquals(expect, ids);

        List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
        Assert.assertEquals(4, lockList.size());

        this.locksTable.unlock();

        Assert.assertEquals(0, table.size());
        Assert.assertEquals(0, lockList.size());
    }

    @Test
    public void testLocksTableDifferentGroupDifferentLocks() {
        Id id1 = IdGenerator.of(1);
        Id id2 = IdGenerator.of(2);
        Id id3 = IdGenerator.of(3);
        Id id4 = IdGenerator.of(4);
        this.locksTable.lockReads("group", id1);
        this.locksTable.lockReads("group", id2);
        this.locksTable.lockReads("group", id3);
        this.locksTable.lockReads("group", id4);
        this.locksTable.lockReads("group1", id1);
        this.locksTable.lockReads("group1", id2);
        this.locksTable.lockReads("group1", id1);
        LockUtil.Locks locks = Whitebox.getInternalState(this.locksTable,
                                                         "locks");
        Map<String, Set<Id>> table = Whitebox.getInternalState(this.locksTable,
                                                               "table");

        Assert.assertEquals(2, table.size());
        Assert.assertTrue(table.containsKey("group"));
        Assert.assertTrue(table.containsKey("group1"));
        Set<Id> ids = table.get("group");
        Assert.assertEquals(4, ids.size());
        Set<Id> expect = ImmutableSet.of(id1, id2, id3, id4);
        Assert.assertEquals(expect, ids);
        ids = table.get("group1");
        Assert.assertEquals(2, ids.size());
        expect = ImmutableSet.of(id1, id2);
        Assert.assertEquals(expect, ids);

        List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
        Assert.assertEquals(6, lockList.size());

        this.locksTable.unlock();

        Assert.assertEquals(0, table.size());
        Assert.assertEquals(0, lockList.size());
    }

    @Test
    public void testLockTableSameLock1MillionTimes() {
        // ReadLock max lock times is 65535, this test ensures that there is no
        // repeated locking in LocksTable
        Id id = IdGenerator.of(1);
        for (int i = 0; i < 1000000; i++) {
            this.locksTable.lockReads("group", id);
        }
        LockUtil.Locks locks = Whitebox.getInternalState(this.locksTable,
                                                         "locks");
        Map<String, Set<Id>> table = Whitebox.getInternalState(this.locksTable,
                                                               "table");

        Assert.assertEquals(1, table.size());
        Assert.assertTrue(table.containsKey("group"));
        Set<Id> ids = table.get("group");
        Assert.assertEquals(1, ids.size());
        Set<Id> expect = ImmutableSet.of(id);
        Assert.assertEquals(expect, ids);

        List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
        Assert.assertEquals(1, lockList.size());

        this.locksTable.unlock();

        Assert.assertEquals(0, table.size());
        Assert.assertEquals(0, lockList.size());
    }

    @Test
    public void testLockWithMultiThreads() {
        runWithThreads(10, () -> {
            LockUtil.LocksTable locksTable = new LockUtil.LocksTable(GRAPH);
            for (int i = 0; i < 10000; i++) {
                locksTable.lockReads("group", IdGenerator.of(i));
            }
            LockUtil.Locks locks = Whitebox.getInternalState(locksTable,
                                                             "locks");
            Map<String, Set<Id>> table = Whitebox.getInternalState(locksTable,
                                                                   "table");

            Assert.assertEquals(1, table.size());
            Assert.assertTrue(table.containsKey("group"));
            Set<Id> ids = table.get("group");
            Assert.assertEquals(10000, ids.size());

            List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
            Assert.assertEquals(10000, lockList.size());

            locksTable.unlock();
            Assert.assertEquals(0, table.size());
            Assert.assertEquals(0, lockList.size());
        });
    }

    @Test
    public void testSameLockWithMultiThreads() {
        Id id = IdGenerator.of(1);
        runWithThreads(10, () -> {
            LockUtil.LocksTable locksTable = new LockUtil.LocksTable(GRAPH);
            for (int i = 0; i < 10000; i++) {
                locksTable.lockReads("group", id);
            }
            LockUtil.Locks locks = Whitebox.getInternalState(locksTable,
                                                             "locks");
            Map<String, Set<Id>> table = Whitebox.getInternalState(locksTable,
                                                                   "table");

            Assert.assertEquals(1, table.size());
            Assert.assertTrue(table.containsKey("group"));
            Set<Id> ids = table.get("group");
            Assert.assertEquals(1, ids.size());
            Set<Id> expect = ImmutableSet.of(id);
            Assert.assertEquals(expect, ids);

            List<Lock> lockList = Whitebox.getInternalState(locks, "lockList");
            Assert.assertEquals(1, lockList.size());
            locksTable.unlock();
            Assert.assertEquals(0, table.size());
            Assert.assertEquals(0, lockList.size());
        });
    }

    protected static void genLockGroup(String graph, String group) {
        LockManager.instance().create(groupName(graph, group));
    }

    protected static void destroyLockGroup(String graph, String group) {
        LockManager.instance().destroy(groupName(graph, group));
    }

    protected static String groupName(String graph, String group) {
        return String.join("_", graph, group);
    }
}
