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

package org.apache.hugegraph.store.options;

import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.GIB;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MAX_AUTO;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIB;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIN_AUTO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Path;

import org.junit.Test;

public class RocksdbMemoryBudgetTest {

    private static final Path NO_MOUNTINFO = Path.of("/nonexistent/proc/self/mountinfo");

    @Test
    public void testExplicitValueIsKept() {
        assertEquals(32_000_000_000L, RocksdbMemoryBudget.resolve("32000000000"));
        assertEquals(4_000_000_000L, RocksdbMemoryBudget.resolve(" 4000000000 "));
        assertEquals(MIB, RocksdbMemoryBudget.resolve(String.valueOf(MIB)));
    }

    @Test
    public void testExplicitValueBelowOneMibIsRefused() {
        for (String value : new String[]{"1", "2", String.valueOf(MIB - 1)}) {
            try {
                RocksdbMemoryBudget.resolve(value);
                fail("accepted " + value);
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("at least"));
            }
        }
    }

    @Test
    public void testAutoValuesResolveWithinBounds() {
        for (String value : new String[]{null, "", "  ", "auto", "0", " 0 ", "00"}) {
            long budget = RocksdbMemoryBudget.resolve(value);
            assertTrue(String.valueOf(value), budget >= MIN_AUTO && budget <= MAX_AUTO);
        }
    }

    @Test
    public void testNonNumericValueIsRefused() {
        for (String value : new String[]{"4GB", "4g", "-1", "1.5e9", "99999999999999999999", "automatic"}) {
            try {
                RocksdbMemoryBudget.resolve(value);
                fail("accepted " + value);
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("number of bytes"));
            }
        }
    }

    @Test
    public void testAutoFitsContainerLimit() {
        // The setup from apache/hugegraph#3254: 4 GiB limit, 1 GiB heap, 512 MiB direct
        assertEquals(512 * MIB, RocksdbMemoryBudget.auto(4 * GIB, GIB, 512 * MIB));
        assertEquals(2560 * MIB, RocksdbMemoryBudget.auto(8 * GIB, GIB, 512 * MIB));
    }

    @Test
    public void testUnsetDirectMemoryCountsAsTheHeap() {
        assertEquals(512 * MIB, RocksdbMemoryBudget.directMemory(512 * MIB, 8 * GIB));
        assertEquals(4 * GIB, RocksdbMemoryBudget.directMemory(4 * GIB, 8 * GIB));
        // Unset: the JVM allows as much direct memory as heap, so the budget counts that
        assertEquals(8 * GIB, RocksdbMemoryBudget.directMemory(0L, 8 * GIB));
        assertEquals(512 * MIB, RocksdbMemoryBudget.directMemory(0L, 512 * MIB));
    }

    @Test
    public void testBareMetalBudgetsWithTheLauncherDirectCap() {
        // The default launcher picks a heap of at most 2 GiB and caps direct memory at it
        assertEquals(5376 * MIB, RocksdbMemoryBudget.auto(16 * GIB, 2 * GIB, 2 * GIB));
        // A raised heap with a smaller explicit -XX:MaxDirectMemorySize keeps a usable budget
        assertEquals(2304 * MIB, RocksdbMemoryBudget.auto(16 * GIB, 8 * GIB, 2 * GIB));
        assertEquals(12544 * MIB, RocksdbMemoryBudget.auto(64 * GIB, 36 * GIB, 2 * GIB));
        // Without one, direct memory counts as a second heap and the floor applies
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(
                64 * GIB, 36 * GIB, RocksdbMemoryBudget.directMemory(0L, 36 * GIB)));
    }

    @Test
    public void testUnreadableCgroupKeepsJvmMemory() {
        Path missing = Path.of("/nonexistent/proc/self/cgroup");
        assertEquals(-1L, CgroupMemoryLimit.read(missing, NO_MOUNTINFO,
                                                 Path.of("/nonexistent")).bytes);
        assertEquals(16 * GIB, RocksdbMemoryBudget.effectivePhysical(16 * GIB, -1L));
        assertEquals(4 * GIB, RocksdbMemoryBudget.effectivePhysical(-1L, 4 * GIB));
        assertEquals(4 * GIB, RocksdbMemoryBudget.effectivePhysical(4 * GIB, 8 * GIB));
    }

    @Test
    public void testAutoIsClamped() {
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(2 * GIB, GIB, GIB));
        assertEquals(MAX_AUTO, RocksdbMemoryBudget.auto(1024 * GIB, 8 * GIB, 8 * GIB));
    }

    @Test
    public void testUnknownPhysicalMemoryFallsBackToHeap() {
        assertEquals(2 * GIB, RocksdbMemoryBudget.auto(-1L, 2 * GIB, 2 * GIB));
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(0L, 64 * MIB, 64 * MIB));
    }

    @Test
    public void testAutoAliases() {
        for (String value : new String[]{null, "", "  ", "auto", "AUTO", " auto ", "0", "00"}) {
            assertEquals(String.valueOf(value), 0L, RocksdbMemoryBudget.parse(value));
        }
        assertEquals(4_000_000_000L, RocksdbMemoryBudget.parse(" 4000000000 "));
    }

    @Test
    public void testLogUnits() {
        assertEquals("1.5 GiB", RocksdbMemoryBudget.format(RocksdbMemoryBudget.RESERVED));
        assertEquals("512 MiB", RocksdbMemoryBudget.format(512 * MIB));
        assertEquals("4 GiB", RocksdbMemoryBudget.format(4 * GIB));
        assertEquals("29.8 GiB", RocksdbMemoryBudget.format(MAX_AUTO));
        assertEquals("1000 B", RocksdbMemoryBudget.format(1000));
        assertEquals("0 B", RocksdbMemoryBudget.format(0));
        assertEquals("unknown", RocksdbMemoryBudget.format(-1L));
    }

    @Test
    public void testReserveIsTheRaftLogCachePlusNativeMemory() {
        assertEquals(RaftRocksdbOptions.RAFT_LOG_CACHE_SIZE, RocksdbMemoryBudget.RAFT_LOG_RESERVE);
        assertEquals(1536 * MIB, RocksdbMemoryBudget.RESERVED);
    }

    @Test
    public void testPhysicalLimitSource() {
        CgroupMemoryLimit v2 = new CgroupMemoryLimit(4 * GIB, 2, false);
        assertEquals("cgroup-v2", RocksdbMemoryBudget.source(64 * GIB, v2));
        assertEquals("cgroup-v2", RocksdbMemoryBudget.source(-1L, v2));
        assertEquals("jvm", RocksdbMemoryBudget.source(2 * GIB, v2));
        assertEquals("jvm", RocksdbMemoryBudget.source(16 * GIB, CgroupMemoryLimit.NONE));
        assertEquals("unknown", RocksdbMemoryBudget.source(-1L, CgroupMemoryLimit.NONE));
    }
}
