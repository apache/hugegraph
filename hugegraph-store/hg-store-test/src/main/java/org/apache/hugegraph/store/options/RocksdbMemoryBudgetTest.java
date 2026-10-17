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

import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MAX_AUTO;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIB;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIN_AUTO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.junit.Test;

public class RocksdbMemoryBudgetTest {

    private static final long GIB = 1024 * MIB;
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
    public void testUnsetZeroOrBlankIsAuto() {
        for (String value : new String[]{null, "", "  ", "0", " 0 ", "00"}) {
            long budget = RocksdbMemoryBudget.resolve(value);
            assertTrue(String.valueOf(value), budget >= MIN_AUTO && budget <= MAX_AUTO);
        }
    }

    @Test
    public void testNonNumericValueIsRefused() {
        for (String value : new String[]{"4GB", "4g", "-1", "1.5e9", "99999999999999999999"}) {
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
    public void testAncestorCgroupV2LimitIsUsed() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2");
        try {
            // A 4 GiB limit on the pod, none on the container or above the pod
            write(root, "kubepods/memory.max", "max");
            write(root, "kubepods/pod1/memory.max", String.valueOf(4 * GIB));
            write(root, "kubepods/pod1/ctr/memory.max", "max\n");
            Path proc = write(root, "proc-self-cgroup", "0::/kubepods/pod1/ctr\n");
            long limit = RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, root);
            assertEquals(4 * GIB, limit);

            // The case from review: a 64 GiB host reported by the JVM, -Xmx1g and
            // -XX:MaxDirectMemorySize=512m get the 4 GiB limit's budget, not host RAM's
            long physical = RocksdbMemoryBudget.effectivePhysical(64 * GIB, limit);
            assertEquals(512 * MIB, RocksdbMemoryBudget.auto(physical, GIB, 512 * MIB));

            // The container's own lower limit wins over the pod's
            write(root, "kubepods/pod1/ctr/memory.max", String.valueOf(2 * GIB));
            assertEquals(2 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV2NamespaceRootAndNoLimit() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2");
        try {
            // Inside a cgroup namespace the process sees "/" and the mount is its cgroup
            Path proc = write(root, "proc-self-cgroup", "0::/\n");
            write(root, "memory.max", "max");
            assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, root));
            write(root, "memory.max", String.valueOf(3 * GIB));
            assertEquals(3 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, root));

            // A path the mount doesn't show falls back to the mount root
            Path hidden = write(root, "proc-hidden", "0::/system.slice/hidden.service\n");
            assertEquals(3 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(hidden, NO_MOUNTINFO, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV1HierarchicalLimitIsUsed() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v1");
        try {
            Path proc = write(root, "proc-self-cgroup",
                              "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n");
            // Docker mounts the container's own memory cgroup, so /docker/abc isn't there
            write(root, "memory/memory.limit_in_bytes", "9223372036854771712");
            write(root, "memory/memory.stat",
                  "cache 0\nhierarchical_memory_limit " + (4 * GIB) + "\n");
            assertEquals(4 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, root));

            write(root, "memory/memory.stat", "hierarchical_memory_limit 9223372036854771712\n");
            assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV2SubtreeMountIsRebased() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-subtree");
        try {
            // cgroupfs mounted from /kubepods: the pod's limit is at <mount>/pod1, not at
            // <mount>/kubepods/pod1, and the mount root itself is unlimited
            Path mount = root.resolve("sys-fs-cgroup");
            write(mount, "memory.max", "max");
            write(mount, "pod1/memory.max", String.valueOf(4 * GIB));
            write(mount, "pod1/ctr/memory.max", "max");
            Path proc = write(root, "proc-self-cgroup", "0::/kubepods/pod1/ctr\n");
            Path mountinfo = write(root, "mountinfo",
                                   "25 1 8:1 / / rw - ext4 /dev/sda1 rw\n" +
                                   "30 25 0:26 /kubepods " + mount +
                                   " rw,nosuid shared:4 - cgroup2 cgroup2 rw\n");
            long limit = RocksdbMemoryBudget.cgroupMemoryLimit(proc, mountinfo, root);
            assertEquals(4 * GIB, limit);
            long physical = RocksdbMemoryBudget.effectivePhysical(64 * GIB, limit);
            assertEquals(512 * MIB, RocksdbMemoryBudget.auto(physical, GIB, 512 * MIB));

            // Without the mount root the path is looked up under kubepods/ and missed
            assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(proc, NO_MOUNTINFO, mount));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV1SubtreeMountAndEscapedMountPoint() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v1-subtree");
        try {
            Path mount = root.resolve("memory dir");
            write(mount, "memory.limit_in_bytes", "9223372036854771712");
            write(mount, "memory.stat", "hierarchical_memory_limit " + (4 * GIB) + "\n");
            Path proc = write(root, "proc-self-cgroup", "4:memory:/docker/abc\n");
            // mountinfo writes the space in the mount point as \040
            Path mountinfo = write(root, "mountinfo",
                                   "40 25 0:30 /docker/abc " +
                                   mount.toString().replace(" ", "\\040") +
                                   " rw,nosuid - cgroup cgroup rw,memory\n");
            assertEquals(4 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, mountinfo, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV2MountOrderDoesNotHideAncestorLimit() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-mounts");
        try {
            // A full view of the hierarchy with the 4 GiB limit on the pod
            Path full = root.resolve("full");
            write(full, "kubepods/pod1/memory.max", String.valueOf(4 * GIB));
            write(full, "kubepods/pod1/ctr/memory.max", "max");
            // A leaf-only view of the container and an unrelated subtree, both unlimited
            Path leaf = root.resolve("leaf");
            write(leaf, "memory.max", "max");
            Path other = root.resolve("other");
            write(other, "memory.max", "max");
            Path proc = write(root, "proc-self-cgroup", "0::/kubepods/pod1/ctr\n");
            String fullLine = "31 25 0:26 / " + full + " rw - cgroup2 cgroup2 rw\n";
            String leafLine = "32 25 0:26 /kubepods/pod1/ctr " + leaf + " rw - cgroup2 cgroup2 rw\n";
            String otherLine = "33 25 0:26 /system.slice " + other + " rw - cgroup2 cgroup2 rw\n";
            for (String order : new String[]{otherLine + leafLine + fullLine,
                                             fullLine + leafLine + otherLine,
                                             leafLine + otherLine + fullLine}) {
                Path mountinfo = write(root, "mountinfo", order);
                long limit = RocksdbMemoryBudget.cgroupMemoryLimit(proc, mountinfo, root);
                assertEquals(order, 4 * GIB, limit);
                long physical = RocksdbMemoryBudget.effectivePhysical(64 * GIB, limit);
                assertEquals(512 * MIB, RocksdbMemoryBudget.auto(physical, GIB, 512 * MIB));
            }
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV1MountOrderDoesNotHideAncestorLimit() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v1-mounts");
        try {
            Path full = root.resolve("full");
            write(full, "kubepods/pod1/ctr/memory.limit_in_bytes", "9223372036854771712");
            write(full, "kubepods/pod1/ctr/memory.stat",
                  "hierarchical_memory_limit " + (4 * GIB) + "\n");
            Path other = root.resolve("other");
            write(other, "memory.limit_in_bytes", "9223372036854771712");
            Path proc = write(root, "proc-self-cgroup", "4:memory:/kubepods/pod1/ctr\n");
            String fullLine = "41 25 0:30 / " + full + " rw - cgroup cgroup rw,memory\n";
            String otherLine = "42 25 0:30 /system.slice " + other + " rw - cgroup cgroup rw,memory\n";
            // A cpu-only mount is not the memory controller and is ignored
            String cpuLine = "43 25 0:31 / " + other + " rw - cgroup cgroup rw,cpu,cpuacct\n";
            for (String order : new String[]{otherLine + cpuLine + fullLine,
                                             fullLine + otherLine + cpuLine}) {
                Path mountinfo = write(root, "mountinfo", order);
                assertEquals(order, 4 * GIB,
                             RocksdbMemoryBudget.cgroupMemoryLimit(proc, mountinfo, root));
            }
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testInheritedNamespaceMountIsFlaggedNotTreatedAsUnlimited() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-namespace");
        try {
            // cgroup_namespaces(7): /proc/self/cgroup says "/" while a cgroup2 mount made
            // outside the namespace has root "/..". The namespace root is a child with a
            // 2 GiB limit whose name can't be learned from inside; its parent is unlimited
            Path mount = root.resolve("sys-fs-cgroup");
            write(mount, "memory.max", "max");
            write(mount, "ns-child/memory.max", String.valueOf(2 * GIB));
            Path proc = write(root, "proc-self-cgroup", "0::/\n");
            Path mountinfo = write(root, "mountinfo",
                                   "35 25 0:26 /.. " + mount + " rw - cgroup2 cgroup2 rw\n");
            RocksdbMemoryBudget.CgroupLimit limit =
                    RocksdbMemoryBudget.cgroupLimit(proc, mountinfo, root);
            assertTrue(limit.unmatched);
            assertEquals(-1L, limit.bytes);

            // A limit at the visible mount root still bounds the result
            write(mount, "memory.max", String.valueOf(8 * GIB));
            limit = RocksdbMemoryBudget.cgroupLimit(proc, mountinfo, root);
            assertTrue(limit.unmatched);
            assertEquals(8 * GIB, limit.bytes);

            // v1 with the same layout
            Path v1 = root.resolve("memory");
            write(v1, "memory.limit_in_bytes", "9223372036854771712");
            write(v1, "memory.stat", "hierarchical_memory_limit " + (8 * GIB) + "\n");
            Path proc1 = write(root, "proc-v1", "4:memory:/\n");
            Path mountinfo1 = write(root, "mountinfo-v1",
                                    "45 25 0:30 /.. " + v1 + " rw - cgroup cgroup rw,memory\n");
            limit = RocksdbMemoryBudget.cgroupLimit(proc1, mountinfo1, root);
            assertTrue(limit.unmatched);
            assertEquals(8 * GIB, limit.bytes);
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testMatchedOrMissingMountsAreNotFlagged() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-flag");
        try {
            Path mount = root.resolve("sys-fs-cgroup");
            write(mount, "memory.max", String.valueOf(4 * GIB));
            Path proc = write(root, "proc-self-cgroup", "0::/\n");
            Path mountinfo = write(root, "mountinfo",
                                   "30 25 0:26 / " + mount + " rw - cgroup2 cgroup2 rw\n");
            RocksdbMemoryBudget.CgroupLimit limit =
                    RocksdbMemoryBudget.cgroupLimit(proc, mountinfo, root);
            assertFalse(limit.unmatched);
            assertEquals(4 * GIB, limit.bytes);
            // Without any mountinfo entry the default layout is used and nothing is flagged
            assertFalse(RocksdbMemoryBudget.cgroupLimit(proc, NO_MOUNTINFO, mount).unmatched);
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testMountRootContainsMembership() {
        assertTrue(RocksdbMemoryBudget.contains("/", "/kubepods/pod1/ctr"));
        assertTrue(RocksdbMemoryBudget.contains("/kubepods", "/kubepods/pod1/ctr"));
        assertTrue(RocksdbMemoryBudget.contains("/kubepods/", "/kubepods/pod1/ctr"));
        assertTrue(RocksdbMemoryBudget.contains("/kubepods/pod1/ctr", "/kubepods/pod1/ctr"));
        assertFalse(RocksdbMemoryBudget.contains("/system.slice", "/kubepods/pod1/ctr"));
        assertFalse(RocksdbMemoryBudget.contains("/kubepods", "/kubepodsx/pod1"));
    }

    @Test
    public void testMembershipIsRebasedOntoMountRoot() {
        assertEquals("pod1/ctr", RocksdbMemoryBudget.rebase("/kubepods/pod1/ctr", "/kubepods"));
        assertEquals("pod1/ctr", RocksdbMemoryBudget.rebase("/kubepods/pod1/ctr", "/kubepods/"));
        assertEquals("", RocksdbMemoryBudget.rebase("/kubepods", "/kubepods"));
        assertEquals("a/b", RocksdbMemoryBudget.rebase("/a/b", "/"));
        assertEquals("", RocksdbMemoryBudget.rebase("/", "/"));
        // A path outside the mount root is kept and then falls back to the mount
        assertEquals("other/x", RocksdbMemoryBudget.rebase("/other/x", "/kubepods"));
        assertEquals("kubepodsx/y", RocksdbMemoryBudget.rebase("/kubepodsx/y", "/kubepods"));
    }

    @Test
    public void testUnreadableCgroupKeepsJvmMemory() {
        Path missing = Path.of("/nonexistent/proc/self/cgroup");
        assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(missing, NO_MOUNTINFO, Path.of("/nonexistent")));
        assertEquals(16 * GIB, RocksdbMemoryBudget.effectivePhysical(16 * GIB, -1L));
        assertEquals(4 * GIB, RocksdbMemoryBudget.effectivePhysical(-1L, 4 * GIB));
        assertEquals(4 * GIB, RocksdbMemoryBudget.effectivePhysical(4 * GIB, 8 * GIB));
    }

    private static Path write(Path root, String relative, String content) throws IOException {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    @Test
    public void testAutoIsClamped() {
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(2 * GIB, GIB, GIB));
        assertEquals(MAX_AUTO, RocksdbMemoryBudget.auto(1024 * GIB, 8 * GIB, 8 * GIB));
    }

    @Test
    public void testDisruptorBufferSizeIsNeverZero() {
        assertEquals(32, RocksdbMemoryBudget.disruptorBufferSize(512 * MIB));
        assertEquals(32, RocksdbMemoryBudget.disruptorBufferSize(1_000_000_000L));
        assertEquals(1024, RocksdbMemoryBudget.disruptorBufferSize(32_000_000_000L));
    }

    @Test
    public void testDerivedWriteBufferSizePassesRocksdbValidation() {
        // The 4 GiB container from apache/hugegraph#3254 auto-sizes to 512 MiB, whose
        // thousandth is under the 1 MiB rocksdb.write_buffer_size minimum
        long budget = RocksdbMemoryBudget.auto(4 * GIB, GIB, 512 * MIB);
        assertEquals(MIB, RocksdbMemoryBudget.writeBufferSize(budget));
        assertEquals(MIB, RocksdbMemoryBudget.writeBufferSize(MIB));
        assertEquals(32_000_000L, RocksdbMemoryBudget.writeBufferSize(32_000_000_000L));

        // The same validation BusinessHandlerImpl.initRocksdb() runs at Store startup
        OptionSpace.register("rocksdb", RocksDBOptions.class.getName());
        RocksDBOptions.instance();
        HugeConfig config = new HugeConfig(rocksdbConfig(budget,
                                                         RocksdbMemoryBudget.writeBufferSize(budget)));
        assertEquals(MIB, (long) config.get(RocksDBOptions.MEMTABLE_SIZE));
        try {
            new HugeConfig(rocksdbConfig(budget, budget / 1000));
            fail("accepted a write buffer under 1 MiB");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("write_buffer_size"));
        }
    }

    private static Map<String, Object> rocksdbConfig(long totalMemory, long writeBufferSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.total_memory_size", String.valueOf(totalMemory));
        config.put("rocksdb.write_buffer_size", String.valueOf(writeBufferSize));
        return config;
    }

    @Test
    public void testUnknownPhysicalMemoryFallsBackToHeap() {
        assertEquals(2 * GIB, RocksdbMemoryBudget.auto(-1L, 2 * GIB, 2 * GIB));
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(0L, 64 * MIB, 64 * MIB));
    }
}
