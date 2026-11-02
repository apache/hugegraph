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
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class CgroupMemoryLimitTest {

    private static final Path NO_MOUNTINFO = Path.of("/nonexistent/proc/self/mountinfo");

    @Test
    public void testAncestorCgroupV2LimitIsUsed() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2");
        try {
            // A 4 GiB limit on the pod, none on the container or above the pod
            write(root, "kubepods/memory.max", "max");
            write(root, "kubepods/pod1/memory.max", String.valueOf(4 * GIB));
            write(root, "kubepods/pod1/ctr/memory.max", "max\n");
            Path proc = write(root, "proc-self-cgroup", "0::/kubepods/pod1/ctr\n");
            long limit = CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).bytes;
            assertEquals(4 * GIB, limit);
            assertEquals(2, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).version);

            // The case from review: a 64 GiB host reported by the JVM, -Xmx1g and
            // -XX:MaxDirectMemorySize=512m get the 4 GiB limit's budget, not host RAM's
            long physical = RocksdbMemoryBudget.effectivePhysical(64 * GIB, limit);
            assertEquals(512 * MIB, RocksdbMemoryBudget.auto(physical, GIB, 512 * MIB));

            // The container's own lower limit wins over the pod's
            write(root, "kubepods/pod1/ctr/memory.max", String.valueOf(2 * GIB));
            assertEquals(2 * GIB, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).bytes);
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
            assertEquals(-1L, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).bytes);
            write(root, "memory.max", String.valueOf(3 * GIB));
            assertEquals(3 * GIB, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).bytes);

            // A path the mount doesn't show falls back to the mount root
            Path hidden = write(root, "proc-hidden", "0::/system.slice/hidden.service\n");
            assertEquals(3 * GIB, CgroupMemoryLimit.read(hidden, NO_MOUNTINFO, root).bytes);
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
            assertEquals(4 * GIB, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).bytes);
            assertEquals(1, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).version);

            write(root, "memory/memory.stat", "hierarchical_memory_limit 9223372036854771712\n");
            assertEquals(-1L, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, root).bytes);
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
            long limit = CgroupMemoryLimit.read(proc, mountinfo, root).bytes;
            assertEquals(4 * GIB, limit);
            long physical = RocksdbMemoryBudget.effectivePhysical(64 * GIB, limit);
            assertEquals(512 * MIB, RocksdbMemoryBudget.auto(physical, GIB, 512 * MIB));
            // Only a subtree is mounted, so anything above /kubepods is invisible
            assertTrue(CgroupMemoryLimit.read(proc, mountinfo, root).incomplete);

            // Without the mount root the path is looked up under kubepods/ and missed
            assertEquals(-1L, CgroupMemoryLimit.read(proc, NO_MOUNTINFO, mount).bytes);
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testStricterLimitAboveCgroupV2SubtreeMount() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-above-subtree");
        try {
            // /outer is limited to 2 GiB, the pod to 4 GiB, kubepods and the container not
            Path full = root.resolve("full");
            write(full, "outer/memory.max", String.valueOf(2 * GIB));
            write(full, "outer/kubepods/memory.max", "max");
            write(full, "outer/kubepods/pod1/memory.max", String.valueOf(4 * GIB));
            write(full, "outer/kubepods/pod1/ctr/memory.max", "max");
            // The same hierarchy mounted from /outer/kubepods hides the 2 GiB on /outer
            Path sub = root.resolve("sub");
            write(sub, "memory.max", "max");
            write(sub, "pod1/memory.max", String.valueOf(4 * GIB));
            write(sub, "pod1/ctr/memory.max", "max");
            Path proc = write(root, "proc-self-cgroup", "0::/outer/kubepods/pod1/ctr\n");
            String fullLine = "31 25 0:26 / " + full + " rw - cgroup2 cgroup2 rw\n";
            String subLine = "32 25 0:26 /outer/kubepods " + sub +
                             " rw - cgroup2 cgroup2 rw\n";

            // Subtree only: the 2 GiB limit above the mount is missed, and that is flagged
            Path subOnly = write(root, "mountinfo-sub", subLine);
            CgroupMemoryLimit limit =
                    CgroupMemoryLimit.read(proc, subOnly, root);
            assertEquals(4 * GIB, limit.bytes);
            assertTrue(limit.incomplete);

            // With a readable full-hierarchy view in either order the 2 GiB is found
            for (String order : new String[]{subLine + fullLine, fullLine + subLine}) {
                Path mountinfo = write(root, "mountinfo", order);
                limit = CgroupMemoryLimit.read(proc, mountinfo, root);
                assertEquals(order, 2 * GIB, limit.bytes);
                assertFalse(order, limit.incomplete);
            }

            // A "/" mount that doesn't show the process's path doesn't count as a full view
            Path empty = root.resolve("empty-full");
            write(empty, "memory.max", "max");
            Path unreadable = write(root, "mountinfo-unreadable", subLine +
                                    "33 25 0:26 / " + empty + " rw - cgroup2 cgroup2 rw\n");
            limit = CgroupMemoryLimit.read(proc, unreadable, root);
            assertEquals(4 * GIB, limit.bytes);
            assertTrue(limit.incomplete);
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
            assertEquals(4 * GIB, CgroupMemoryLimit.read(proc, mountinfo, root).bytes);
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
                long limit = CgroupMemoryLimit.read(proc, mountinfo, root).bytes;
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
                             CgroupMemoryLimit.read(proc, mountinfo, root).bytes);
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
            CgroupMemoryLimit limit =
                    CgroupMemoryLimit.read(proc, mountinfo, root);
            assertTrue(limit.incomplete);
            assertEquals(-1L, limit.bytes);

            // A limit at the visible mount root still bounds the result
            write(mount, "memory.max", String.valueOf(8 * GIB));
            limit = CgroupMemoryLimit.read(proc, mountinfo, root);
            assertTrue(limit.incomplete);
            assertEquals(8 * GIB, limit.bytes);

            // v1 with the same layout
            Path v1 = root.resolve("memory");
            write(v1, "memory.limit_in_bytes", "9223372036854771712");
            write(v1, "memory.stat", "hierarchical_memory_limit " + (8 * GIB) + "\n");
            Path proc1 = write(root, "proc-v1", "4:memory:/\n");
            Path mountinfo1 = write(root, "mountinfo-v1",
                                    "45 25 0:30 /.. " + v1 + " rw - cgroup cgroup rw,memory\n");
            limit = CgroupMemoryLimit.read(proc1, mountinfo1, root);
            assertTrue(limit.incomplete);
            assertEquals(8 * GIB, limit.bytes);
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testNamespaceRootViewIsFlagged() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-ns-root");
        try {
            // A private cgroup namespace: the process is at "/", the cgroup2 mount made
            // inside the namespace is rooted at "/", and its own memory.max is "max". A
            // stricter limit on the parent of the namespace root can't be seen from here
            Path mount = root.resolve("sys-fs-cgroup");
            write(mount, "memory.max", "max");
            Path proc = write(root, "proc-self-cgroup", "0::/\n");
            Path mountinfo = write(root, "mountinfo",
                                   "30 25 0:26 / " + mount + " rw - cgroup2 cgroup2 rw\n");
            CgroupMemoryLimit limit =
                    CgroupMemoryLimit.read(proc, mountinfo, root);
            assertEquals(-1L, limit.bytes);
            assertTrue(limit.incomplete);

            // A finite limit at the namespace root still applies, but a stricter one on a
            // hidden ancestor (say 1 GiB above this 2 GiB) can't be ruled out
            write(mount, "memory.max", String.valueOf(2 * GIB));
            limit = CgroupMemoryLimit.read(proc, mountinfo, root);
            assertEquals(2 * GIB, limit.bytes);
            assertTrue(limit.incomplete);

            // v1 reports the whole hierarchy in hierarchical_memory_limit, so it isn't flagged
            Path v1 = root.resolve("memory");
            write(v1, "memory.limit_in_bytes", "9223372036854771712");
            write(v1, "memory.stat", "hierarchical_memory_limit 9223372036854771712\n");
            Path proc1 = write(root, "proc-v1", "4:memory:/\n");
            Path mountinfo1 = write(root, "mountinfo-v1",
                                    "45 25 0:30 / " + v1 + " rw - cgroup cgroup rw,memory\n");
            assertFalse(CgroupMemoryLimit.read(proc1, mountinfo1, root).incomplete);
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testMatchedOrMissingMountsAreNotFlagged() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2-flag");
        try {
            // A host service: the full hierarchy is mounted and every ancestor is readable
            Path mount = root.resolve("sys-fs-cgroup");
            write(mount, "memory.max", "max");
            write(mount.resolve("system.slice"), "memory.max", "max");
            write(mount.resolve("system.slice/hg.service"), "memory.max",
                  String.valueOf(4 * GIB));
            Path proc = write(root, "proc-self-cgroup", "0::/system.slice/hg.service\n");
            Path mountinfo = write(root, "mountinfo",
                                   "30 25 0:26 / " + mount + " rw - cgroup2 cgroup2 rw\n");
            CgroupMemoryLimit limit =
                    CgroupMemoryLimit.read(proc, mountinfo, root);
            assertFalse(limit.incomplete);
            assertEquals(4 * GIB, limit.bytes);
            // Without any mountinfo entry the default layout is used and nothing is flagged
            assertFalse(CgroupMemoryLimit.read(proc, NO_MOUNTINFO, mount).incomplete);
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testMountRootContainsMembership() {
        assertTrue(CgroupMemoryLimit.contains("/", "/kubepods/pod1/ctr"));
        assertTrue(CgroupMemoryLimit.contains("/kubepods", "/kubepods/pod1/ctr"));
        assertTrue(CgroupMemoryLimit.contains("/kubepods/", "/kubepods/pod1/ctr"));
        assertTrue(CgroupMemoryLimit.contains("/kubepods/pod1/ctr", "/kubepods/pod1/ctr"));
        assertFalse(CgroupMemoryLimit.contains("/system.slice", "/kubepods/pod1/ctr"));
        assertFalse(CgroupMemoryLimit.contains("/kubepods", "/kubepodsx/pod1"));
    }

    @Test
    public void testMembershipIsRebasedOntoMountRoot() {
        assertEquals("pod1/ctr", CgroupMemoryLimit.rebase("/kubepods/pod1/ctr", "/kubepods"));
        assertEquals("pod1/ctr", CgroupMemoryLimit.rebase("/kubepods/pod1/ctr", "/kubepods/"));
        assertEquals("", CgroupMemoryLimit.rebase("/kubepods", "/kubepods"));
        assertEquals("a/b", CgroupMemoryLimit.rebase("/a/b", "/"));
        assertEquals("", CgroupMemoryLimit.rebase("/", "/"));
        // A path outside the mount root is kept and then falls back to the mount
        assertEquals("other/x", CgroupMemoryLimit.rebase("/other/x", "/kubepods"));
        assertEquals("kubepodsx/y", CgroupMemoryLimit.rebase("/kubepodsx/y", "/kubepods"));
    }

    private static Path write(Path root, String relative, String content) throws IOException {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
