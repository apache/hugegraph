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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.OperatingSystemMXBean;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolves rocksdb.total_memory_size, the budget RaftRocksdbOptions splits into the
 * write buffer manager's cache and the block cache. Both caches are off-heap, so an
 * unset or "0" budget is sized from the memory left after the heap, direct memory and
 * the fixed raft log cache, instead of a fixed number that can exceed a container limit.
 */
@Slf4j
public final class RocksdbMemoryBudget {

    public static final String KEY = "total_memory_size";

    static final long MIB = 1024L * 1024L;
    // The 1 GiB raft log block cache (RaftRocksdbOptions) plus thread stacks, metaspace
    // and other native allocations that sit outside the budget
    static final long RESERVED = 1024 * MIB + 512 * MIB;
    static final long MIN_AUTO = 256 * MIB;
    // RaftRocksdbOptions gives the write buffer manager 2/3 of the budget, and a 0-byte
    // write buffer manager means no memtable limit at all
    static final long MIN_EXPLICIT = MIB;
    // The value shipped before auto-sizing; a large host never gets more than it had
    static final long MAX_AUTO = 32_000_000_000L;

    private RocksdbMemoryBudget() {
    }

    /**
     * Returns the configured budget in bytes, or an automatic one when the value is
     * missing, blank or 0.
     */
    public static long resolve(String configured) {
        long value = isAuto(configured) ? 0L : parse(configured);
        long heap = Runtime.getRuntime().maxMemory();
        CgroupLimit cgroup = cgroupLimit(Paths.get("/proc/self/cgroup"),
                                         Paths.get("/proc/self/mountinfo"),
                                         Paths.get("/sys/fs/cgroup"));
        long physical = effectivePhysical(jvmMemory(), cgroup.bytes);
        if (value > 0) {
            if (value < MIN_EXPLICIT) {
                throw new IllegalArgumentException(String.format(
                        "rocksdb.total_memory_size must be 0 (auto) or at least %s bytes, " +
                        "got %s", MIN_EXPLICIT, value));
            }
            if (value < MIN_AUTO) {
                log.warn("rocksdb.total_memory_size={} is below the {} bytes auto-sizing " +
                         "never goes under", value, MIN_AUTO);
            }
            if (value == MAX_AUTO) {
                // Most likely an application-pd.yml kept from before auto-sizing
                log.warn("rocksdb.total_memory_size={} is the fixed value shipped before " +
                         "auto-sizing; set it to 0 or remove it to size the budget from " +
                         "the available memory", value);
            }
            long direct = maxDirectMemory(heap);
            long left = physical - heap - direct - RESERVED;
            if (physical > 0 && value > left) {
                log.warn("rocksdb.total_memory_size={} exceeds the {} bytes left after the " +
                         "heap, direct memory and the {} bytes reserve (physical={}, heap={}, " +
                         "direct={}); the process may be OOM-killed",
                         value, left, RESERVED, physical, heap, direct);
            }
            return value;
        }
        long direct = maxDirectMemory(heap);
        long budget = auto(physical, heap, direct);
        log.info("rocksdb.total_memory_size is unset or 0, auto-sizing to {} bytes " +
                 "(physical={}, heap={}, direct={})", budget, physical, heap, direct);
        if (cgroup.unmatched) {
            log.warn("No cgroup mount in /proc/self/mountinfo contains this process's " +
                     "cgroup (an inherited mount in a cgroup namespace), so only the " +
                     "limits visible at the mount root are counted and a tighter limit on " +
                     "the process's own cgroup may be missed; set rocksdb.total_memory_size " +
                     "or HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE explicitly");
        }
        if (physical > 0 && physical - heap - direct - RESERVED < 2 * MIN_AUTO) {
            log.warn("Heap, direct memory and the {} bytes reserve leave too little of the " +
                     "{} bytes physical memory, rocksdb gets the {} bytes floor; lower -Xmx " +
                     "or -XX:MaxDirectMemorySize", RESERVED, physical, MIN_AUTO);
        }
        return budget;
    }

    static boolean isAuto(String configured) {
        return configured == null || configured.trim().isEmpty();
    }

    static long parse(String configured) {
        try {
            long value = Long.parseLong(configured.trim());
            if (value < 0) {
                throw new NumberFormatException("negative");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
                    "rocksdb.total_memory_size must be a number of bytes, got '%s'",
                    configured), e);
        }
    }

    /**
     * Half of what is left after the heap, direct memory and the reserve, clamped to
     * [MIN_AUTO, MAX_AUTO]. Falls back to the heap size, the previous default, when
     * physical memory is unknown.
     */
    static long auto(long physical, long heap, long direct) {
        if (physical <= 0) {
            return Math.min(Math.max(heap, MIN_AUTO), MAX_AUTO);
        }
        long available = physical - heap - direct - RESERVED;
        return Math.min(Math.max(available / 2, MIN_AUTO), MAX_AUTO);
    }

    /**
     * The raft disruptor buffer size AppConfig derives from the budget when
     * raft.disruptorBufferSize is 0: the budget in GB, at least 1, rounded to a power
     * of two, times 32.
     */
    public static int disruptorBufferSize(long totalMemory) {
        // A budget under 1 GB would otherwise give log(0) and a zero-sized buffer
        int size = (int) Math.max(1, totalMemory / 1000 / 1000 / 1000);
        return (int) Math.pow(2, Math.round(Math.log(size) / Math.log(2))) * 32;
    }

    /**
     * The write_buffer_size AppConfig derives when the key is unset or 0: a thousandth
     * of the budget, but never below the 1 MiB rocksdb.write_buffer_size accepts
     * (RocksDBOptions.MEMTABLE_SIZE), which an auto-sized budget under 1 GiB would
     * otherwise fall under.
     */
    public static long writeBufferSize(long totalMemory) {
        return Math.max(totalMemory / 1000, MIB);
    }

    private static long jvmMemory() {
        try {
            // Reports the cgroup v1/v2 memory limit inside a container, but older JDK 17
            // updates may not see a limit set only on a parent cgroup
            OperatingSystemMXBean os = ManagementFactory.getPlatformMXBean(
                    OperatingSystemMXBean.class);
            return os == null ? -1L : os.getTotalMemorySize();
        } catch (RuntimeException | LinkageError e) {
            log.warn("Failed to read physical memory size", e);
            return -1L;
        }
    }

    /**
     * A cgroup memory limit in bytes (-1 for none), and whether a cgroup mount existed
     * but none of them contained this process's cgroup, in which case bytes only covers
     * what is visible at those mounts' roots.
     */
    static final class CgroupLimit {

        final long bytes;
        final boolean unmatched;

        CgroupLimit(long bytes, boolean unmatched) {
            this.bytes = bytes;
            this.unmatched = unmatched;
        }
    }

    /**
     * The smaller of what the JVM reports and the cgroup limit, ignoring unknown (<= 0)
     * values.
     */
    static long effectivePhysical(long jvm, long cgroup) {
        if (cgroup <= 0) {
            return jvm;
        }
        return jvm <= 0 ? cgroup : Math.min(jvm, cgroup);
    }

    /**
     * The lowest memory limit on this process's cgroup or any visible ancestor, or -1
     * when there is none or it can't be read. The membership path from /proc/self/cgroup
     * is rebased onto every mount of the controller in /proc/self/mountinfo whose root
     * contains it, so a cgroupfs mounted from a subtree (root /kubepods at /sys/fs/cgroup)
     * is walked from the right directory, and the lowest limit over all those mounts is
     * taken: a leaf-only or unrelated mount listed first can't hide a fuller view listed
     * later. Without a matching mount the default root is used. cgroup v2
     * walks memory.max from the process's cgroup up to the mount; cgroup v1 reads
     * hierarchical_memory_limit, which already accounts for ancestors. A limit above the
     * mount root, as inside a private cgroup namespace, stays invisible.
     */
    static long cgroupMemoryLimit(Path procSelfCgroup, Path mountinfo, Path defaultRoot) {
        return cgroupLimit(procSelfCgroup, mountinfo, defaultRoot).bytes;
    }

    static CgroupLimit cgroupLimit(Path procSelfCgroup, Path mountinfo, Path defaultRoot) {
        List<String[]> v2Mounts = cgroupMounts(mountinfo, true);
        List<String[]> v1Mounts = cgroupMounts(mountinfo, false);
        List<String> lines;
        try {
            lines = Files.readAllLines(procSelfCgroup, StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            return new CgroupLimit(-1L, false);
        }
        long limit = -1L;
        boolean unmatched = false;
        for (String line : lines) {
            String[] parts = line.split(":", 3);
            if (parts.length < 3) {
                continue;
            }
            boolean v2 = parts[0].equals("0") && parts[1].isEmpty();
            if (!v2 && !("," + parts[1] + ",").contains(",memory,")) {
                continue;
            }
            List<String[]> mounts = v2 ? v2Mounts : v1Mounts;
            boolean matched = false;
            for (String[] mount : mounts) {
                if (!contains(mount[0], parts[2])) {
                    // A mount of another subtree says nothing about this cgroup
                    continue;
                }
                matched = true;
                Path point = Paths.get(mount[1]);
                String relative = rebase(parts[2], mount[0]);
                limit = lowest(limit, v2 ? cgroupV2Limit(point, relative) :
                                      cgroupV1Limit(point, relative));
            }
            if (!matched && !mounts.isEmpty()) {
                // In a cgroup namespace /proc/self/cgroup says "/" while a mount made
                // outside it has a root such as "/..": the process's own cgroup can't be
                // located from here. The limits at each mount root still bound it.
                unmatched = true;
                for (String[] mount : mounts) {
                    Path point = Paths.get(mount[1]);
                    limit = lowest(limit, v2 ? readLimit(point.resolve("memory.max")) :
                                          cgroupV1Limit(point, ""));
                }
            } else if (!matched) {
                String relative = rebase(parts[2], "/");
                limit = lowest(limit, v2 ? cgroupV2Limit(defaultRoot, relative) :
                                      cgroupV1Limit(defaultRoot.resolve("memory"), relative));
            }
        }
        return new CgroupLimit(limit, unmatched);
    }

    /**
     * The {root, mount point} of every cgroup v2 mount, or of every cgroup v1 mount
     * carrying the memory controller, in /proc/self/mountinfo order.
     */
    private static List<String[]> cgroupMounts(Path mountinfo, boolean v2) {
        List<String[]> mounts = new ArrayList<>();
        List<String> lines;
        try {
            lines = Files.readAllLines(mountinfo, StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            return mounts;
        }
        for (String line : lines) {
            // id parent major:minor root mount-point options [optional...] - type source super
            int separator = line.indexOf(" - ");
            if (separator < 0) {
                continue;
            }
            String[] fields = line.substring(0, separator).split(" ");
            String[] tail = line.substring(separator + 3).split(" ");
            if (fields.length < 5 || tail.length < 1) {
                continue;
            }
            boolean match = v2 ? tail[0].equals("cgroup2") :
                            tail[0].equals("cgroup") && tail.length >= 3 &&
                            ("," + tail[2] + ",").contains(",memory,");
            if (match) {
                mounts.add(new String[]{unescape(fields[3]), unescape(fields[4])});
            }
        }
        return mounts;
    }

    // Whether a mount whose root is mountRoot shows the cgroup at membership
    static boolean contains(String mountRoot, String membership) {
        String path = membership.startsWith("/") ? membership : "/" + membership;
        String root = mountRoot.endsWith("/") && mountRoot.length() > 1 ?
                      mountRoot.substring(0, mountRoot.length() - 1) : mountRoot;
        return root.equals("/") || path.equals(root) || path.startsWith(root + "/");
    }

    /**
     * The membership path relative to a mount whose root is mountRoot: /kubepods/pod1/ctr
     * under root /kubepods is pod1/ctr. A path outside the mount root is kept as is.
     */
    static String rebase(String membership, String mountRoot) {
        String path = membership.startsWith("/") ? membership : "/" + membership;
        String root = mountRoot.endsWith("/") && mountRoot.length() > 1 ?
                      mountRoot.substring(0, mountRoot.length() - 1) : mountRoot;
        if (!root.equals("/")) {
            if (path.equals(root)) {
                return "";
            }
            if (path.startsWith(root + "/")) {
                path = path.substring(root.length());
            }
        }
        return path.substring(1);
    }

    // mountinfo writes space, tab, newline and backslash as octal escapes such as \040
    private static String unescape(String field) {
        StringBuilder out = new StringBuilder(field.length());
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if (c == '\\' && i + 4 <= field.length() &&
                field.substring(i + 1, i + 4).matches("[0-7]{3}")) {
                out.append((char) Integer.parseInt(field.substring(i + 1, i + 4), 8));
                i += 3;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static long cgroupV2Limit(Path root, String relative) {
        Path dir = root.resolve(relative).normalize();
        if (!dir.startsWith(root) || !Files.isDirectory(dir)) {
            // The process path is not visible from this mount (a cgroup namespace)
            dir = root;
        }
        long limit = -1L;
        for (Path d = dir; d != null && d.startsWith(root); d = d.getParent()) {
            limit = lowest(limit, readLimit(d.resolve("memory.max")));
        }
        return limit;
    }

    private static long cgroupV1Limit(Path base, String relative) {
        Path dir = base.resolve(relative).normalize();
        if (!dir.startsWith(base) || !Files.isDirectory(dir)) {
            dir = base;
        }
        long limit = readLimit(dir.resolve("memory.limit_in_bytes"));
        try {
            for (String line : Files.readAllLines(dir.resolve("memory.stat"),
                                                  StandardCharsets.UTF_8)) {
                if (line.startsWith("hierarchical_memory_limit ")) {
                    limit = lowest(limit, parseLimit(line.substring(line.indexOf(' ') + 1)));
                }
            }
        } catch (IOException | RuntimeException e) {
            // memory.stat is optional here, memory.limit_in_bytes already counted
        }
        return limit;
    }

    private static long readLimit(Path file) {
        try {
            return parseLimit(new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException e) {
            return -1L;
        }
    }

    // "max" (v2) and the near-Long.MAX_VALUE page-aligned v1 default mean no limit
    private static long parseLimit(String value) {
        String trimmed = value.trim();
        if (trimmed.isEmpty() || trimmed.equals("max")) {
            return -1L;
        }
        try {
            long limit = Long.parseLong(trimmed);
            return limit <= 0 || limit >= Long.MAX_VALUE / 2 ? -1L : limit;
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    private static long lowest(long a, long b) {
        if (a <= 0) {
            return b;
        }
        return b <= 0 ? a : Math.min(a, b);
    }

    private static long maxDirectMemory(long heap) {
        try {
            HotSpotDiagnosticMXBean hotspot = ManagementFactory.getPlatformMXBean(
                    HotSpotDiagnosticMXBean.class);
            return directMemory(Long.parseLong(
                    hotspot.getVMOption("MaxDirectMemorySize").getValue()), heap);
        } catch (RuntimeException | LinkageError e) {
            log.warn("Failed to read MaxDirectMemorySize, assuming it is unset", e);
            return directMemory(0L, heap);
        }
    }

    /**
     * The direct memory the budget leaves room for: -XX:MaxDirectMemorySize as given, or,
     * when it is unset (0), the heap size, which is what the JVM then allows.
     * bin/start-hugegraph-store.sh sets the flag to the heap it picks unless -j overrides
     * it, so a large heap with a smaller cap should pass that cap explicitly.
     */
    static long directMemory(long configured, long heap) {
        return configured > 0 ? configured : heap;
    }
}
