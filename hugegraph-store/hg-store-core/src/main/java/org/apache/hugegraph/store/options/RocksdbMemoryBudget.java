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

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.RoundingMode;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.OperatingSystemMXBean;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolves rocksdb.total_memory_size, the budget RaftRocksdbOptions splits into the
 * write buffer manager's cache and the block cache. Both caches are off-heap, so "auto"
 * sizes the budget from the memory left after the heap, direct memory and the reserve,
 * instead of a fixed number that can exceed a container limit.
 */
@Slf4j
public final class RocksdbMemoryBudget {

    public static final String KEY = "total_memory_size";
    public static final String AUTO = "auto";

    static final long MIB = 1024L * 1024L;
    static final long GIB = 1024L * MIB;
    // The raft log block cache, which the raft log memtables are charged to
    static final long RAFT_LOG_RESERVE = RaftRocksdbOptions.RAFT_LOG_CACHE_SIZE;
    // Thread stacks, metaspace and other native allocations outside the budget
    static final long NATIVE_RESERVE = 512 * MIB;
    static final long RESERVED = RAFT_LOG_RESERVE + NATIVE_RESERVE;
    static final long MIN_AUTO = 256 * MIB;
    // RaftRocksdbOptions gives the write buffer manager 2/3 of the budget, and a 0-byte
    // write buffer manager means no memtable limit at all
    static final long MIN_EXPLICIT = MIB;
    // The value shipped before auto-sizing; a large host never gets more than it had
    static final long MAX_AUTO = 32_000_000_000L;

    private RocksdbMemoryBudget() {
    }

    /**
     * Returns the configured budget in bytes, or an automatic one for "auto". A missing,
     * blank or 0 value also means auto, as before "auto" existed.
     */
    public static long resolve(String configured) {
        long explicit = parse(configured);
        long heap = Runtime.getRuntime().maxMemory();
        long direct = maxDirectMemory(heap);
        long jvm = jvmMemory();
        CgroupMemoryLimit cgroup = CgroupMemoryLimit.detect();
        long physical = effectivePhysical(jvm, cgroup.bytes);
        long left = physical - heap - direct - RESERVED;
        long budget = explicit > 0 ? explicit : auto(physical, heap, direct);

        log.info("RocksDB memory budget: {} ({} bytes), mode={}, physical-limit={}, " +
                 "source={}, heap={}, direct={}, reserve={}, cgroup-view={}",
                 format(budget), budget, explicit > 0 ? "explicit" : AUTO, format(physical),
                 source(jvm, cgroup), format(heap), format(direct), format(RESERVED),
                 cgroup.incomplete ? "incomplete" : "complete");
        if (explicit > 0) {
            if (explicit == MAX_AUTO) {
                // Most likely an application-pd.yml kept from before auto-sizing
                log.warn("rocksdb.total_memory_size={} is the fixed value shipped before " +
                         "auto-sizing; set it to auto to size the budget from the available " +
                         "memory", explicit);
            } else if (explicit < MIN_AUTO) {
                log.warn("rocksdb.total_memory_size={} is below the {} floor auto-sizing " +
                         "never goes under", format(explicit), format(MIN_AUTO));
            }
            if (physical > 0 && explicit > left) {
                log.warn("rocksdb.total_memory_size={} exceeds the {} left after the heap, " +
                         "direct memory and reserve; the process may be OOM-killed",
                         format(explicit), format(Math.max(left, 0L)));
            }
            return budget;
        }
        if (cgroup.incomplete) {
            log.warn("This process can't see its whole cgroup hierarchy (a cgroup namespace " +
                     "or subtree-only cgroup mounts), so a tighter memory limit above it may " +
                     "be missed; set rocksdb.total_memory_size or " +
                     "HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE to a number of bytes if one applies");
        }
        if (physical > 0 && left < 2 * MIN_AUTO) {
            log.warn("Heap, direct memory and the {} reserve leave too little of the {} " +
                     "physical limit, RocksDB gets the {} floor; lower -Xmx or " +
                     "-XX:MaxDirectMemorySize", format(RESERVED), format(physical),
                     format(MIN_AUTO));
        }
        return budget;
    }

    /**
     * The explicit budget in bytes, or 0 for auto: "auto", a missing or blank value, or 0.
     */
    static long parse(String configured) {
        String value = configured == null ? "" : configured.trim();
        if (value.isEmpty() || value.equalsIgnoreCase(AUTO)) {
            return 0L;
        }
        long bytes;
        try {
            bytes = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
                    "rocksdb.total_memory_size must be auto or a number of bytes, got '%s'",
                    configured), e);
        }
        if (bytes < 0) {
            throw new IllegalArgumentException(String.format(
                    "rocksdb.total_memory_size must be auto or a number of bytes, got '%s'",
                    configured));
        }
        if (bytes > 0 && bytes < MIN_EXPLICIT) {
            throw new IllegalArgumentException(String.format(
                    "rocksdb.total_memory_size must be auto or at least %s bytes, got %s",
                    MIN_EXPLICIT, bytes));
        }
        return bytes;
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
     * The smaller of what the JVM reports and the cgroup limit, ignoring unknown (<= 0)
     * values.
     */
    static long effectivePhysical(long jvm, long cgroup) {
        if (cgroup <= 0) {
            return jvm;
        }
        return jvm <= 0 ? cgroup : Math.min(jvm, cgroup);
    }

    // Where the physical limit used by effectivePhysical() came from
    static String source(long jvm, CgroupMemoryLimit cgroup) {
        if (cgroup.bytes > 0 && (jvm <= 0 || cgroup.bytes <= jvm)) {
            return "cgroup-v" + cgroup.version;
        }
        return jvm > 0 ? "jvm" : "unknown";
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

    // Bytes in binary units for the logs, such as 1.5 GiB or 512 MiB
    static String format(long bytes) {
        if (bytes < 0) {
            return "unknown";
        }
        if (bytes >= GIB) {
            return scaled(bytes, GIB) + " GiB";
        }
        if (bytes >= MIB) {
            return scaled(bytes, MIB) + " MiB";
        }
        return bytes + " B";
    }

    private static String scaled(long bytes, long unit) {
        return BigDecimal.valueOf(bytes).divide(BigDecimal.valueOf(unit), 2, RoundingMode.HALF_UP)
                         .stripTrailingZeros().toPlainString();
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
}
