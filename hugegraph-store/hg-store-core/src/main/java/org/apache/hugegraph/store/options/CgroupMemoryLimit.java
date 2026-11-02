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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The lowest cgroup memory limit visible to this process. cgroup v2 walks memory.max from
 * the process's cgroup up to the mount root; cgroup v1 reads hierarchical_memory_limit,
 * which already covers the ancestors. A limit above what the mounts show, as inside a
 * private cgroup namespace or under a subtree-only cgroup2 mount, can't be read: the
 * result is then marked incomplete so the caller can warn, and nothing is guessed.
 */
final class CgroupMemoryLimit {

    static final CgroupMemoryLimit NONE = new CgroupMemoryLimit(-1L, 0, false);

    // Bytes, or -1 when no limit is set or none could be read
    final long bytes;
    // 1 or 2 for the cgroup version the limit came from, 0 when there is no limit
    final int version;
    // Whether a stricter limit may sit above the part of the hierarchy that is visible
    final boolean incomplete;

    CgroupMemoryLimit(long bytes, int version, boolean incomplete) {
        this.bytes = bytes;
        this.version = version;
        this.incomplete = incomplete;
    }

    static CgroupMemoryLimit detect() {
        return read(Paths.get("/proc/self/cgroup"), Paths.get("/proc/self/mountinfo"),
                    Paths.get("/sys/fs/cgroup"));
    }

    /**
     * The membership path from /proc/self/cgroup is rebased onto every mount of the
     * controller in /proc/self/mountinfo whose root contains it, and the lowest limit over
     * all of them is taken, so mount order doesn't matter. Without a matching mount
     * defaultRoot is used.
     */
    static CgroupMemoryLimit read(Path procSelfCgroup, Path mountinfo, Path defaultRoot) {
        List<String> lines;
        try {
            lines = Files.readAllLines(procSelfCgroup, StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            return NONE;
        }
        List<String[]> v2Mounts = mounts(mountinfo, true);
        List<String[]> v1Mounts = mounts(mountinfo, false);
        long limit = -1L;
        int version = 0;
        boolean incomplete = false;
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
            boolean fullView = false;
            long lineLimit = -1L;
            for (String[] mount : mounts) {
                if (!contains(mount[0], parts[2])) {
                    // A mount of another subtree says nothing about this cgroup
                    continue;
                }
                matched = true;
                Path point = Paths.get(mount[1]);
                String relative = rebase(parts[2], mount[0]);
                // A full-hierarchy mount only covers the ancestors if this process's own
                // directory is reachable through it
                fullView |= mount[0].equals("/") && visible(point, relative);
                lineLimit = lowest(lineLimit, v2 ? v2Limit(point, relative) :
                                              v1Limit(point, relative));
            }
            if (!matched && !mounts.isEmpty()) {
                // In a cgroup namespace /proc/self/cgroup says "/" while a mount made outside
                // it has a root such as "/..", so the process's own cgroup can't be located.
                // The limits at the mount roots still bound it
                incomplete = true;
                for (String[] mount : mounts) {
                    Path point = Paths.get(mount[1]);
                    lineLimit = lowest(lineLimit, v2 ? readLimit(point.resolve("memory.max")) :
                                                  v1Limit(point, ""));
                }
            } else if (!matched) {
                String relative = rebase(parts[2], "/");
                lineLimit = v2 ? v2Limit(defaultRoot, relative) :
                            v1Limit(defaultRoot.resolve("memory"), relative);
            } else if (v2 && (!fullView || parts[2].equals("/"))) {
                // Only subtree mounts such as /kubepods, or the process sits at the root it
                // can see, which inside a private cgroup namespace is the namespace root: a
                // memory.max above is hidden. v1's hierarchical limit has no such gap
                incomplete = true;
            }
            if (lineLimit > 0 && (limit <= 0 || lineLimit < limit)) {
                limit = lineLimit;
                version = v2 ? 2 : 1;
            }
        }
        return new CgroupMemoryLimit(limit, version, incomplete);
    }

    /**
     * The {root, mount point} of every cgroup v2 mount, or of every cgroup v1 mount
     * carrying the memory controller, in /proc/self/mountinfo order.
     */
    private static List<String[]> mounts(Path mountinfo, boolean v2) {
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
        String root = trimSlash(mountRoot);
        return root.equals("/") || path.equals(root) || path.startsWith(root + "/");
    }

    /**
     * The membership path relative to a mount whose root is mountRoot: /kubepods/pod1/ctr
     * under root /kubepods is pod1/ctr. A path outside the mount root is kept as is.
     */
    static String rebase(String membership, String mountRoot) {
        String path = membership.startsWith("/") ? membership : "/" + membership;
        String root = trimSlash(mountRoot);
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

    private static String trimSlash(String root) {
        return root.endsWith("/") && root.length() > 1 ?
               root.substring(0, root.length() - 1) : root;
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

    private static boolean visible(Path point, String relative) {
        Path dir = point.resolve(relative).normalize();
        return dir.startsWith(point) && Files.isDirectory(dir);
    }

    private static long v2Limit(Path root, String relative) {
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

    private static long v1Limit(Path base, String relative) {
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
}
