/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hugegraph.store.upgrade;

import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** A source-bound plan, not an inference of historical graph ownership. */
public final class GraphIdMigrationPlan {

    public static final int LEGACY_ID = 0xFFFE;
    private final String sourceDigest;
    private final List<Rule> rules;
    private final Map<String, Integer> targets;

    /** Prefix is hexadecimal logical-key bytes, excluding the two-byte GraphID. */
    public record Rule(String columnFamily, String prefixHex, String graph) {
        public Rule {
            if (columnFamily == null || columnFamily.isBlank() || graph == null ||
                graph.isBlank() || prefixHex == null) {
                throw new IllegalArgumentException("A rule needs a column family, prefix and graph");
            }
            prefixHex = HexFormat.of().formatHex(HexFormat.of().parseHex(prefixHex));
        }

        public boolean matches(String family, byte[] key) {
            byte[] prefix = HexFormat.of().parseHex(this.prefixHex);
            return this.columnFamily.equals(family) && isLegacy(key) &&
                   key.length >= prefix.length + 2 &&
                   Arrays.equals(key, 2, 2 + prefix.length, prefix, 0, prefix.length);
        }
    }

    public GraphIdMigrationPlan(String sourceDigest, List<Rule> rules,
                                Map<String, Integer> targets) {
        if (sourceDigest == null || !sourceDigest.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Expected a SHA-256 source digest");
        }
        validateRules(rules);
        Set<String> owners = new TreeSet<>();
        rules.forEach(rule -> owners.add(rule.graph()));
        if (!owners.equals(targets.keySet()) ||
            targets.values().stream().anyMatch(id -> id == null || id < 0 || id >= LEGACY_ID) ||
            new TreeSet<>(targets.values()).size() != targets.size()) {
            throw new IllegalArgumentException("Targets must be legal, unique and cover exactly the owners");
        }
        this.sourceDigest = sourceDigest;
        this.rules = List.copyOf(rules);
        this.targets = Collections.unmodifiableMap(new LinkedHashMap<>(targets));
    }

    public static void validateRules(List<Rule> rules) {
        if (rules == null || rules.isEmpty()) {
            throw new IllegalArgumentException("Explicit independently verified ownership rules are required");
        }
        for (int i = 0; i < rules.size(); i++) {
            Rule a = rules.get(i);
            for (int j = i + 1; j < rules.size(); j++) {
                Rule b = rules.get(j);
                if (a.columnFamily().equals(b.columnFamily()) &&
                    (a.prefixHex().startsWith(b.prefixHex()) ||
                     b.prefixHex().startsWith(a.prefixHex()))) {
                    throw new IllegalArgumentException("Overlapping ownership rules: " + a + " / " + b);
                }
            }
        }
    }

    public static Map<String, Integer> allocate(List<Rule> rules, Set<Integer> occupied) {
        validateRules(rules);
        Set<String> owners = new TreeSet<>();
        rules.forEach(rule -> owners.add(rule.graph()));
        Map<String, Integer> targets = new LinkedHashMap<>();
        int next = 0;
        for (String owner : owners) {
            while (next < LEGACY_ID && occupied.contains(next)) {
                next++;
            }
            if (next >= LEGACY_ID) {
                throw new IllegalStateException("No free GraphID for " + owner);
            }
            targets.put(owner, next++);
        }
        return targets;
    }

    public static boolean isLegacy(byte[] key) {
        return key.length >= 2 && (key[0] & 0xFF) == 0xFF && (key[1] & 0xFF) == 0xFE;
    }

    public byte[] rewrite(String family, byte[] key) {
        if (!isLegacy(key)) {
            return key.clone();
        }
        for (Rule rule : this.rules) {
            if (rule.matches(family, key)) {
                int target = this.targets.get(rule.graph());
                byte[] converted = key.clone();
                converted[0] = (byte) (target >>> 8);
                converted[1] = (byte) target;
                return converted;
            }
        }
        throw new IllegalStateException("No proven owner for legacy key in " + family +
                                        ": " + HexFormat.of().formatHex(key));
    }

    public String sourceDigest() {
        return this.sourceDigest;
    }

    public List<Rule> rules() {
        return this.rules;
    }

    public Map<String, Integer> targets() {
        return this.targets;
    }
}
