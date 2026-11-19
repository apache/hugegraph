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

import java.util.*;

public class GraphIdMigrationPlanProbe {
    static int assertions;
    static void require(boolean result) {
        assertions++;
        if (!result) throw new AssertionError("assertion " + assertions);
    }
    static void refused(Runnable operation) {
        assertions++;
        try { operation.run(); } catch (IllegalArgumentException | IllegalStateException expected) { return; }
        throw new AssertionError("Expected refusal " + assertions);
    }
    static GraphIdMigrationPlan plan(List<GraphIdMigrationPlan.Rule> rules, Set<Integer> used) {
        return new GraphIdMigrationPlan("0".repeat(64), rules, GraphIdMigrationPlan.allocate(rules, used));
    }
    public static void main(String[] args) throws Exception {
        var business = new GraphIdMigrationPlan.Rule("g+v", "01", "DEFAULT/business/g");
        var system = new GraphIdMigrationPlan.Rule("g+v", "02", "DEFAULT/~sys_graph/g");
        var rules = List.of(system, business);
        var p = plan(rules, Set.of(0, 2, 4));
        require(p.targets().get(business.graph()) == 1);
        require(p.targets().get(system.graph()) == 3);
        require(plan(List.of(business, system), Set.of(0, 2, 4)).targets().equals(p.targets()));
        byte[] old = {(byte)255, (byte)254, 1, 8, 0, 9};
        byte[] converted = p.rewrite("g+v", old);
        require(converted[0] == 0 && converted[1] == 1);
        require(Arrays.equals(Arrays.copyOfRange(old,2,old.length), Arrays.copyOfRange(converted,2,converted.length)));
        require(GraphIdMigrationPlan.isLegacy(old));
        require(!GraphIdMigrationPlan.isLegacy(converted));
        require(Arrays.equals(converted, p.rewrite("g+v", converted)));
        refused(() -> p.rewrite("g+v", new byte[]{-1,-2,3}));
        refused(() -> p.rewrite("unknown", old));
        refused(() -> plan(List.of(business, new GraphIdMigrationPlan.Rule("g+v", "0101", system.graph())), Set.of()));
        refused(() -> plan(List.of(business, new GraphIdMigrationPlan.Rule("g+v", "", system.graph())), Set.of()));
        refused(() -> plan(List.of(business, business), Set.of()));
        refused(() -> new GraphIdMigrationPlan("bad", rules, p.targets()));
        refused(() -> new GraphIdMigrationPlan("0".repeat(64), rules, Map.of(business.graph(),1,system.graph(),1)));
        refused(() -> new GraphIdMigrationPlan("0".repeat(64), rules, Map.of(business.graph(),65534,system.graph(),1)));
        refused(() -> new GraphIdMigrationPlan.Rule("g+v", "0", business.graph()));
        Set<Integer> exhausted = new HashSet<>();
        for(int i=0;i<65534;i++) exhausted.add(i);
        refused(() -> plan(rules, exhausted));
        Random random = new Random(3224L);
        for(int n=0;n<10000;n++) {
            byte[] key = new byte[4+random.nextInt(100)]; random.nextBytes(key);
            key[0]=-1; key[1]=-2; key[2]=(byte)(1+random.nextInt(2));
            byte[] result=p.rewrite("g+v",key);
            require(Arrays.equals(Arrays.copyOfRange(key,2,key.length),Arrays.copyOfRange(result,2,result.length)));
            require(result[1]==(key[2]==1?1:3));
            require(Arrays.equals(result,p.rewrite("g+v",result)));
        }
        System.out.printf("PASS: %d deterministic planner assertions, including 10000 randomized keys. JVM=%s%n",assertions,System.getProperty("java.version"));
        // Counterexample: changing metadata alone never moves bytes to the new prefix.
        Map<String,String> rows = new HashMap<>(); rows.put("fffe01080009", "value");
        require(rows.get("000101080009")==null);
        System.out.println("PASS: mapping-only counterexample retains unreachable legacy row");
    }
}
