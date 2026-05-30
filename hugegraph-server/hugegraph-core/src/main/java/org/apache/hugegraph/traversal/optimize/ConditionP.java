/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.traversal.optimize;

import org.apache.hugegraph.backend.query.Condition;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

public class ConditionP extends P<Object> {

    private static final long serialVersionUID = 9094970577400072902L;

    private final Condition.RelationType relationType;

    /**
     * Adapter to wrap HugeGraph RelationType into a TinkerPop PBiPredicate
     */
    private static class RelationAdapter implements PBiPredicate<Object, Object> {

        private final Condition.RelationType type;

        RelationAdapter(Condition.RelationType type) {
            this.type = type;
        }

        @Override
        public boolean test(Object value, Object condition) {
            return type.test(value, condition);
        }

        @Override
        public String toString() {
            return type.name();
        }
    }

    private ConditionP(final Condition.RelationType type,
                       final Object value) {
        super(new RelationAdapter(type), value);
        this.relationType = type;
    }

    public Condition.RelationType relationType() {
        return this.relationType;
    }

    public static ConditionP of(Condition.RelationType type, Object value) {
        return new ConditionP(type, value);
    }

    // --- Restored helper factory methods ---

    public static ConditionP textContains(Object value) {
        return new ConditionP(Condition.RelationType.TEXT_CONTAINS, value);
    }

    public static ConditionP contains(Object value) {
        return new ConditionP(Condition.RelationType.CONTAINS, value);
    }

    public static ConditionP containsK(Object value) {
        return new ConditionP(Condition.RelationType.CONTAINS_KEY, value);
    }

    public static ConditionP containsV(Object value) {
        return new ConditionP(Condition.RelationType.CONTAINS_VALUE, value);
    }

    public static ConditionP eq(Object value) {
        return new ConditionP(Condition.RelationType.EQ, value);
    }

    @Override
    public String toString() {
        return this.relationType.name().toLowerCase() +
               "(" + this.getValue() + ")";
    }
}
