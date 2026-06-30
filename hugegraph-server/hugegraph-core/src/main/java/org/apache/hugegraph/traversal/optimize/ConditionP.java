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

package org.apache.hugegraph.traversal.optimize;

import org.apache.hugegraph.backend.query.Condition;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

public class ConditionP extends P<Object> {

    private static final long serialVersionUID = 9094970577400072902L;

    private ConditionP(final PBiPredicate<Object, Object> predicate,
                       Object value) {
        super(predicate, value);
    }

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
        // EQ that can compare two array
        return new ConditionP(Condition.RelationType.EQ, value);
    }
}
