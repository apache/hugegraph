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

package org.apache.hugegraph.opencypher;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.opencypher.gremlin.translation.traversal.TraversalGremlinPredicates;
import org.opencypher.gremlin.traversal.CustomPredicate;

/**
 * Adapt the extension factories compiled against the pre-3.8 P constructor.
 * The original predicates retain Cypher type checks and whole-string regex matching.
 */
public final class CypherGremlinPredicates extends TraversalGremlinPredicates {

    @Override
    public P<Object> regexMatch(Object value) {
        return cypherRegex(value);
    }

    @Override
    public P<Object> isNode() {
        return cypherIsNode();
    }

    @Override
    public P<Object> isRelationship() {
        return cypherIsRelationship();
    }

    @Override
    public P<Object> isString() {
        return cypherIsString();
    }

    public static P<Object> cypherRegex(Object value) {
        return predicate(CustomPredicate.cypherRegex, value);
    }

    public static P<Object> cypherIsNode() {
        return predicate(CustomPredicate.cypherIsNode, null);
    }

    public static P<Object> cypherIsRelationship() {
        return predicate(CustomPredicate.cypherIsRelationship, null);
    }

    public static P<Object> cypherIsString() {
        return predicate(CustomPredicate.cypherIsString, null);
    }

    private static P<Object> predicate(CustomPredicate predicate, Object value) {
        PBiPredicate<Object, Object> adapter = predicate::test;
        return new P<>(adapter, value);
    }
}
