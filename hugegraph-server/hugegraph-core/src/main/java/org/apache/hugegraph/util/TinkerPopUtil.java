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

package org.apache.hugegraph.util;

import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public final class TinkerPopUtil {

    private TinkerPopUtil() {
    }

    public static Directions direction(Direction direction) {
        switch (direction) {
            case OUT:
                return Directions.OUT;
            case IN:
                return Directions.IN;
            case BOTH:
                return Directions.BOTH;
            default:
                throw new AssertionError("Unknown direction: " + direction);
        }
    }

    public static Direction direction(Directions direction) {
        switch (direction) {
            case OUT:
                return Direction.OUT;
            case IN:
                return Direction.IN;
            case BOTH:
                return Direction.BOTH;
            default:
                throw new AssertionError("Unknown direction: " + direction);
        }
    }

    public static Cardinality cardinality(VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case single:
                return Cardinality.SINGLE;
            case list:
                return Cardinality.LIST;
            case set:
                return Cardinality.SET;
            default:
                throw new AssertionError("Unknown cardinality: " + cardinality);
        }
    }
}
