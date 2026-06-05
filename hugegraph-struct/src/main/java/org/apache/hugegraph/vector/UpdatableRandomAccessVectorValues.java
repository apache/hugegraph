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


package org.apache.hugegraph.vector;

import java.util.Map;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;

public class UpdatableRandomAccessVectorValues implements RandomAccessVectorValues {
    private final Map<Integer, VectorFloat<?>> map;
    private final int dimension;

    public UpdatableRandomAccessVectorValues(Map<Integer, VectorFloat<?>> map, int dimension) {
        this.map = map;
        this.dimension = dimension;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int nodeId) {
        return map.get(nodeId);
    }

    @Override
    public boolean isValueShared() {
        return false;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return this;
    }

    public void addNode(int nodeId, VectorFloat<?> vector){
        map.put(nodeId, vector);
    }

    public void removeNode(int nodeId){
        map.remove(nodeId);
    }
}
