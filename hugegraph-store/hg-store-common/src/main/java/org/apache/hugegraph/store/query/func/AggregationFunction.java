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

package org.apache.hugegraph.store.query.func;

/**
 * agg function
 *
 * @param <U> buffer type
 * @param <R> record type
 * @param <T> return type
 */
public interface AggregationFunction<U, R, T> {

    default void init() {
    }

    /**
     * initial value of the merge function
     *
     * @return initial value
     */
    U createBuffer();

    /**
     * get the buffer that created by <code>createBuffer()</code>
     *
     * @return
     */
    U getBuffer();

    /**
     * the operation when iterator the record
     *
     * @param record record
     */
    void iterate(R record);

    /**
     * merge other to buffer
     *
     * @param other other buffer
     */
    void merge(U other);

    /**
     * finial aggregator
     *
     * @return reduce buffer
     */
    T reduce();

}
