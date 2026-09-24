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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.store.query.concurrent.AtomicFloat;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * base of max, min, sum. (input type equals output type)
 *
 * @param <U> buffer type (using for concurrency)
 * @param <T> record type
 */

public abstract class UnaryAggregationFunction<U, T> extends AbstractAggregationFunction<U, T, T> {

    /**
     * create the buffer
     */
    protected Supplier<U> supplier;

    /**
     * filed id
     */
    protected Id field;

    /**
     * type check, filed id and supplier should not be null
     */
    protected UnaryAggregationFunction() {

    }

    /**
     * init the agg function. the generic info of java would be erased during compiling stage,
     * the supplier is used to save the type info mostly.
     *
     * @param field    the field of the element
     * @param supplier use to create buffer.
     */
    public UnaryAggregationFunction(Id field, Supplier<U> supplier) {
        this.field = field;
        this.supplier = supplier;
        buffer = createBuffer();
    }

    public Id getFieldId() {
        return field;
    }

    /**
     * Create a new buffer
     *
     * @return Newly created buffer
     */
    @Override
    public U createBuffer() {
        return initBuffer();
    }

    protected abstract U initBuffer();

    /**
     * Get initial value
     *
     * @param longSupplier    Long Supplier。
     * @param integerSupplier Integer Supplier
     * @param doubleSupplier  Double Supplier
     * @param floatSupplier   Float Supplier
     * @return Returns the type of the initialized value, or the original instance if no matching type is found
     */
    protected U getInitValue(Supplier<AtomicLong> longSupplier,
                             Supplier<AtomicInteger> integerSupplier,
                             Supplier<AtomicDouble> doubleSupplier,
                             Supplier<AtomicFloat> floatSupplier) {
        Object result;
        var ins = this.supplier.get();
        //FIXME Using instance of statement
        switch (ins.getClass().getName()) {
            case "java.lang.Long":
                result = longSupplier.get();
                break;
            case "java.lang.Integer":
                result = integerSupplier.get();
                break;
            case "java.lang.Double":
                result = doubleSupplier.get();
                break;
            case "java.lang.Float":
                result = floatSupplier.get();
                break;
            case "java.lang.String":
                result = null;
                break;
            default:
                result = ins;
                break;
        }

        return (U) result;
    }
}
