/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.query;

import java.util.Iterator;
import java.util.function.BiFunction;

import org.apache.tinkerpop.gremlin.util.NumberHelper;

public class Aggregate<P> {

    private final AggregateFuncDefine<P> func;
    private final String column;

    public Aggregate(AggregateFuncDefine<P> func, String column) {
        this.func = func;
        this.column = column;
    }

    public AggregateFuncDefine<P> func() {
        return this.func;
    }

    public String column() {
        return this.column;
    }

    public boolean countAll() {
        return this.func.countAll() && this.column == null;
    }

    public P reduce(Iterator<P> results) {
        return this.func.reduce(results);
    }

    public P defaultValue() {
        return this.func.defaultValue();
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", this.func.string(),
                             this.column == null ? "*" : this.column);
    }

    public enum AggregateFunc implements AggregateFuncDefine<Number> {

        COUNT("count", 0L, NumberHelper::add),
        MAX("max", -Double.MAX_VALUE, NumberHelper::max),
        MIN("min", Double.MAX_VALUE, NumberHelper::min),
        AVG("avg", 0D, NumberHelper::add),
        SUM("sum", 0L, NumberHelper::add);

        private final String name;
        private final Number defaultValue;
        private final BiFunction<Number, Number, Number> merger;

        AggregateFunc(String name, Number defaultValue,
                      BiFunction<Number, Number, Number> merger) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.merger = merger;
        }

        @Override
        public String string() {
            return this.name;
        }

        @Override
        public boolean countAll() {
            return this == COUNT;
        }

        @Override
        public Number defaultValue() {
            return this.defaultValue;
        }

        @Override
        public Number reduce(Iterator<Number> results) {
            Number number;
            long count = 0L;
            if (results.hasNext()) {
                number = results.next();
                count++;
            } else {
                return this.defaultValue;
            }
            while (results.hasNext()) {
                number = this.merger.apply(number, results.next());
                count++;
            }
            if (this == AVG) {
                number = NumberHelper.div(number, count);
            }
            return number;
        }
    }
}
