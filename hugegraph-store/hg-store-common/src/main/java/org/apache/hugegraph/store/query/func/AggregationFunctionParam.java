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

import org.apache.hugegraph.id.Id;

import lombok.Data;

@Data
public class AggregationFunctionParam {

    private AggregationFunctionType functionType;
    /**
     * the type of aggregation filed.
     * eg: sum(age): the type is integer
     */
    private FieldType fieldType;
    /**
     * field id
     */
    private Id field;

    private AggregationFunctionParam(AggregationFunctionType functionType, FieldType fieldType,
                                     Id filed) {
        this.functionType = functionType;
        this.fieldType = fieldType;
        this.field = filed;
    }

    public static AggregationFunctionParam ofCount() {
        return new AggregationFunctionParam(AggregationFunctionType.COUNT, FieldType.LONG, null);
    }

    public static AggregationFunctionParam ofSum(FieldType fieldType, Id field) {
        return new AggregationFunctionParam(AggregationFunctionType.SUM, fieldType, field);
    }

    public static AggregationFunctionParam ofMin(FieldType fieldType, Id field) {
        return new AggregationFunctionParam(AggregationFunctionType.MIN, fieldType, field);
    }

    public static AggregationFunctionParam ofMax(FieldType fieldType, Id field) {
        return new AggregationFunctionParam(AggregationFunctionType.MAX, fieldType, field);
    }

    public static AggregationFunctionParam ofAvg(FieldType fieldType, Id field) {
        return new AggregationFunctionParam(AggregationFunctionType.AVG, fieldType, field);
    }

    public enum AggregationFunctionType {
        COUNT,
        SUM,
        MIN,
        MAX,
        AVG
    }

    public enum FieldType {
        LONG("java.lang.Long"),
        INTEGER("java.lang.Integer"),
        FLOAT("java.lang.Float"),
        DOUBLE("java.lang.Double"),
        STRING("java.lang.String");

        private final String genericType;

        FieldType(String genericType) {
            this.genericType = genericType;
        }

        public String getGenericType() {
            return genericType;
        }
    }
}
