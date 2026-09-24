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

package org.apache.hugegraph.structure;

import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;

public class BaseProperty<V> {
    private final PropertyKey propertyKey;

    protected V value;

    public BaseProperty(PropertyKey propertyKey, V value) {
        this.propertyKey = propertyKey;
        this.value = value;
    }

    public DataType getDataType() {
        return propertyKey.dataType();
    }

    public void setDataType(DataType dataType) {
        this.propertyKey.dataType(dataType);
    }

    public Cardinality getCardinality() {
        return propertyKey.cardinality();
    }

    public void setCardinality(Cardinality cardinality) {
        this.propertyKey.cardinality(cardinality);
    }

    public V value() {
        return value;
    }

    public void value(V value) {
        this.value = value;
    }

    public PropertyKey propertyKey() {
        return propertyKey;
    }

    public Object serialValue(boolean encodeNumber) {
        return this.propertyKey.serialValue(this.value, encodeNumber);
    }

}
