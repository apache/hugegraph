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

import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;

public class BaseRawElement extends BaseElement implements Cloneable {

    private byte[] key;
    private byte[] value;

    public BaseRawElement(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] key() {
        return this.key;
    }

    public byte[] value() {
        return this.value;
    }

    @Override
    public Object sysprop(HugeKeys key) {
        return null;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public HugeType type() {
        return HugeType.KV_RAW;
    }
}
