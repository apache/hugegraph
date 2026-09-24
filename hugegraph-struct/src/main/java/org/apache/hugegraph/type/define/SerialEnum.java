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

package org.apache.hugegraph.type.define;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.type.HugeType;

import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface SerialEnum {

    public byte code();

//    static Table<Class<?>, Byte, SerialEnum> table = HashBasedTable.create();

    static Map<Class, Map<Byte,SerialEnum>>table =new ConcurrentHashMap<>();

    public static void register(Class<? extends SerialEnum> clazz) {
        Object enums;
        try {
            enums = clazz.getMethod("values").invoke(null);
        } catch (Exception e) {
            throw new HugeException("Exception in backend",  e);
        }
        ConcurrentHashMap map=new ConcurrentHashMap<Byte,SerialEnum>();
        for (SerialEnum e : CollectionUtil.<SerialEnum>toList(enums)) {
            map.put(e.code(), e);
        }
        table.put(clazz,map);
    }


    public static <T extends SerialEnum> T fromCode(Class<T> clazz, byte code) {
        Map clazzMap=table.get(clazz);
        if (clazzMap == null) {
            SerialEnum.register(clazz);
            clazzMap=table.get(clazz);
        }
        E.checkArgument(clazzMap != null, "Can't get class registery for %s",
                        clazz.getSimpleName());
        T value = (T) clazzMap.get(code);
        if (value == null) {
            E.checkArgument(false, "Can't construct %s from code %s",
                            clazz.getSimpleName(), code);
        }
        return value;
    }

    public static void registerInternalEnums() {
        SerialEnum.register(Action.class);
        SerialEnum.register(AggregateType.class);
        SerialEnum.register(Cardinality.class);
        SerialEnum.register(DataType.class);
        SerialEnum.register(Directions.class);
        SerialEnum.register(Frequency.class);
        SerialEnum.register(HugeType.class);
        SerialEnum.register(IdStrategy.class);
        SerialEnum.register(IndexType.class);
        SerialEnum.register(SchemaStatus.class);
//        SerialEnum.register(HugePermission.class);
    }
}
