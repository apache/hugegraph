/*
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

package org.apache.hugegraph.query.serializer;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.hugegraph.exception.BackendException;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

// TODO: optimize by binary protocol
public abstract class AbstractSerializerAdapter<T> implements JsonSerializer<T>,
                                                              JsonDeserializer<T> {

    //Note: By overriding the method to get the mapping
    public abstract Map<String, Type> validType();

    @Override
    public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws
                                                                                             JsonParseException {
        JsonObject object = json.getAsJsonObject();
        String type = object.get("cls").getAsString();
        JsonElement element = object.get("el");
        try {
            return context.deserialize(element, validType().get(type));
        } catch (Exception e) {
            throw new BackendException("Unknown element type: " + type, e);
        }
    }

    @Override
    public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        Class clazz = src.getClass();
        result.add("cls", new JsonPrimitive(clazz.getSimpleName().substring(0, 1).toUpperCase()));
        result.add("el", context.serialize(src, clazz));
        return result;
    }
}
