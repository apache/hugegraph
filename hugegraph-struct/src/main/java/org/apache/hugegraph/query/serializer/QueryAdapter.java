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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.type.define.Directions;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.reflect.TypeToken;

public class QueryAdapter extends AbstractSerializerAdapter<Condition> {

    static ImmutableMap<String, Type> cls =
            ImmutableMap.<String, Type>builder()
                        // TODO: uncomment later
                        .put("N", Condition.Not.class)
                        .put("A", Condition.And.class)
                        .put("O", Condition.Or.class)
                        .put("S", Condition.SyspropRelation.class)
                        .put("U", Condition.UserpropRelation.class)
                        .build();

    static boolean isPrimitive(Class clz) {
        try {
            return (clz == Date.class) || ((Class) clz.getField("TYPE").get(null)).isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Map<String, Type> validType() {
        return cls;
    }

    @Override
    public Condition deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        Condition condition = super.deserialize(json, typeOfT, context);
        if (condition instanceof Condition.Relation) {
            JsonObject object = json.getAsJsonObject();
            if (object.has("el")) {
                JsonElement elElement = object.get("el");
                JsonElement valueElement = elElement.getAsJsonObject().get("value");
                if (valueElement.isJsonObject()) {
                    String cls = valueElement.getAsJsonObject().get("cls").getAsString();
                    try {
                        Class actualClass = Class.forName(cls);
                        Object obj = context.deserialize(valueElement, actualClass);
                        ((Condition.Relation) condition).value(obj);
                    } catch (ClassNotFoundException e) {
                        throw new JsonParseException(e.getMessage());
                    }
                } else if (elElement.getAsJsonObject().has("valuecls")) {
                    if (valueElement.isJsonArray()) {
                        String cls = elElement.getAsJsonObject().get("valuecls").getAsString();
                        try {
                            Class actualClass = Class.forName(cls);
                            Type type = TypeToken.getParameterized(ArrayList.class, actualClass)
                                                 .getType();
                            Object value = context.deserialize(valueElement, type);
                            ((Condition.Relation) condition).value(value);
                        } catch (ClassNotFoundException e) {
                            throw new JsonParseException(e.getMessage());
                        }
                    } else {
                        String cls = elElement.getAsJsonObject().get("valuecls").getAsString();
                        try {
                            Class actualClass = Class.forName(cls);
                            Object obj = context.deserialize(valueElement, actualClass);
                            ((Condition.Relation) condition).value(obj);
                        } catch (ClassNotFoundException e) {
                            throw new JsonParseException(e.getMessage());
                        }
                    }

                } else if (valueElement.isJsonPrimitive() &&
                           valueElement.getAsJsonPrimitive().isString()) {
                    switch ((String) ((Condition.Relation) condition).value()) {
                        case "OUT":
                            ((Condition.Relation) condition).value(Directions.OUT);
                            break;
                        case "IN":
                            ((Condition.Relation) condition).value(Directions.IN);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return condition;
    }

    @Override
    public JsonElement serialize(Condition src, Type typeOfSrc, JsonSerializationContext context) {
        JsonElement result = super.serialize(src, typeOfSrc, context);
        if (src instanceof Condition.Relation) {
            JsonObject object = result.getAsJsonObject();
            JsonElement valueElement = object.get("el").getAsJsonObject().get("value");
            if (valueElement.isJsonObject()) {
                valueElement.getAsJsonObject()
                            .add("cls",
                                 new JsonPrimitive(
                                         ((Condition.Relation) src).value().getClass().getName()));
            } else if (isPrimitive(((Condition.Relation) src).value().getClass())) {
                object.get("el").getAsJsonObject()
                      .add("valuecls",
                           new JsonPrimitive(
                                   ((Condition.Relation) src).value().getClass().getName()));
            } else if (valueElement.isJsonArray()) {
                if (((Condition.Relation) src).value() instanceof List) {
                    String valueCls =
                            ((List) ((Condition.Relation) src).value()).get(0).getClass().getName();
                    object.get("el").getAsJsonObject().add("valuecls", new JsonPrimitive(valueCls));
                }
            }
        }
        return result;
    }
}
