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
import java.util.Locale;

import org.apache.hugegraph.query.Aggregate;
import org.apache.hugegraph.query.Aggregate.AggregateFunc;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/** Preserves the original query aggregate descriptor without Java class names. */
public final class AggregateAdapter implements JsonSerializer<Aggregate<Number>>,
                                               JsonDeserializer<Aggregate<Number>> {

    @Override
    public JsonElement serialize(Aggregate<Number> aggregate, Type type,
                                 JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        // Both the shared enum and external strategies expose their function name.
        // Only names supported by the original query protocol may cross the wire.
        AggregateFunc func = function(aggregate.func().string().toUpperCase(Locale.ROOT));
        result.addProperty("func", func.name());
        if (aggregate.column() != null) {
            result.addProperty("column", aggregate.column());
        }
        return result;
    }

    @Override
    public Aggregate<Number> deserialize(JsonElement json, Type type,
                                         JsonDeserializationContext context) {
        JsonObject aggregate = json.getAsJsonObject();
        AggregateFunc func = function(aggregate.get("func").getAsString());
        JsonElement column = aggregate.get("column");
        return new Aggregate<>(func, column == null || column.isJsonNull() ? null :
                                    column.getAsString());
    }

    private static AggregateFunc function(String name) {
        try {
            return AggregateFunc.valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new JsonParseException("Unsupported aggregate function: " + name, e);
        }
    }
}
