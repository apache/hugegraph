/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api.schema;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hugegraph.api.graph.PropertiesDeserializer;
import org.apache.hugegraph.schema.Userdata;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * The userdata of a property key: {@code ~default_value} is read like an
 * element property (a JSON fraction becomes a BigDecimal with every digit,
 * so a DECIMAL default reaches the key exactly), every other entry keeps
 * Jackson's default types, so custom metadata such as {@code {"rate":0.85}}
 * stays a double and round-trips as a JSON number.
 */
public class UserdataDeserializer extends StdDeserializer<Userdata> {

    private static final long serialVersionUID = 1L;

    public UserdataDeserializer() {
        super(Userdata.class);
    }

    @Override
    public Userdata deserialize(JsonParser parser, DeserializationContext context)
                                throws IOException {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NULL) {
            return null;
        }
        if (token != JsonToken.START_OBJECT) {
            throw JsonMappingException.from(parser,
                  "Expected an object for 'user_data', but got " + token);
        }
        Map<String, Object> map = new LinkedHashMap<>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String name = parser.currentName();
            parser.nextToken();
            if (Userdata.DEFAULT_VALUE.equals(name)) {
                map.put(name, PropertiesDeserializer.readValue(parser));
            } else {
                map.put(name, context.readValue(parser, Object.class));
            }
        }
        return new Userdata(map);
    }
}
