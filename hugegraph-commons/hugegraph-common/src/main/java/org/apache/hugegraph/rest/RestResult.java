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

package org.apache.hugegraph.rest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import lombok.SneakyThrows;
import okhttp3.Response;

public class RestResult {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // NOTE: jackson will synchronize DateFormat
        MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    private final int status;
    private final RestHeaders headers;
    private final String content;

    public RestResult(Response response) {
        this(response.code(), getResponseContent(response),
             RestHeaders.convertToRestHeaders(response.headers()));
    }

    public RestResult(int status, String content, RestHeaders headers) {
        this.status = status;
        this.headers = headers;
        this.content = content;
    }

    @SneakyThrows
    private static String getResponseContent(Response response) {
        return response.body().string();
    }

    public static void registerModule(Module module) {
        MAPPER.registerModule(module);
    }

    public int status() {
        return this.status;
    }

    public RestHeaders headers() {
        return this.headers;
    }

    public String content() {
        return this.content;
    }

    public <T> T readObject(Class<T> clazz) {
        try {
            return MAPPER.readValue(this.content, clazz);
        } catch (Exception e) {
            throw new SerializeException("Failed to deserialize: %s", e, this.content);
        }
    }

    @SuppressWarnings("deprecation")
    public <T> List<T> readList(String key, Class<T> clazz) {
        try {
            JsonNode root = MAPPER.readTree(this.content);
            JsonNode element = root.get(key);
            if (element == null) {
                throw new SerializeException("Can't find value of the key: %s in json.", key);
            }
            JavaType type = MAPPER.getTypeFactory()
                                  .constructParametrizedType(ArrayList.class,
                                                             List.class, clazz);
            return MAPPER.convertValue(element, type);
        } catch (IOException e) {
            throw new SerializeException("Failed to deserialize %s", e, this.content);
        }
    }

    @SuppressWarnings("deprecation")
    public <T> List<T> readList(Class<T> clazz) {
        JavaType type = MAPPER.getTypeFactory()
                              .constructParametrizedType(ArrayList.class,
                                                         List.class, clazz);
        try {
            return MAPPER.readValue(this.content, type);
        } catch (IOException e) {
            throw new SerializeException("Failed to deserialize %s", e, this.content);
        }
    }

    @Override
    public String toString() {
        return String.format("{status=%s, headers=%s, content=%s}", this.status, this.headers,
                             this.content);
    }
}
