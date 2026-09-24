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

package org.apache.hugegraph.pd.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.util.VersionUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

public class API {

    // TODO: use a flexible way to define the version
    // refer: https://github.com/apache/hugegraph/pull/2528#discussion_r1573823996
    public static final String VERSION = VersionUtil.getVersionFromProperties();
    public static final String PD = "PD";
    public static final String STORE = "STORE";
    public static String STATUS_KEY = "status";
    public static String ERROR_KEY = "error";
    public static String QUOTATION = "\"";
    public static String COMMA = ",";
    public static String COLON = ": ";

    public <T extends MessageOrBuilder> String toJSON(List<T> values, String key) {

        StringBuilder builder = new StringBuilder();
        builder.append("{")
               .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("0")
               .append(COMMA)
               .append(QUOTATION).append(key).append(QUOTATION).append(COLON)
               .append("[ ");

        if (values != null) {
            values.forEach(s -> {
                try {
                    builder.append(JsonFormat.printer().print(s));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                builder.append(",");
            });
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("]}");
        return builder.toString();
    }

    public String toJSON(MessageOrBuilder value, String key) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
               .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("0")
               .append(COMMA)
               .append(QUOTATION).append(key).append(QUOTATION).append(COLON);
        try {
            if (value != null) {
                builder.append(JsonFormat.printer().print(value));
            } else {
                builder.append("{}");
            }
            builder.append("}");
            return builder.toString();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return toJSON(e);
        }

    }

    public <T extends MessageOrBuilder> String toJSON(Map<String, List<T>> values) {
        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        for (Map.Entry<String, List<T>> entry : values.entrySet()) {
            String entryKey = entry.getKey();
            List<T> entryValue = entry.getValue();
            builder.append(QUOTATION).append(entryKey).append(QUOTATION).append(COLON).append("[");
            if ((entryValue != null) && !(entryValue.isEmpty())) {
                entryValue.forEach(s -> {
                    try {
                        if (s == null) {
                            builder.append("null");
                        } else {
                            builder.append(JsonFormat.printer().print(s));
                        }
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    builder.append(",");
                });
                builder.deleteCharAt(builder.length() - 1);
            }
            builder.append("]").append(COMMA);
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append("}");
        return builder.toString();
    }

    public String toJSON(PDException exception) {
        Map<String, Object> m = new HashMap<>();
        m.put(STATUS_KEY, exception.getErrorCode());
        m.put(ERROR_KEY, exception.getMessage() == null ? "" : exception.getMessage());
        return toJSON(m);
    }

    public String toJSON(Throwable exception) {
        Map<String, Object> m = new HashMap<>();
        m.put(STATUS_KEY, -1);
        m.put(ERROR_KEY,
              exception == null || exception.getMessage() == null ? "" : exception.getMessage());
        return toJSON(m);
    }

    public String toJSON(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    public Map<String, Object> okMap(String k, Object v) {
        Map<String, Object> map = new HashMap<>();
        map.put(STATUS_KEY, 0);
        map.put(k, v);
        return map;
    }

    public <T extends MessageOrBuilder> String toJSON(List<T> values,
                                                      JsonFormat.TypeRegistry registry) {

        StringBuilder builder = new StringBuilder();
        builder.append("{")
               .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("0")
               .append(COMMA)
               .append(QUOTATION).append("log").append(QUOTATION).append(COLON)
               .append("[ ");
        JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(registry);
        if (values != null) {
            values.forEach(s -> {
                try {
                    builder.append(printer.print(s));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                builder.append(",");
            });
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("]}");
        return builder.toString();
    }

}
