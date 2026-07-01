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

package org.apache.hugegraph.schema;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.type.define.Action;
import org.apache.hugegraph.util.DateUtil;

public class Userdata extends HashMap<String, Object> {

    private static final long serialVersionUID = -1235451175617197049L;

    public static final String CREATE_TIME = "~create_time";
    public static final String DEFAULT_VALUE = "~default_value";

    public Userdata() {
    }

    public Userdata(Map<String, Object> map) {
        this.putAll(map);
    }

    /**
     * Normalizes the value before storing so the {@link #CREATE_TIME}-is-Date
     * invariant holds regardless of how entries are added.
     */
    @Override
    public Object put(String key, Object value) {
        return super.put(key, normalizeValue(key, value));
    }

    @Override
    public void putAll(Map<? extends String, ?> map) {
        for (Map.Entry<? extends String, ?> e : map.entrySet()) {
            this.put(e.getKey(), e.getValue());
        }
    }

    /**
     * Normalize internal userdata values whose runtime type can diverge from
     * their serialized form. The only such key today is {@link #CREATE_TIME}:
     * it is written as a {@link java.util.Date} but persisted as a formatted
     * JSON string by the backend serializers, and Jackson cannot re-type a
     * value to {@code Date} when the target is a raw {@code Map}. This method
     * restores the original type after deserialization. Idempotent for values
     * already of the expected type.
     * <p>
     * An empty string is passed through unchanged: it is the key-only
     * placeholder used by the {@code eliminate()}/{@code DELETE} builder flow
     * (e.g. {@code .userdata(CREATE_TIME, "").eliminate()}), where the value is
     * ignored and only the key drives {@code removeUserdata}. Parsing it would
     * fail before the eliminate path can apply its key-only semantics.
     */
    public static Object normalizeValue(String key, Object value) {
        if (CREATE_TIME.equals(key) && value instanceof String &&
            !((String) value).isEmpty()) {
            try {
                return DateUtil.parse((String) value);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(String.format(
                          "Invalid userdata '%s' value: '%s'",
                          CREATE_TIME, value), e);
            }
        }
        return value;
    }

    public static void check(Userdata userdata, Action action) {
        if (userdata == null) {
            return;
        }
        switch (action) {
            case INSERT:
            case APPEND:
                for (Map.Entry<String, Object> e : userdata.entrySet()) {
                    if (e.getValue() == null) {
                        throw new NotAllowException(
                                "Not allowed to pass null userdata value " +
                                "when create or append schema");
                    }
                }
                break;
            case ELIMINATE:
            case DELETE:
                // pass
                break;
            default:
                throw new AssertionError(String.format(
                        "Unknown schema action '%s'", action));
        }
    }
}
