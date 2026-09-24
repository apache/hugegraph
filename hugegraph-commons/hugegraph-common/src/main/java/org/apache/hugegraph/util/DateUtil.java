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

package org.apache.hugegraph.util;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.date.SafeDateFormat;

import com.google.common.collect.ImmutableMap;

public final class DateUtil {

    public static final Date DATE_ZERO = new Date(0L);

    private static final Map<String, String> VALID_DFS = ImmutableMap.of(
            "^\\d{4}-\\d{1,2}-\\d{1,2}",
            "yyyy-MM-dd",
            "^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}",
            "yyyy-MM-dd HH:mm:ss",
            "^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{1,3}",
            "yyyy-MM-dd HH:mm:ss.SSS"
    );

    private static final Map<String, SafeDateFormat> DATE_FORMATS = new ConcurrentHashMap<>();

    public static Date parse(String value) {
        for (Map.Entry<String, String> entry : VALID_DFS.entrySet()) {
            if (value.matches(entry.getKey())) {
                return parse(value, entry.getValue());
            }
        }
        throw new IllegalArgumentException(String.format(
                "Expected date format is: %s, but got '%s'", VALID_DFS.values(), value));
    }

    public static Date parse(String value, String df) {
        SafeDateFormat dateFormat = getDateFormat(df);
        return dateFormat.parse(value);
    }

    public static Date now() {
        return new Date();
    }

    private static SafeDateFormat getDateFormat(String df) {
        SafeDateFormat dateFormat = DATE_FORMATS.get(df);
        if (dateFormat == null) {
            dateFormat = new SafeDateFormat(df);
            SafeDateFormat previous = DATE_FORMATS.putIfAbsent(df, dateFormat);
            if (previous != null) {
                dateFormat = previous;
            }
        }
        return dateFormat;
    }

    public static Object toPattern(String df) {
        SafeDateFormat dateFormat = getDateFormat(df);
        return dateFormat.toPattern();
    }
}
