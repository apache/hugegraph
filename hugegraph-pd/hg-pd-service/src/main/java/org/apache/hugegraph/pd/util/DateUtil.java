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

package org.apache.hugegraph.pd.util;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;

public class DateUtil {

    private static String DATE = "yyyy-MM-dd";
    private static String DATETIME = "yyyy-MM-dd HH:mm:ss";
    private static String DATETIME_MM = "yyyy-MM-dd HH:mm";
    private static String DATETIME_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
    private static String TIME = "HH:mm";
    private static String TIME_SS = "HH:mm:ss";
    private static String SYS_DATE = "yyyy/MM/dd";
    private static String SYS_DATETIME = "yyyy/MM/dd HH:mm:ss";
    private static String SYS_DATETIME_MM = "yyyy/MM/dd HH:mm";
    private static String SYS_DATETIME_SSS = "yyyy/MM/dd HH:mm:ss.SSS";
    private static String NONE_DATE = "yyyyMMdd";
    private static String NONE_DATETIME = "yyyyMMddHHmmss";
    private static String NONE_DATETIME_MM = "yyyyMMddHHmm";
    private static String NONE_DATETIME_SSS = "yyyyMMddHHmmssSSS";
    private static String[] PATTERNS = new String[]{
            DATE,
            DATETIME,
            DATETIME_MM,
            DATETIME_SSS,
            TIME,
            TIME_SS,
            SYS_DATE,
            SYS_DATETIME,
            SYS_DATETIME_MM,
            SYS_DATETIME_SSS,
            NONE_DATE,
            NONE_DATETIME,
            NONE_DATETIME_MM,
            NONE_DATETIME_SSS
    };

    public static String[] getDefaultPattern() {
        return PATTERNS;
    }

    public static Date getDate(String date) throws PDException {
        try {
            return DateUtils.parseDate(date, getDefaultPattern());
        } catch (ParseException e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

}
