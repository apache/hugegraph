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

package org.apache.hugegraph.store.util;

public class HgStoreException extends RuntimeException {

    public static final int EC_NOEXCEPT = 0;
    public static final int EC_FAIL = 1000;
    // data format not support
    public static final int EC_DATAFMT_NOT_SUPPORTED = 1001;
    public static final int EC_CLOSE = 1002;
    public static final int EC_RKDB_CREATE_FAIL = 1201;
    public static final int EC_RKDB_DOPUT_FAIL = 1202;
    public static final int EC_RKDB_DODEL_FAIL = 1203;
    public static final int EC_RDKDB_DOSINGLEDEL_FAIL = 1204;
    public static final int EC_RKDB_DODELPREFIX_FAIL = 1205;
    public static final int EC_RKDB_DODELRANGE_FAIL = 1206;
    public static final int EC_RKDB_DOMERGE_FAIL = 1207;
    public static final int EC_RKDB_DOGET_FAIL = 1208;
    public static final int EC_RKDB_PD_FAIL = 1209;
    public static final int EC_RKDB_EXPORT_SNAPSHOT_FAIL = 1214;
    public static final int EC_RKDB_IMPORT_SNAPSHOT_FAIL = 1215;
    public static final int EC_RKDB_SNAPSHOT_SAVE_BUSY_FAIL = 1217;
    private static final long serialVersionUID = 5193624480997934335L;
    private final int code;

    public HgStoreException() {
        super();
        this.code = EC_NOEXCEPT;
    }

    public HgStoreException(String message) {
        super(message);
        this.code = EC_FAIL;
    }

    public HgStoreException(int exceptCode, String message) {
        super(message);
        this.code = exceptCode;
    }

    public HgStoreException(int exceptCode, Throwable cause) {
        super(codeToMsg(exceptCode), cause);
        this.code = exceptCode;
    }

    public HgStoreException(int exceptCode, String message, Object... args) {
        super(String.format(message, args));
        this.code = exceptCode;
    }

    public HgStoreException(String message, Throwable cause) {
        super(message, cause);
        this.code = EC_FAIL;
    }

    public static String codeToMsg(int code) {
        return "errorCode = " + code;
    }

    public int getCode() {
        return this.code;
    }
}
