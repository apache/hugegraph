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

package org.apache.hugegraph.struct.schema;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.type.define.Action;

public class Userdata extends HashMap<String, Object> {

    private static final long serialVersionUID = -1235451175617197049L;

    public static final String CREATE_TIME = "~create_time";
    public static final String DEFAULT_VALUE = "~default_value";

    public Userdata() {
    }

    public Userdata(Map<String, Object> map) {
        this.putAll(map);
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
