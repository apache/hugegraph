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

package org.apache.hugegraph.ranger;

/**
 * Keys for Ranger-specific properties read from rest-server.properties.
 *
 * <pre>
 *   # Name of the Ranger service definition registered in Ranger Admin
 *   auth.ranger.service_name=hugegraph
 *
 *   # Path to ranger-hugegraph-security.xml (Ranger client config)
 *   auth.ranger.config=/etc/ranger/hugegraph/ranger-hugegraph-security.xml
 * </pre>
 */
public final class RangerOptions {

    private RangerOptions() {
    }

    /** rest-server.properties key for the Ranger service name. */
    public static final String RANGER_SERVICE_NAME = "auth.ranger.service_name";

    /** rest-server.properties key for the path to ranger-*-security.xml. */
    public static final String RANGER_CONFIG_FILE = "auth.ranger.config";

    /** Default Ranger service name if not configured. */
    public static final String DEFAULT_SERVICE_NAME = "hugegraph";
}
