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

package org.apache.hugegraph.version;

import org.apache.hugegraph.util.VersionUtil;
import org.apache.hugegraph.util.VersionUtil.Version;

public class CoreVersion {

    public static final String NAME = "hugegraph-core";
    public static final String DEFAULT_VERSION = VersionUtil.getVersionFromProperties();
    /**
     * The second parameter of Version.of() is for IDE running without JAR
     */
    public static final Version VERSION = Version.of(CoreVersion.class, DEFAULT_VERSION);
    /**
     * Update it when the gremlin version changed, search "tinkerpop.version" in pom
     */
    public static final String GREMLIN_VERSION = "3.8.1";

    static {
        // Check versions of the dependency packages
        CoreVersion.check();
    }

    public static void check() {
        // Check the version of hugegraph-common
        VersionUtil.check(CommonVersion.VERSION, "1.0", "1.55", CommonVersion.NAME);
    }
}
