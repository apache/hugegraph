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

package org.apache.hugegraph.pd.upgrade;

import java.util.LinkedList;
import java.util.List;

import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.upgrade.scripts.PartitionMetaUpgrade;
import org.apache.hugegraph.pd.upgrade.scripts.TaskCleanUpgrade;

@Useless("upgrade related")
public class VersionScriptFactory {

    private static List<VersionUpgradeScript> SCRIPTS = new LinkedList<>();
    private static volatile VersionScriptFactory factory;

    static {
        registerScript(new PartitionMetaUpgrade());
        registerScript(new TaskCleanUpgrade());
    }

    private VersionScriptFactory() {

    }

    public static VersionScriptFactory getInstance() {
        if (factory == null) {
            synchronized (VersionScriptFactory.class) {
                if (factory == null) {
                    factory = new VersionScriptFactory();
                }
            }
        }
        return factory;
    }

    public static void registerScript(VersionUpgradeScript script) {
        SCRIPTS.add(script);
    }

    public List<VersionUpgradeScript> getScripts() {
        return SCRIPTS;
    }
}
