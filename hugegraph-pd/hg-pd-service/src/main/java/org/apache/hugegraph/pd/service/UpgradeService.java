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

package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.rest.API;
import org.apache.hugegraph.pd.upgrade.VersionScriptFactory;
import org.apache.hugegraph.pd.upgrade.VersionUpgradeScript;

import lombok.extern.slf4j.Slf4j;

@Useless("upgrade related")
@Slf4j
public class UpgradeService {

    private static final String VERSION_KEY = "DATA_VERSION";

    private static final String RUN_LOG_PREFIX = "SCRIPT_RUN_LOG";

    private PDConfig pdConfig;

    private KvService kvService;

    public UpgradeService(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
        this.kvService = new KvService(pdConfig);
    }

    public void upgrade() throws PDException {

        log.info("upgrade service start");
        VersionScriptFactory factory = VersionScriptFactory.getInstance();
        var dataVersion = getDataVersion();
        log.info("now db data version : {}", dataVersion);
        for (VersionUpgradeScript script : factory.getScripts()) {
            // Executed, run once skipped
            if (isExecuted(script.getClass().getName()) && script.isRunOnce()) {
                log.info("Script {} is Executed and is run once", script.getClass().getName());
                continue;
            }

            // Determine the conditions for skipping
            if (dataVersion == null && !script.isRunWithoutDataVersion() || dataVersion != null &&
                                                                            !versionCompare(
                                                                                    dataVersion,
                                                                                    script.getHighVersion(),
                                                                                    script.getLowVersion())) {
                log.info(
                        "Script {} is did not match version requirements, current data " +
                        "version:{}, current version:{}"
                        + "script run version({} to {}), run without data version:{}",
                        script.getClass().getName(),
                        dataVersion,
                        API.VERSION,
                        script.getHighVersion(),
                        script.getLowVersion(),
                        script.isRunWithoutDataVersion());
                continue;
            }

            script.runInstruction(pdConfig);
            logRun(script.getClass().getName());
        }

        writeCurrentDataVersion();
    }

    private boolean isExecuted(String className) throws PDException {
        var ret = kvService.get(RUN_LOG_PREFIX + "/" + className);
        return ret.length() > 0;
    }

    private void logRun(String className) throws PDException {
        kvService.put(RUN_LOG_PREFIX + "/" + className, API.VERSION);
    }

    private String getDataVersion() throws PDException {
        return kvService.get(VERSION_KEY);
    }

    private boolean versionCompare(String dataVersion, String high, String low) {
        var currentVersion = API.VERSION;
        return (high.equals(VersionUpgradeScript.UNLIMITED_VERSION) ||
                high.compareTo(dataVersion) >= 0)
               && (low.equals(VersionUpgradeScript.UNLIMITED_VERSION) ||
                   low.compareTo(currentVersion) <= 0);
    }

    private void writeCurrentDataVersion() throws PDException {
        log.info("update db version to {}", API.VERSION);
        kvService.put(VERSION_KEY, API.VERSION);
    }

}
