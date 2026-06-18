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

package org.apache.hugegraph.backend.store.hstore;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Utility for configuring cloud sync on hstore backend.
 * Usage:
 * <pre>
 *   HugeConfig config = ...;
 *   if (HstoreCloudConfigUtil.isCloudEnabled(config)) {
 *       HstoreCloudConfigUtil.configureStoreNodeEnvironment(config, storeNodeIndex);
 *   }
 * </pre>
 */
public class HstoreCloudConfigUtil {

    private static final Logger LOG = Log.logger(HstoreCloudConfigUtil.class);

    private HstoreCloudConfigUtil() {
        // Utility class
    }

    /**
     * Check if cloud sync is enabled via hstore configuration.
     */
    public static boolean isCloudEnabled(HugeConfig config) {
        return config.get(HstoreOptions.CLOUD_ENABLED);
    }

    /**
     * Print cloud configuration summary for debugging/logging.
     */
    public static String getConfigSummary(HugeConfig config) {
        if (!isCloudEnabled(config)) {
            return "Cloud sync disabled";
        }

        return String.format(
            "Cloud sync enabled: bucket=%s, region=%s, endpoint=%s, " +
            "syncMode=%s, syncIntervalSeconds=%s, pathStyle=%s",
            config.get(HstoreOptions.CLOUD_BUCKET),
            config.get(HstoreOptions.CLOUD_REGION),
            config.get(HstoreOptions.CLOUD_ENDPOINT),
            config.get(HstoreOptions.CLOUD_SYNC_MODE),
            config.get(HstoreOptions.CLOUD_SYNC_INTERVAL_SECONDS),
            config.get(HstoreOptions.CLOUD_PATH_STYLE)
        );
    }

    /**
     * Log cloud configuration if enabled.
     */
    public static void logCloudConfigIfEnabled(HugeConfig config) {
        if (isCloudEnabled(config)) {
            LOG.info("Hstore backend initialized with cloud sync: {}",
                    getConfigSummary(config));
        }
    }
}

