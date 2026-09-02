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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ranger Admin-side service class for HugeGraph.
 * <p>
 * Loaded by Ranger Admin via {@code Class.newInstance()} to validate connection
 * configuration and provide resource lookup for the policy editor UI.
 * Must have a public no-arg constructor and must extend {@link RangerBaseService}.
 */
public class RangerHugeGraphService extends RangerBaseService {

    private static final Logger LOG =
            LoggerFactory.getLogger(RangerHugeGraphService.class);

    private static final String CONFIG_URL      = "hugegraph.url";
    private static final int    CONNECT_TIMEOUT = 5000;

    public RangerHugeGraphService() {
        // Required: no-arg constructor for Ranger Admin Class.newInstance()
    }

    /**
     * Tests connectivity to HugeGraph by making a GET request to /apis/version.
     * Called by Ranger Admin's "Test Connection" button.
     */
    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        HashMap<String, Object> result = new HashMap<>();

        String baseUrl = getConfigValue();
        String testUrl = baseUrl.replaceAll("/+$", "") + "/apis/version";

        LOG.info("Testing connection to HugeGraph at {}", testUrl);

        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(testUrl).openConnection();
            conn.setConnectTimeout(CONNECT_TIMEOUT);
            conn.setReadTimeout(CONNECT_TIMEOUT);
            conn.setRequestMethod("GET");

            int code = conn.getResponseCode();
            if (code == 200 || code == 401) {
                // 401 means HugeGraph is running but requires auth — still reachable
                result.put("connectivityStatus", "success");
                LOG.info("HugeGraph connection test succeeded (HTTP {})", code);
            } else {
                result.put("connectivityStatus", "failed");
                LOG.warn("HugeGraph connection test failed (HTTP {})", code);
            }
        } catch (Exception e) {
            result.put("connectivityStatus", "failed");
            result.put("message", e.getMessage());
            LOG.error("HugeGraph connection test failed: {}", e.getMessage());
            throw e;
        }

        return result;
    }

    /**
     * Returns candidate resource values for the Ranger policy editor autocomplete.
     * Currently returns an empty list; can be extended to list graphs/labels.
     */
    @Override
    public List<String> lookupResource(ResourceLookupContext context) {
        return new ArrayList<>();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private String getConfigValue() {
        if (configs != null && configs.containsKey(RangerHugeGraphService.CONFIG_URL)) {
            String v = configs.get(RangerHugeGraphService.CONFIG_URL);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return "http://localhost:8080";
    }
}
