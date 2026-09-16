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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    private static final String CONFIG_USERNAME = "username";
    private static final String CONFIG_PASSWORD = "password";
    private static final int    CONNECT_TIMEOUT = 5000;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Mirrors {@code org.apache.hugegraph.auth.ResourceType}'s lowercased enum names.
     * Hardcoded rather than referencing that enum directly: {@code hugegraph-core} is
     * deliberately excluded from this jar (it's already on the HugeGraph server's own
     * classpath) but is NOT present on Ranger Admin's classpath, where this class is
     * loaded — importing it would throw NoClassDefFoundError on every lookup call.
     */
    private static final String[] RESOURCE_TYPES = {
            "none", "status", "vertex", "edge", "vertex_aggr", "edge_aggr", "var",
            "gremlin", "task", "property_key", "vertex_label", "edge_label",
            "index_label", "schema", "meta", "all", "grant", "user_group",
            "project", "target", "metrics", "root"
    };

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

        String baseUrl = getBaseUrl();
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
     * Returns candidate resource values for the Ranger policy editor autocomplete,
     * filtered by the user's typed prefix and scoped by any already-selected
     * parent resources (e.g. graph depends on graphspace).
     */
    @Override
    public List<String> lookupResource(ResourceLookupContext context) {
        String resourceName = context.getResourceName();
        Map<String, List<String>> resources = context.getResources();
        String userInput = context.getUserInput();

        List<String> values;
        try {
            switch (resourceName) {
                case RangerHugeGraphPlugin.RES_GRAPHSPACE:
                    values = lookupGraphSpaces();
                    break;
                case RangerHugeGraphPlugin.RES_GRAPH:
                    String parentGs = firstValue(resources, RangerHugeGraphPlugin.RES_GRAPHSPACE);
                    values = lookupGraphs(parentGs);
                    break;
                case RangerHugeGraphPlugin.RES_RESOURCE_TYPE:
                    values = lookupResourceTypes();
                    break;
                case RangerHugeGraphPlugin.RES_LABEL:
                    String labelGs = firstValue(resources, RangerHugeGraphPlugin.RES_GRAPHSPACE);
                    String labelGraph = firstValue(resources, RangerHugeGraphPlugin.RES_GRAPH);
                    values = lookupLabels(labelGs, labelGraph);
                    break;
                default:
                    values = new ArrayList<>();
            }
        } catch (Exception e) {
            LOG.error("lookupResource failed for resourceName={}: {}",
                      resourceName, e.getMessage());
            return new ArrayList<>();
        }

        if (userInput == null || userInput.isEmpty()) {
            return values;
        }
        List<String> filtered = new ArrayList<>();
        for (String value : values) {
            if (value.startsWith(userInput)) {
                filtered.add(value);
            }
        }
        return filtered;
    }

    // ------------------------------------------------------------------
    // Resource lookup helpers
    // ------------------------------------------------------------------

    private List<String> lookupGraphSpaces() {
        JsonNode root = getJson(getBaseUrl() + "/graphspaces");
        return toStringList(root, "graphSpaces");
    }

    private List<String> lookupGraphs(String graphSpace) {
        if (graphSpace == null) {
            return new ArrayList<>();
        }
        JsonNode root = getJson(getBaseUrl() + "/graphspaces/" + graphSpace + "/graphs");
        return toStringList(root, "graphs");
    }

    private List<String> lookupResourceTypes() {
        return new ArrayList<>(Arrays.asList(RESOURCE_TYPES));
    }

    private List<String> lookupLabels(String graphSpace, String graph) {
        if (graphSpace == null || graph == null) {
            return new ArrayList<>();
        }
        String schemaUrl = getBaseUrl() + "/graphspaces/" + graphSpace +
                            "/graphs/" + graph + "/schema";

        Set<String> labels = new LinkedHashSet<>();
        labels.addAll(toStringList(getJson(schemaUrl + "/vertexlabels"), "vertexlabels", "name"));
        labels.addAll(toStringList(getJson(schemaUrl + "/edgelabels"), "edgelabels", "name"));
        return new ArrayList<>(labels);
    }

    // ------------------------------------------------------------------
    // HTTP / config helpers
    // ------------------------------------------------------------------

    private JsonNode getJson(String url) {
        try {
            HttpURLConnection conn = openConnection(url);
            int code = conn.getResponseCode();
            if (code != 200) {
                LOG.warn("HugeGraph lookup request to {} returned HTTP {}", url, code);
                return null;
            }
            try (InputStream in = conn.getInputStream()) {
                return MAPPER.readTree(in);
            }
        } catch (IOException e) {
            LOG.error("HugeGraph lookup request to {} failed: {}", url, e.getMessage());
            return null;
        }
    }

    /**
     * Extracts the string values of a top-level array field, treating each
     * element as a plain string (e.g. {@code {"graphSpaces": ["a", "b"]}}).
     */
    private List<String> toStringList(JsonNode root, String arrayField) {
        List<String> values = new ArrayList<>();
        if (root == null || !root.has(arrayField)) {
            return values;
        }
        for (JsonNode element : root.get(arrayField)) {
            values.add(element.asText());
        }
        return values;
    }

    /**
     * Extracts a string field from each element of a top-level array
     * (e.g. {@code {"vertexlabels": [{"name": "person"}, ...]}}).
     */
    private List<String> toStringList(JsonNode root, String arrayField, String elementField) {
        List<String> values = new ArrayList<>();
        if (root == null || !root.has(arrayField)) {
            return values;
        }
        for (JsonNode element : root.get(arrayField)) {
            JsonNode field = element.get(elementField);
            if (field != null) {
                values.add(field.asText());
            }
        }
        return values;
    }

    private static String firstValue(Map<String, List<String>> resources, String key) {
        if (resources == null) {
            return null;
        }
        List<String> values = resources.get(key);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    private HttpURLConnection openConnection(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(CONNECT_TIMEOUT);
        conn.setRequestMethod("GET");

        String username = configs != null ? configs.get(CONFIG_USERNAME) : null;
        String password = configs != null ? configs.get(CONFIG_PASSWORD) : null;
        if (username != null && password != null) {
            String credentials = username + ":" + password;
            String encoded = Base64.getEncoder().encodeToString(
                    credentials.getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + encoded);
        }
        return conn;
    }

    private String getBaseUrl() {
        if (configs != null && configs.containsKey(RangerHugeGraphService.CONFIG_URL)) {
            String v = configs.get(RangerHugeGraphService.CONFIG_URL);
            if (v != null && !v.isEmpty()) {
                return v.replaceAll("/+$", "");
            }
        }
        return "http://localhost:8080";
    }
}
