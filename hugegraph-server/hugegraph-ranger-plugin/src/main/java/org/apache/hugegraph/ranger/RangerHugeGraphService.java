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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Ranger Admin-side service class for HugeGraph.
 * <p>
 * Loaded by Ranger Admin via {@code Class.newInstance()} to validate connection
 * configuration and provide resource lookup for the policy editor UI.
 * Must have a public no-arg constructor and must extend {@link RangerBaseService}.
 * <p>
 * This class is loaded by <b>Ranger Admin</b>, not by HugeGraph, so it must not
 * reference any {@code org.apache.hugegraph.*} class: the shaded plugin JAR
 * deliberately excludes hugegraph-core and hugegraph-api, and Ranger Admin's
 * classpath has neither. Resource names and resource-type values are therefore
 * duplicated here as literals rather than read from the HugeGraph enums.
 * <p>
 * For the policy editor's autocomplete to call {@link #lookupResource}, the
 * service definition's {@code implClass} must name this class <i>and</i> the
 * shaded plugin JAR must sit on Ranger Admin's classpath under
 * {@code ews/webapp/WEB-INF/classes/ranger-plugins/hugegraph/}.
 */
public class RangerHugeGraphService extends RangerBaseService {

    private static final Logger LOG =
            LoggerFactory.getLogger(RangerHugeGraphService.class);

    private static final String CONFIG_URL      = "hugegraph.url";
    private static final String CONFIG_USERNAME = "username";
    private static final String CONFIG_PASSWORD = "password";

    private static final String DEFAULT_URL        = "http://localhost:8080";
    private static final String DEFAULT_GRAPHSPACE = "DEFAULT";

    // Mirrors RangerHugeGraphPlugin.RES_* — kept as local literals so this
    // Admin-side class stays free of any dependency on the plugin class.
    private static final String RES_GRAPHSPACE    = "graphspace";
    private static final String RES_GRAPH         = "graph";
    private static final String RES_RESOURCE_TYPE = "resource-type";
    private static final String RES_LABEL         = "label";

    private static final int CONNECT_TIMEOUT = 5000;

    /*
     * Mirrors org.apache.hugegraph.auth.ResourceType, lowercased exactly the way
     * RangerHugeGraphPlugin.buildRequest() lowercases it when populating the
     * "resource-type" element of a RangerAccessResource. Keep in sync with that
     * enum; it cannot be referenced directly (see the class javadoc).
     */
    private static final List<String> RESOURCE_TYPES = Collections.unmodifiableList(
            Arrays.asList("none", "status", "vertex", "edge", "vertex_aggr", "edge_aggr",
                          "var", "gremlin", "task", "property_key", "vertex_label",
                          "edge_label", "index_label", "schema", "meta", "all", "grant",
                          "user_group", "project", "target", "metrics", "root"));

    public RangerHugeGraphService() {
        // Required: no-arg constructor for Ranger Admin Class.newInstance()
    }

    /**
     * Tests connectivity to HugeGraph by making a GET request to /versions.
     * Called by Ranger Admin's "Test Connection" button.
     */
    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        HashMap<String, Object> result = new HashMap<>();

        String testUrl = baseUrl() + "/versions";
        LOG.info("Testing connection to HugeGraph at {}", testUrl);

        try {
            HttpURLConnection conn = openConnection(testUrl);
            int code;
            try {
                code = conn.getResponseCode();
            } finally {
                conn.disconnect();
            }

            // 401/403 mean HugeGraph is up but rejected the configured
            // credentials — the endpoint is still reachable.
            if (code == 200 || code == 401 || code == 403) {
                result.put("connectivityStatus", true);
                result.put("message", "Connected to HugeGraph (HTTP " + code + ")");
                LOG.info("HugeGraph connection test succeeded (HTTP {})", code);
            } else {
                result.put("connectivityStatus", false);
                result.put("message", "Unexpected HTTP " + code + " from " + testUrl);
                LOG.warn("HugeGraph connection test failed (HTTP {})", code);
            }
        } catch (Exception e) {
            result.put("connectivityStatus", false);
            result.put("message", e.getMessage());
            LOG.error("HugeGraph connection test failed: {}", e.getMessage());
            throw e;
        }

        return result;
    }

    /**
     * Returns candidate resource values for the Ranger policy editor autocomplete,
     * read live from the HugeGraph REST API:
     * <ul>
     *   <li>{@code graphspace} — {@code GET /graphspaces}</li>
     *   <li>{@code graph} — {@code GET /graphspaces/{graphspace}/graphs}</li>
     *   <li>{@code resource-type} — the static {@link #RESOURCE_TYPES} list</li>
     *   <li>{@code label} — vertex and edge labels of the selected graph</li>
     * </ul>
     */
    @Override
    public List<String> lookupResource(ResourceLookupContext context) {
        if (context == null || context.getResourceName() == null) {
            return Collections.emptyList();
        }

        String resourceName = context.getResourceName();
        Map<String, List<String>> selected = context.getResources();
        List<String> candidates;

        try {
            switch (resourceName) {
                case RES_GRAPHSPACE:
                    candidates = lookupGraphSpaces();
                    break;
                case RES_GRAPH:
                    candidates = lookupGraphs(selectedOne(selected, RES_GRAPHSPACE,
                                                          DEFAULT_GRAPHSPACE));
                    break;
                case RES_RESOURCE_TYPE:
                    candidates = RESOURCE_TYPES;
                    break;
                case RES_LABEL:
                    candidates = lookupLabels(selectedOne(selected, RES_GRAPHSPACE,
                                                          DEFAULT_GRAPHSPACE),
                                              selectedOne(selected, RES_GRAPH, null));
                    break;
                default:
                    LOG.debug("No lookup support for resource '{}'", resourceName);
                    return Collections.emptyList();
            }
        } catch (Exception e) {
            // Never propagate: Ranger Admin renders a lookup exception as a UI
            // error dialog, which is worse than an empty autocomplete list.
            LOG.warn("Resource lookup for '{}' failed: {}", resourceName, e.toString());
            return Collections.emptyList();
        }

        return filterByUserInput(candidates, context.getUserInput());
    }

    // ------------------------------------------------------------------
    // Per-resource lookups
    // ------------------------------------------------------------------

    private List<String> lookupGraphSpaces() {
        try {
            return stringArray(getJson("/graphspaces"), "graphSpaces");
        } catch (Exception e) {
            // Standalone HugeGraph answers /graphspaces with HTTP 400
            // ("GraphSpace management is not supported in standalone mode");
            // there DEFAULT is the only graphspace that exists.
            LOG.debug("Listing graphspaces failed ({}), falling back to '{}'",
                      e.toString(), DEFAULT_GRAPHSPACE);
            return Collections.singletonList(DEFAULT_GRAPHSPACE);
        }
    }

    private List<String> lookupGraphs(String graphSpace) throws IOException {
        String path = "/graphspaces/" + encode(graphSpace) + "/graphs";
        return stringArray(getJson(path), "graphs");
    }

    private List<String> lookupLabels(String graphSpace, String graph) throws IOException {
        if (graph == null || graph.isEmpty() || "*".equals(graph)) {
            // Labels are graph-scoped; without a concrete graph there is
            // nothing to enumerate.
            return Collections.emptyList();
        }

        String schemaPath = "/graphspaces/" + encode(graphSpace) +
                            "/graphs/" + encode(graph) + "/schema/";
        List<String> labels = new ArrayList<>();
        labels.addAll(namesOf(getJson(schemaPath + "vertexlabels"), "vertexlabels"));
        labels.addAll(namesOf(getJson(schemaPath + "edgelabels"), "edgelabels"));
        return labels;
    }

    // ------------------------------------------------------------------
    // HTTP / JSON helpers
    // ------------------------------------------------------------------

    private JsonObject getJson(String path) throws IOException {
        String url = baseUrl() + path;
        HttpURLConnection conn = openConnection(url);
        try {
            int code = conn.getResponseCode();
            if (code != 200) {
                throw new IOException("HTTP " + code + " from " + url);
            }
            JsonElement parsed = JsonParser.parseString(readBody(conn.getInputStream()));
            if (!parsed.isJsonObject()) {
                throw new IOException("Expected a JSON object from " + url);
            }
            return parsed.getAsJsonObject();
        } finally {
            conn.disconnect();
        }
    }

    private HttpURLConnection openConnection(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(CONNECT_TIMEOUT);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        String username = configValue(CONFIG_USERNAME, null);
        String password = configValue(CONFIG_PASSWORD, null);
        if (username != null && !username.isEmpty()) {
            String creds = username + ":" + (password == null ? "" : password);
            String encoded = Base64.getEncoder().encodeToString(
                    creds.getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + encoded);
        }
        return conn;
    }

    private static String readBody(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = in.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Extracts a JSON array of strings, e.g. {@code {"graphs":["hugegraph"]}}.
     */
    private static List<String> stringArray(JsonObject root, String field) {
        List<String> values = new ArrayList<>();
        JsonArray array = arrayOf(root, field);
        for (int i = 0; array != null && i < array.size(); i++) {
            JsonElement element = array.get(i);
            if (element != null && element.isJsonPrimitive()) {
                values.add(element.getAsString());
            }
        }
        return values;
    }

    /**
     * Extracts the {@code name} of every object in a JSON array, e.g.
     * {@code {"vertexlabels":[{"name":"person",...}]}}.
     */
    private static List<String> namesOf(JsonObject root, String field) {
        List<String> names = new ArrayList<>();
        JsonArray array = arrayOf(root, field);
        for (int i = 0; array != null && i < array.size(); i++) {
            JsonElement element = array.get(i);
            if (element == null || !element.isJsonObject()) {
                continue;
            }
            JsonElement name = element.getAsJsonObject().get("name");
            if (name != null && name.isJsonPrimitive()) {
                names.add(name.getAsString());
            }
        }
        return names;
    }

    private static JsonArray arrayOf(JsonObject root, String field) {
        JsonElement element = root == null ? null : root.get(field);
        return element != null && element.isJsonArray() ? element.getAsJsonArray() : null;
    }

    // ------------------------------------------------------------------
    // Misc helpers
    // ------------------------------------------------------------------

    /**
     * Narrows candidates to those matching what the user has typed so far, then
     * de-duplicates and sorts them. Ranger Admin passes the partial input in
     * {@link ResourceLookupContext#getUserInput()}.
     */
    private static List<String> filterByUserInput(List<String> candidates, String userInput) {
        String prefix = userInput == null ? "" : userInput.trim().toLowerCase();
        TreeSet<String> matches = new TreeSet<>();
        for (String candidate : candidates) {
            if (candidate == null || candidate.isEmpty()) {
                continue;
            }
            if (prefix.isEmpty() || candidate.toLowerCase().startsWith(prefix)) {
                matches.add(candidate);
            }
        }
        return new ArrayList<>(matches);
    }

    /**
     * Returns the single parent resource value already chosen in the policy
     * editor, or {@code defaultValue} when none is selected yet.
     */
    private static String selectedOne(Map<String, List<String>> resources, String name,
                                      String defaultValue) {
        if (resources == null) {
            return defaultValue;
        }
        List<String> values = resources.get(name);
        if (values == null || values.isEmpty()) {
            return defaultValue;
        }
        String value = values.get(0);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private String baseUrl() {
        return configValue(CONFIG_URL, DEFAULT_URL).replaceAll("/+$", "");
    }

    private String configValue(String key, String defaultValue) {
        if (configs != null) {
            String value = configs.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return defaultValue;
    }

    private static String encode(String segment) {
        try {
            return URLEncoder.encode(segment, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // UTF-8 is always available
            throw new IllegalStateException(e);
        }
    }
}
