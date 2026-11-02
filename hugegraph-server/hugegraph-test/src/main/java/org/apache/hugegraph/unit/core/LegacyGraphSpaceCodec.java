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

package org.apache.hugegraph.unit.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.SafeDateUtil;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.slf4j.Logger;

/**
 * Frozen pre-change codec from Apache master b43b011d6c48b57d93ae4aa212a435144b64266d.
 * GraphSpaceDeserializer, GraphSpace fields, full constructor, setters and info() are
 * copied from hugegraph-core's io/HugeGraphSONModule.java and space/GraphSpace.java.
 * Only the model type is renamed; unrelated business methods are omitted.
 * Writing info() through JsonUtil matches the old GraphSpaceSerializer's field loop.
 * Keep this independent of the current GraphSpace codec: unknown fields must pass
 * through the old getValueAsString()/configs/putAll path during both round trips.
 */
@SuppressWarnings("serial")
final class LegacyGraphSpaceCodec {

    private static final Logger LOG = Log.logger(LegacyGraphSpaceCodec.class);
    private static final String DF = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(
            new SimpleModule().addDeserializer(LegacyGraphSpace.class, new GraphSpaceDeserializer()));

    private LegacyGraphSpaceCodec() {
    }

    static String rewrite(String json) throws IOException {
        return JsonUtil.toJson(MAPPER.readValue(json, LegacyGraphSpace.class).info());
    }

    static String create(String name, int usedGb) {
        LegacyGraphSpace space = new LegacyGraphSpace(name, "legacy", "legacy record",
                4, 8, 100, 100, 100, null, null, null,
                0, 0, usedGb, 0, 0, false, "anonymous", Map.of("user_config", "preserved"));
        return JsonUtil.toJson(space.info());
    }

    private static class GraphSpaceDeserializer
            extends StdDeserializer<LegacyGraphSpace> {

        public GraphSpaceDeserializer() {
            super(LegacyGraphSpace.class);
        }

        @Override
        public LegacyGraphSpace deserialize(JsonParser jsonParser,
                                      DeserializationContext ctxt)
                throws IOException {
            if (jsonParser.getCurrentToken() != JsonToken.START_OBJECT) {
                throw new HugeException("Invalid start marker");
            }

            String name = null;
            String nickname = null;
            String description = null;

            Number maxGraphNumber = 0;
            Number maxRoleNumber = 0;

            Number cpuLimit = 0;
            Number memoryLimit = 0;
            Number storageLimit = 0;

            Number computeCpuLimit = 0;
            Number computeMemoryLimit = 0;

            String oltpNamespace = null;
            String olapNamespace = null;
            String storageNamespace = null;

            Number cpuUsed = 0;
            Number memoryUsed = 0;
            Number storageUsed = 0;
            Number graphNumberUsed = 0;
            Number roleNumberUsed = 0;
            Boolean auth = false;

            String operatorImagePath = "";
            String internalAlgorithmImageUrl = "";

            String creator = LegacyGraphSpace.DEFAULT_CREATOR_NAME;
            Date create = null;
            Date update = null;

            Map<String, Object> configs = new HashMap<>();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jsonParser.getCurrentName();
                jsonParser.nextToken();
                if ("name".equals(fieldName)) {
                    name = jsonParser.getText();
                } else if ("nickname".equals(fieldName)) {
                    nickname = jsonParser.getText();
                } else if ("description".equals(fieldName)) {
                    description = jsonParser.getText();
                } else if ("max_graph_number".equals(fieldName)) {
                    maxGraphNumber = jsonParser.getNumberValue();
                } else if ("max_role_number".equals(fieldName)) {
                    maxRoleNumber = jsonParser.getNumberValue();
                } else if ("cpu_limit".equals(fieldName)) {
                    cpuLimit = jsonParser.getNumberValue();
                } else if ("memory_limit".equals(fieldName)) {
                    memoryLimit = jsonParser.getNumberValue();
                } else if ("compute_cpu_limit".equals(fieldName)) {
                    computeCpuLimit = jsonParser.getNumberValue();
                } else if ("compute_memory_limit".equals(fieldName)) {
                    computeMemoryLimit = jsonParser.getNumberValue();
                } else if ("storage_limit".equals(fieldName)) {
                    storageLimit = jsonParser.getNumberValue();
                } else if ("oltp_namespace".equals(fieldName)) {
                    oltpNamespace = jsonParser.getText();
                } else if ("olap_namespace".equals(fieldName)) {
                    olapNamespace = jsonParser.getText();
                } else if ("storage_namespace".equals(fieldName)) {
                    storageNamespace = jsonParser.getText();
                } else if ("cpu_used".equals(fieldName)) {
                    cpuUsed = jsonParser.getNumberValue();
                } else if ("memory_used".equals(fieldName)) {
                    memoryUsed = jsonParser.getNumberValue();
                } else if ("storage_used".equals(fieldName)) {
                    storageUsed = jsonParser.getNumberValue();
                } else if ("graph_number_used".equals(fieldName)) {
                    graphNumberUsed = jsonParser.getNumberValue();
                } else if ("role_number_used".equals(fieldName)) {
                    roleNumberUsed = jsonParser.getNumberValue();
                } else if ("auth".equals(fieldName)) {
                    auth = jsonParser.getBooleanValue();
                } else if ("operator_image_path".equals(fieldName)) {
                    operatorImagePath = jsonParser.getText();
                } else if ("internal_algorithm_image_url".equals(fieldName)) {
                    internalAlgorithmImageUrl = jsonParser.getText();
                } else if ("creator".equals(fieldName)) {
                    creator = jsonParser.getText();
                } else if ("create_time".equals(fieldName)) {
                    String val = jsonParser.getValueAsString();
                    if (val == null) {
                        create = new Date();
                    } else {
                        try {
                            create = SafeDateUtil.parse(val, DF);
                        } catch (ParseException e) {
                            LOG.warn("Failed to parse date: {}", val, e);
                            create = new Date();
                        }
                    }
                } else if ("update_time".equals(fieldName)) {
                    String val = jsonParser.getValueAsString();
                    if (val == null) {
                        update = new Date();
                    } else {
                        try {
                            update = SafeDateUtil.parse(val, DF);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            update = new Date();
                        }
                    }
                } else {
                    configs.put(fieldName, jsonParser.getValueAsString());
                }
            }
            jsonParser.close();

            LegacyGraphSpace space = new LegacyGraphSpace(name, nickname, description,
                                              cpuLimit.intValue(),
                                              memoryLimit.intValue(),
                                              storageLimit.intValue(),
                                              maxGraphNumber.intValue(),
                                              maxRoleNumber.intValue(),
                                              oltpNamespace,
                                              olapNamespace,
                                              storageNamespace,
                                              cpuUsed.intValue(),
                                              memoryUsed.intValue(),
                                              storageUsed.intValue(),
                                              graphNumberUsed.intValue(),
                                              roleNumberUsed.intValue(),
                                              auth,
                                              creator,
                                              configs);

            space.updateTime(update);
            space.createTime(create);
            space.computeCpuLimit(computeCpuLimit.intValue());
            space.computeMemoryLimit(computeMemoryLimit.intValue());
            space.operatorImagePath(operatorImagePath);
            space.internalAlgorithmImageUrl(internalAlgorithmImageUrl);
            return space;
        }
    }

    private static class LegacyGraphSpace {

        public static final String DEFAULT_GRAPH_SPACE_SERVICE_NAME = "DEFAULT";
        public static final String DEFAULT_NICKNAME = "默认图空间";
        public static final String DEFAULT_GRAPH_SPACE_DESCRIPTION =
                "The system default graph space";
        public static final String DEFAULT_CREATOR_NAME = "anonymous";

        public static final int DEFAULT_CPU_LIMIT = 4;
        public static final int DEFAULT_MEMORY_LIMIT = 8;
        public static final int DEFAULT_STORAGE_LIMIT = 100;

        public static final int DEFAULT_MAX_GRAPH_NUMBER = 100;
        public static final int DEFAULT_MAX_ROLE_NUMBER = 100;
        private final String creator;
        public int storageLimit; // GB
        public String oltpNamespace;
        private String name;
        private String nickname;
        private String description;
        private int cpuLimit;
        private int memoryLimit; // GB
        private int computeCpuLimit;
        private int computeMemoryLimit; // GB
        private String olapNamespace;
        private String storageNamespace;
        private int maxGraphNumber;
        private int maxRoleNumber;
        private Boolean auth;
        private Map<String, Object> configs;
        private int cpuUsed;
        private int memoryUsed; // GB
        private int storageUsed; // GB
        private int graphNumberUsed;
        private int roleNumberUsed;
        private String operatorImagePath = ""; // path of compute operator image
        private String internalAlgorithmImageUrl = "";
        private Date createTime;
        private Date updateTime;

        public LegacyGraphSpace(String name, String nickname, String description,
                          int cpuLimit,
                          int memoryLimit, int storageLimit, int maxGraphNumber,
                          int maxRoleNumber, String oltpNamespace,
                          String olapNamespace, String storageNamespace,
                          int cpuUsed, int memoryUsed, int storageUsed,
                          int graphNumberUsed, int roleNumberUsed,
                          boolean auth, String creator, Map<String, Object> config) {
            E.checkArgument(name != null && !StringUtils.isEmpty(name),
                            "The name of graph space can't be null or empty");
            E.checkArgument(cpuLimit > 0, "The cpu limit must > 0");
            E.checkArgument(memoryLimit > 0, "The memory limit must > 0");
            E.checkArgument(storageLimit > 0, "The storage limit must > 0");
            E.checkArgument(maxGraphNumber > 0, "The max graph number must > 0");
            this.name = name;
            this.nickname = nickname;
            this.description = description;

            this.cpuLimit = cpuLimit;
            this.memoryLimit = memoryLimit;
            this.storageLimit = storageLimit;

            this.maxGraphNumber = maxGraphNumber;
            this.maxRoleNumber = maxRoleNumber;

            this.oltpNamespace = oltpNamespace;
            this.olapNamespace = olapNamespace;
            this.storageNamespace = storageNamespace;

            this.cpuUsed = cpuUsed;
            this.memoryUsed = memoryUsed;
            this.storageUsed = storageUsed;

            this.graphNumberUsed = graphNumberUsed;
            this.roleNumberUsed = roleNumberUsed;

            this.auth = auth;
            this.creator = creator;

            this.configs = new HashMap<>();
            if (config != null) {
                this.configs = config;
            }
        }

        public void computeCpuLimit(int computeCpuLimit) {
            E.checkArgument(computeCpuLimit >= 0,
                            "The compute cpu limit must be >= 0, but got: %s", computeCpuLimit);
            this.computeCpuLimit = computeCpuLimit;
        }

        public void computeMemoryLimit(int computeMemoryLimit) {
            E.checkArgument(computeMemoryLimit >= 0,
                            "The compute memory limit must be >= 0, but got: %s",
                            computeMemoryLimit);
            this.computeMemoryLimit = computeMemoryLimit;
        }

        public void operatorImagePath(String path) {
            this.operatorImagePath = path;
        }

        public void internalAlgorithmImageUrl(String url) {
            if (StringUtils.isNotBlank(url)) {
                this.internalAlgorithmImageUrl = url;
            }
        }

        public void updateTime(Date update) {
            this.updateTime = update;
        }

        public void createTime(Date create) {
            this.createTime = create;
        }

        public Map<String, Object> info() {
            Map<String, Object> infos = new LinkedHashMap<>();
            infos.put("name", this.name);
            infos.put("nickname", this.nickname);
            infos.put("description", this.description);

            infos.put("cpu_limit", this.cpuLimit);
            infos.put("memory_limit", this.memoryLimit);
            infos.put("storage_limit", this.storageLimit);

            infos.put("compute_cpu_limit", this.computeCpuLimit);
            infos.put("compute_memory_limit", this.computeMemoryLimit);

            infos.put("oltp_namespace", this.oltpNamespace);
            infos.put("olap_namespace", this.olapNamespace);
            infos.put("storage_namespace", this.storageNamespace);

            infos.put("max_graph_number", this.maxGraphNumber);
            infos.put("max_role_number", this.maxRoleNumber);

            infos.putAll(this.configs);
            // sources used info is not automatically updated, it could be
            // updated by pdClient of GraphManager
            infos.put("cpu_used", this.cpuUsed);
            infos.put("memory_used", this.memoryUsed);
            infos.put("storage_used", this.storageUsed);
            // Locale.ROOT: the default locale may format with a decimal comma
            // ("0,33" on pl_PL, de_DE, fr_FR, ...), which parseFloat() rejects
            float storageUserPercent = Float.parseFloat(
                    String.format(Locale.ROOT, "%.2f", (float) this.storageUsed /
                                                       ((float) this.storageLimit * 1.0)));
            infos.put("storage_percent", storageUserPercent);
            infos.put("graph_number_used", this.graphNumberUsed);
            infos.put("role_number_used", this.roleNumberUsed);

            infos.put("auth", this.auth);

            infos.put("operator_image_path", this.operatorImagePath);
            infos.put("internal_algorithm_image_url", this.internalAlgorithmImageUrl);

            infos.put("create_time", this.createTime);
            infos.put("update_time", this.updateTime);
            infos.put("creator", this.creator);
            return infos;
        }
    }
}
