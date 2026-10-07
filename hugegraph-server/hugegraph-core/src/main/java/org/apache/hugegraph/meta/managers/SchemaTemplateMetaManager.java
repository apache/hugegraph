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

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SCHEMA_TEMPLATE;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.logging.log4j.util.Strings;

public class SchemaTemplateMetaManager extends AbstractMetaManager {

    public SchemaTemplateMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    public Set<String> schemaTemplates(String graphSpace) {
        Set<String> result = new HashSet<>();
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                this.schemaTemplatePrefix(graphSpace));
        for (String key : keyValues.keySet()) {
            String[] parts = key.split(META_PATH_DELIMITER);
            result.add(parts[parts.length - 1]);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public SchemaTemplate schemaTemplate(String graphSpace,
                                         String schemaTemplate) {
        String s = this.metaDriver.get(this.schemaTemplateKey(graphSpace,
                                                              schemaTemplate));
        if (StringUtils.isEmpty(s)) {
            return null;
        }
        return SchemaTemplate.fromMap(JsonUtil.fromJson(s, Map.class));
    }

    public void addSchemaTemplate(String graphSpace, SchemaTemplate template) {

        String key = this.schemaTemplateKey(graphSpace, template.name());

        String data = this.metaDriver.get(key);
        if (StringUtils.isNotEmpty(data)) {
            throw new HugeException("Cannot create schema template " +
                                    "since it has been created");
        }

        this.metaDriver.put(this.schemaTemplateKey(graphSpace, template.name()),
                            JsonUtil.toJson(template.asMap()));
    }

    public void updateSchemaTemplate(String graphSpace,
                                     SchemaTemplate template) {
        this.metaDriver.put(this.schemaTemplateKey(graphSpace, template.name()),
                            JsonUtil.toJson(template.asMap()));
    }

    public void removeSchemaTemplate(String graphSpace, String name) {
        this.metaDriver.delete(this.schemaTemplateKey(graphSpace, name));
    }

    public void clearSchemaTemplate(String graphSpace) {
        String prefix = this.schemaTemplatePrefix(graphSpace);
        this.metaDriver.deleteWithPrefix(prefix);
    }

    private String schemaTemplatePrefix(String graphSpace) {
        return this.schemaTemplateKey(graphSpace, Strings.EMPTY);
    }

    private String schemaTemplateKey(String graphSpace, String schemaTemplate) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SCHEMA_TEMPLATE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_SCHEMA_TEMPLATE,
                           schemaTemplate);
    }
}
