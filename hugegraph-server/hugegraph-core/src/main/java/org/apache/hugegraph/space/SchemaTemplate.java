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

package org.apache.hugegraph.space;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableMap;

public class SchemaTemplate {

    public static SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    protected Date createTime;
    protected Date updateTime;
    protected String creator;
    private final String name;
    private String schema;

    public SchemaTemplate(String name, String schema) {
        E.checkArgument(name != null && !name.isEmpty(),
                        "The name of schema template can't be null or empty");
        E.checkArgument(schema != null && !schema.isEmpty(),
                        "The schema template can't be null or empty");
        this.name = name;
        this.schema = schema;
        this.createTime = new Date();
        this.updateTime = createTime;
    }

    public SchemaTemplate(String name, String schema, Date create, String creator) {
        E.checkArgument(name != null && !name.isEmpty(),
                        "The name of schema template can't be null or empty");
        E.checkArgument(schema != null && !schema.isEmpty(),
                        "The schema template can't be null or empty");
        this.name = name;
        this.schema = schema;
        this.createTime = create;
        this.updateTime = createTime;

        this.creator = creator;
    }

    public static SchemaTemplate fromMap(Map<String, String> map) {
        try {
            SchemaTemplate template = new SchemaTemplate(map.get("name"),
                                                         map.get("schema"),
                                                         FORMATTER.parse(map.get("create")),
                                                         map.get("creator"));

            template.updateTime(FORMATTER.parse(map.get("update")));
            return template;

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    public String name() {
        return this.name;
    }

    public String schema() {
        return this.schema;
    }

    public void schema(String schema) {
        this.schema = schema;
    }

    public Date create() {
        return this.createTime;
    }

    public Date createTime() {
        return this.createTime;
    }

    public Date update() {
        return this.updateTime;
    }

    public Date updateTime() {
        return this.updateTime;
    }

    public void create(Date create) {
        this.createTime = create;
    }

    public String creator() {
        return this.creator;
    }

    public void creator(String creator) {
        this.creator = creator;
    }

    public void updateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public void refreshUpdateTime() {
        this.updateTime = new Date();
    }

    public Map<String, String> asMap() {
        String createStr = FORMATTER.format(this.createTime);
        String updateStr = FORMATTER.format(this.updateTime);
        return new ImmutableMap.Builder<String, String>()
                .put("name", this.name)
                .put("schema", this.schema)
                .put("create", createStr)
                .put("create_time", createStr)
                .put("update", updateStr)
                .put("update_time", updateStr)
                .put("creator", this.creator)
                .build();
    }
}
