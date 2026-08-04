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

package org.apache.hugegraph.meta;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.type.define.GraphStatus;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

/**
 * The status of one graph on one server, stored as a json value under the
 * per-server graph status key.
 * <p>
 * Servers of different versions read each other's status, so a value written
 * with fields this version doesn't know must stay readable. An unknown status
 * value is a different matter: it can't be mapped to a state and the whole
 * entry is dropped, which leaves the server that wrote it uncounted and can
 * only hold the aggregate below READY.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GraphStatusEntry {

    public static final String SERVER_KEY = "server";
    public static final String STATUS_KEY = "status";
    public static final String MESSAGE_KEY = "message";
    public static final String UPDATE_TIME_KEY = "update_time";

    private static final Logger LOG = Log.logger(GraphStatusEntry.class);

    @JsonProperty(SERVER_KEY)
    private String server;

    @JsonProperty(STATUS_KEY)
    private GraphStatus status;

    @JsonProperty(MESSAGE_KEY)
    private String message;

    @JsonProperty(UPDATE_TIME_KEY)
    private long updateTime;

    public GraphStatusEntry() {
        // Pass
    }

    public GraphStatusEntry(String server, GraphStatus status, String message,
                            long updateTime) {
        this.server = server;
        this.status = status;
        this.message = message;
        this.updateTime = updateTime;
    }

    public static GraphStatusEntry fromValue(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            return JsonUtil.fromJson(value, GraphStatusEntry.class);
        } catch (RuntimeException e) {
            LOG.debug("Malformed graph status payload, ignoring: {}", value, e);
            return null;
        }
    }

    public String server() {
        return this.server;
    }

    public GraphStatus status() {
        return this.status;
    }

    public String message() {
        return this.message;
    }

    public long updateTime() {
        return this.updateTime;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(SERVER_KEY, this.server);
        map.put(STATUS_KEY, this.status == null ? null : this.status.name());
        if (this.message != null) {
            map.put(MESSAGE_KEY, this.message);
        }
        map.put(UPDATE_TIME_KEY, this.updateTime);
        return map;
    }
}
