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

package org.apache.hugegraph.api.graph;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.define.UpdateStrategy;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

public class BatchAPI extends API {

    private static final Logger LOG = Log.logger(BatchAPI.class);

    // NOTE: VertexAPI and EdgeAPI should share a counter
    private static final AtomicInteger BATCH_WRITE_THREADS = new AtomicInteger(0);

    static {
        MetricsUtil.registerGauge(RestServer.class, "batch-write-threads",
                                  BATCH_WRITE_THREADS::intValue);
    }

    private final Meter batchMeter;

    public BatchAPI() {
        this.batchMeter = MetricsUtil.registerMeter(this.getClass(), "batch-commit");
    }

    public <R> R commit(HugeConfig config, HugeGraph g, int size,
                        Callable<R> callable) {
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        int writingThreads = BATCH_WRITE_THREADS.incrementAndGet();
        if (writingThreads > maxWriteThreads) {
            BATCH_WRITE_THREADS.decrementAndGet();
            throw new HugeException("The rest server is too busy to write");
        }

        LOG.debug("The batch writing threads is {}", BATCH_WRITE_THREADS);
        try {
            R result = commit(g, callable);
            this.batchMeter.mark(size);
            return result;
        } finally {
            BATCH_WRITE_THREADS.decrementAndGet();
        }
    }

    @JsonIgnoreProperties(value = {"type"})
    @Schema(description = "Base class for vertex/edge in batch operations")
    protected abstract static class JsonElement implements Checkable {

        @Schema(description = "The vertex/edge ID. If not specified, " +
                               "it will be automatically generated based on ID strategy")
        @JsonProperty("id")
        public Object id;
        @Schema(description = "The vertex/edge label")
        @JsonProperty("label")
        public String label;
        @Schema(description = "The properties of the vertex/edge in key-value format")
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @Schema(description = "The type of element (vertex or edge)", hidden = true)
        @JsonProperty("type")
        public String type;

        @Override
        public abstract void checkCreate(boolean isBatch);

        @Override
        public abstract void checkUpdate();

        protected abstract Object[] properties();
    }

    protected void updateExistElement(JsonElement oldElement, JsonElement newElement,
                                      Map<String, UpdateStrategy> strategies) {
        if (oldElement == null) {
            return;
        }
        E.checkArgument(newElement != null, "The json element can't be null");

        for (Map.Entry<String, UpdateStrategy> kv : strategies.entrySet()) {
            String key = kv.getKey();
            UpdateStrategy updateStrategy = kv.getValue();
            if (oldElement.properties.get(key) != null &&
                newElement.properties.get(key) != null) {
                Object value = updateStrategy.checkAndUpdateProperty(
                        oldElement.properties.get(key),
                        newElement.properties.get(key));
                newElement.properties.put(key, value);
            } else if (oldElement.properties.get(key) != null &&
                       newElement.properties.get(key) == null) {
                // If new property is null & old is present, use old property
                newElement.properties.put(key, oldElement.properties.get(key));
            }
        }
    }

    protected void updateExistElement(HugeGraph g, Element oldElement, JsonElement newElement,
                                      Map<String, UpdateStrategy> strategies) {
        if (oldElement == null) {
            return;
        }
        E.checkArgument(newElement != null, "The json element can't be null");

        for (Map.Entry<String, UpdateStrategy> kv : strategies.entrySet()) {
            String key = kv.getKey();
            UpdateStrategy updateStrategy = kv.getValue();
            if (oldElement.property(key).isPresent() &&
                newElement.properties.get(key) != null) {
                Object value = updateStrategy.checkAndUpdateProperty(
                        oldElement.property(key).value(),
                        newElement.properties.get(key));
                value = g.propertyKey(key).validValueOrThrow(value);
                newElement.properties.put(key, value);
            } else if (oldElement.property(key).isPresent() &&
                       newElement.properties.get(key) == null) {
                // If new property is null & old is present, use old property
                newElement.properties.put(key, oldElement.value(key));
            }
        }
    }

    protected static void updateProperties(HugeElement element, JsonElement jsonElement,
                                           boolean append) {
        for (Map.Entry<String, Object> e : jsonElement.properties.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            if (append) {
                element.property(key, value);
            } else {
                element.property(key).remove();
            }
        }
    }
}
