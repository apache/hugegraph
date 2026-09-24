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

package org.apache.hugegraph.store.node.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.node.util.HgRegexUtil;

import com.alipay.sofa.jraft.core.NodeMetrics;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

/**
 * 2022/1/4
 */
@Slf4j
public class JRaftMetrics {

    public final static String PREFIX = "jraft";
    public static final String LABELS = "quantile";
    public static final String LABEL_50 = "0.5";
    public static final String LABEL_75 = "0.75";
    public static final String LABEL_95 = "0.95";
    public static final String LABEL_98 = "0.98";
    public static final String LABEL_99 = "0.99";
    public static final String LABEL_999 = "0.999";
    private final static HgStoreEngine storeEngine = HgStoreEngine.getInstance();
    private final static AtomicInteger groups = new AtomicInteger(0);
    private final static Tag handleDataTag = Tag.of("handle", "data");
    // private final static Tag handleTxTag = Tag.of("handle", "tx"); //reservation
    private final static Set<String> groupSet = new HashSet<>();
    private final static String REGEX_REFINE_REPLICATOR = "(replicator)(.+?:\\d+)(.*)";
    private static MeterRegistry registry;

    private JRaftMetrics() {
    }

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        Gauge.builder(PREFIX + ".groups", JRaftMetrics::updateGroups)
             .description("Number of raft-groups, which handled the data of graph.")
             .tags(Collections.singleton(handleDataTag))
             .register(registry);

    }

    private static int updateGroups() {
        int buf = getGroups();
        if (buf != groups.get()) {
            groups.set(buf);
            registerNodeMetrics();
        }
        return buf;
    }

    private static int getGroups() {
        return storeEngine.getRaftGroupCount();
    }

    private static Map<String, NodeMetrics> getRaftGroupMetrics() {
        Map<String, NodeMetrics> map = storeEngine.getNodeMetrics();

        if (map == null) {
            return Collections.emptyMap();
        }

        return map;
    }

    private static void registerNodeMetrics() {
        Map<String, NodeMetrics> map = getRaftGroupMetrics();

        synchronized (groupSet) {
            map.forEach((group, metrics) -> {
                if (!groupSet.add(group)) {
                    return;
                }

                metrics.getMetricRegistry().getGauges()
                       .forEach((k, v) -> registerGauge(group, k, v));
                metrics.getMetricRegistry().getMeters()
                       .forEach((k, v) -> registerMeter(group, k, v));
                metrics.getMetricRegistry().getCounters()
                       .forEach((k, v) -> registerCounter(group, k, v));
                metrics.getMetricRegistry().getTimers()
                       .forEach((k, v) -> registerTimer(group, k, v));
                metrics.getMetricRegistry().getHistograms()
                       .forEach((k, v) -> registerHistogram(group, k, v));
            });
        }

    }

    private static String refineMetrics(String name, List<Tag> tags) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        List<String> buf = HgRegexUtil.toGroupValues(REGEX_REFINE_REPLICATOR, name);
        String res = null;

        /*Extracted name of replicator into a tag.*/

        if (buf != null && buf.size() == 4) {
            res = buf.get(1) + buf.get(3);

            String value = buf.get(2);

            if (value != null && value.startsWith("-")) {
                value = value.substring(1);
            }

            tags.add(Tag.of("replicator", value));
        } else {
            res = name;
        }

        return res;
    }

    private static void registerHistogram(String group, String name,
                                          com.codahale.metrics.Histogram histogram) {
        if (histogram == null) {
            return;
        }

        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        String baseName = PREFIX + "." + name.toLowerCase();

        Gauge.builder(baseName + ".median", histogram, h -> h.getSnapshot().getMedian())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".min", histogram, h -> h.getSnapshot().getMin())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".max", histogram, h -> h.getSnapshot().getMax())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".mean", histogram, h -> h.getSnapshot().getMean())
             .tags(tags).register(registry);

        baseName = baseName + ".summary";
        Gauge.builder(baseName, histogram, h -> h.getSnapshot().getMedian())
             .tags(tags).tag(LABELS, LABEL_50).register(registry);
        Gauge.builder(baseName, histogram, h -> h.getSnapshot().get75thPercentile())
             .tags(tags).tag(LABELS, LABEL_75).register(registry);
        Gauge.builder(baseName, histogram, h -> h.getSnapshot().get95thPercentile())
             .tags(tags).tag(LABELS, LABEL_95).register(registry);
        Gauge.builder(baseName, histogram, h -> h.getSnapshot().get98thPercentile())
             .tags(tags).tag(LABELS, LABEL_98).register(registry);
        Gauge.builder(baseName, histogram, h -> h.getSnapshot().get99thPercentile())
             .tags(tags).tag(LABELS, LABEL_99).register(registry);
        Gauge.builder(baseName, histogram, h -> h.getSnapshot().get999thPercentile())
             .tags(tags).tag(LABELS, LABEL_999).register(registry);

        Gauge.builder(baseName + ".sum", histogram,
                      h -> Arrays.stream(h.getSnapshot().getValues()).sum())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".count", histogram, h -> h.getSnapshot().size())
             .tags(tags).register(registry);

    }

    private static void registerTimer(String group, String name, com.codahale.metrics.Timer timer) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        String baseName = PREFIX + "." + name.toLowerCase();

        Gauge.builder(baseName + ".count", timer, t -> t.getCount())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".min", timer, t -> t.getSnapshot().getMin())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".max", timer, t -> t.getSnapshot().getMax())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".mean", timer, t -> t.getSnapshot().getMean())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".stddev", timer, t -> t.getSnapshot().getStdDev())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".p50", timer, t -> t.getSnapshot().getMedian())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".p75", timer, t -> t.getSnapshot().get75thPercentile())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".p95", timer, t -> t.getSnapshot().get95thPercentile())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".p98", timer, t -> t.getSnapshot().get98thPercentile())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".p99", timer, t -> t.getSnapshot().get99thPercentile())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".p999", timer, t -> t.getSnapshot().get999thPercentile())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".m1_rate", timer, t -> t.getOneMinuteRate())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".m5_rate", timer, t -> t.getFiveMinuteRate())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".m15_rate", timer, t -> t.getFifteenMinuteRate())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".mean_rate", timer, t -> t.getMeanRate())
             .tags(tags).register(registry);
    }

    private static void registerMeter(String group, String name, com.codahale.metrics.Meter meter) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        String baseName = PREFIX + "." + name.toLowerCase();

        Gauge.builder(baseName + ".count", meter, Meter::getCount)
             .tags(tags)
             .register(registry);

        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "1m")
             .register(registry);
        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "5m")
             .register(registry);
        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "15m")
             .register(registry);
        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "mean")
             .register(registry);

    }

    private static void registerCounter(String group, String name,
                                        com.codahale.metrics.Counter counter) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        name = name.toLowerCase();

        // Adapted a counter to be a gauge.
        Gauge.builder(PREFIX + "." + name + ".count", counter, Counter::getCount)
             .tags(tags).register(registry);
    }

    private static void registerGauge(String group, String name,
                                      com.codahale.metrics.Gauge<?> gauge) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        name = name.toLowerCase();

        if (gauge.getValue() instanceof Number) {
            Gauge.builder(PREFIX + "." + name, gauge, (g) -> ((Number) g.getValue()).doubleValue())
                 .tags(tags).register(registry);
        } else {
            Gauge.builder(PREFIX + "." + name, () -> 1.0)
                 .tags(tags)
                 .tag("str.gauge", String.valueOf(gauge.getValue())).register(registry);
        }

    }
}
