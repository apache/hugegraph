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

package org.apache.hugegraph.metrics;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.server.util.MetricManager;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricsUtil {

    public static final String METRICS_PATH_TOTAL_COUNTER = "TOTAL_COUNTER";
    public static final String METRICS_PATH_FAILED_COUNTER = "FAILED_COUNTER";
    public static final String METRICS_PATH_SUCCESS_COUNTER = "SUCCESS_COUNTER";
    public static final String METRICS_PATH_RESPONSE_TIME_HISTOGRAM =
            "RESPONSE_TIME_HISTOGRAM";
    public static final String P75_ATTR = "{name=\"p75\",} ";
    public static final String P95_ATTR = "{name=\"p95\",} ";
    public static final String P98_ATTR = "{name=\"p98\",} ";
    public static final String P99_ATTR = "{name=\"p99\",} ";
    public static final String P999_ATTR = "{name=\"p999\",} ";
    public static final String MEAN_RATE_ATRR = "{name=\"mean_rate\",} ";
    public static final String ONE_MIN_RATE_ATRR = "{name=\"m1_rate\",} ";
    public static final String FIVE_MIN_RATE_ATRR = "{name=\"m5_rate\",} ";
    public static final String FIFT_MIN_RATE_ATRR = "{name=\"m15_rate\",} ";
    public static final MetricRegistry REGISTRY = MetricManager.INSTANCE.getRegistry();
    public static final String STR_HELP = "# HELP ";
    public static final String STR_TYPE = "# TYPE ";
    public static final String HISTOGRAM_TYPE = "histogram";
    public static final String UNTYPED = "untyped";
    public static final String GAUGE_TYPE = "gauge";
    public static final String END_LSTR = "\n";
    public static final String SPACE_STR = " ";
    public static final String VERSION_STR = "{version=\"";
    public static final String COUNT_ATTR = "{name=\"count\",} ";
    public static final String MIN_ATTR = "{name=\"min\",} ";
    public static final String MAX_ATTR = "{name=\"max\",} ";
    public static final String MEAN_ATTR = "{name=\"mean\",} ";
    public static final String STDDEV_ATTR = "{name=\"stddev\",} ";
    public static final String P50_ATTR = "{name=\"p50\",} ";

    public static final String LEFT_NAME_STR = "{name=\"";
    public static final String RIGHT_NAME_STR = "\",} ";
    public static final String PROM_HELP_NAME = "hugegraph_info";

    private static final Pattern INVALID_PROMETHEUS_NAME_CHAR =
            Pattern.compile("[^a-zA-Z0-9_:]");

    public static <T> Gauge<T> registerGauge(Class<?> clazz, String name,
                                             Gauge<T> gauge) {
        return REGISTRY.register(MetricRegistry.name(clazz, name), gauge);
    }

    public static Counter registerCounter(Class<?> clazz, String name) {
        return REGISTRY.counter(MetricRegistry.name(clazz, name));
    }

    public static Counter registerCounter(String name) {
        return REGISTRY.counter(MetricRegistry.name(name));
    }

    public static Histogram registerHistogram(Class<?> clazz, String name) {
        return REGISTRY.histogram(MetricRegistry.name(clazz, name));
    }

    public static Histogram registerHistogram(String name) {
        return REGISTRY.histogram(name);
    }

    public static Meter registerMeter(Class<?> clazz, String name) {
        return REGISTRY.meter(MetricRegistry.name(clazz, name));
    }

    public static Timer registerTimer(Class<?> clazz, String name) {
        return REGISTRY.timer(MetricRegistry.name(clazz, name));
    }

    /**
     * Convert a metric key into a valid Prometheus metric name, which must
     * match [a-zA-Z_:][a-zA-Z0-9_:]*. Every other character becomes '_',
     * and a leading digit gets a '_' prefix. Only call this when writing
     * Prometheus output; registry keys stay unchanged.
     */
    public static String sanitizePrometheusMetricName(String originalName) {
        String sanitized = INVALID_PROMETHEUS_NAME_CHAR.matcher(originalName)
                                                       .replaceAll("_");
        if (sanitized.isEmpty()) {
            return "_";
        }
        if (Character.isDigit(sanitized.charAt(0))) {
            return "_" + sanitized;
        }
        return sanitized;
    }

    public static void writePrometheusFormat(StringBuilder promeMetrics, MetricRegistry registry) {
        // gauges
        registry.getGauges().forEach((key, gauge) -> {
            if (gauge != null) {
                String helpName = sanitizePrometheusMetricName(key);
                promeMetrics.append(STR_HELP)
                            .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                            .append(helpName).append(SPACE_STR + GAUGE_TYPE + END_LSTR);
                promeMetrics.append(helpName).append(SPACE_STR).append(gauge.getValue())
                            .append(END_LSTR);
            }
        });

        // histograms
        registry.getHistograms().forEach((key, histogram) -> {
            if (histogram != null) {
                String helpName = sanitizePrometheusMetricName(key);
                promeMetrics.append(STR_HELP)
                            .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                            .append(helpName)
                            .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promeMetrics.append(helpName)
                            .append(COUNT_ATTR).append(histogram.getCount()).append(END_LSTR);
                promeMetrics.append(
                        exportSnapshot(helpName, histogram.getSnapshot()));
            }
        });

        // meters
        registry.getMeters().forEach((key, metric) -> {
            if (metric != null) {
                String helpName = sanitizePrometheusMetricName(key);
                promeMetrics.append(STR_HELP)
                            .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                            .append(helpName)
                            .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promeMetrics.append(helpName)
                            .append(COUNT_ATTR).append(metric.getCount()).append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(MEAN_RATE_ATRR).append(metric.getMeanRate()).append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(ONE_MIN_RATE_ATRR).append(metric.getOneMinuteRate())
                            .append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(FIVE_MIN_RATE_ATRR).append(metric.getFiveMinuteRate())
                            .append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(FIFT_MIN_RATE_ATRR).append(metric.getFifteenMinuteRate())
                            .append(END_LSTR);
            }
        });

        // timer
        registry.getTimers().forEach((key, timer) -> {
            if (timer != null) {
                String helpName = sanitizePrometheusMetricName(key);
                promeMetrics.append(STR_HELP)
                            .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                            .append(helpName)
                            .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promeMetrics.append(helpName)
                            .append(COUNT_ATTR).append(timer.getCount()).append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(ONE_MIN_RATE_ATRR).append(timer.getOneMinuteRate())
                            .append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(FIVE_MIN_RATE_ATRR).append(timer.getFiveMinuteRate())
                            .append(END_LSTR);
                promeMetrics.append(helpName)
                            .append(FIFT_MIN_RATE_ATRR).append(timer.getFifteenMinuteRate())
                            .append(END_LSTR);
                promeMetrics.append(
                        exportSnapshot(helpName, timer.getSnapshot()));
            }
        });
    }

    /**
     * Write the per-path request statistics, one gauge family per path with
     * a quoted "name" label for each statistic.
     */
    public static void writeStatisticsPrometheusFormat(
            StringBuilder promeMetrics, Map<String, Map<String, Object>> metricMap) {
        metricMap.forEach((methodKey, itemMetricMap) -> {
            String metricName = sanitizePrometheusMetricName(methodKey);
            promeMetrics.append(STR_HELP)
                        .append(metricName).append(END_LSTR);
            promeMetrics.append(STR_TYPE)
                        .append(metricName).append(SPACE_STR + GAUGE_TYPE + END_LSTR);
            itemMetricMap.forEach((labelName, value) -> {
                promeMetrics.append(metricName).append(LEFT_NAME_STR).append(labelName)
                            .append(RIGHT_NAME_STR).append(value).append(END_LSTR);
            });
        });
    }

    public static String exportSnapshot(final String helpName, final Snapshot snapshot) {
        if (snapshot == null) {
            return "";
        }
        StringBuilder snapMetrics = new StringBuilder();
        snapMetrics.append(helpName)
                   .append(MIN_ATTR).append(snapshot.getMin()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(MAX_ATTR).append(snapshot.getMax()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(MEAN_ATTR).append(snapshot.getMean()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(STDDEV_ATTR).append(snapshot.getStdDev()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(P50_ATTR).append(snapshot.getMedian()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(P75_ATTR).append(snapshot.get75thPercentile()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(P95_ATTR).append(snapshot.get95thPercentile()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(P98_ATTR).append(snapshot.get98thPercentile()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(P99_ATTR).append(snapshot.get99thPercentile()).append(END_LSTR);
        snapMetrics.append(helpName)
                   .append(P999_ATTR).append(snapshot.get999thPercentile()).append(END_LSTR);
        return snapMetrics.toString();
    }
}
