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

package org.apache.hugegraph.api.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hugegraph.metrics.MetricsUtil.COUNT_ATTR;
import static org.apache.hugegraph.metrics.MetricsUtil.END_LSTR;
import static org.apache.hugegraph.metrics.MetricsUtil.FIFT_MIN_RATE_ATRR;
import static org.apache.hugegraph.metrics.MetricsUtil.FIVE_MIN_RATE_ATRR;
import static org.apache.hugegraph.metrics.MetricsUtil.GAUGE_TYPE;
import static org.apache.hugegraph.metrics.MetricsUtil.HISTOGRAM_TYPE;
import static org.apache.hugegraph.metrics.MetricsUtil.LEFT_NAME_STR;
import static org.apache.hugegraph.metrics.MetricsUtil.MEAN_RATE_ATRR;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_FAILED_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_RESPONSE_TIME_HISTOGRAM;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_SUCCESS_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_TOTAL_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.ONE_MIN_RATE_ATRR;
import static org.apache.hugegraph.metrics.MetricsUtil.PROM_HELP_NAME;
import static org.apache.hugegraph.metrics.MetricsUtil.RIGHT_NAME_STR;
import static org.apache.hugegraph.metrics.MetricsUtil.SPACE_STR;
import static org.apache.hugegraph.metrics.MetricsUtil.STR_HELP;
import static org.apache.hugegraph.metrics.MetricsUtil.STR_TYPE;
import static org.apache.hugegraph.metrics.MetricsUtil.UNTYPED;
import static org.apache.hugegraph.metrics.MetricsUtil.VERSION_STR;
import static org.apache.hugegraph.metrics.MetricsUtil.exportSnapshot;
import static org.apache.hugegraph.metrics.MetricsUtil.replaceDotDashInKey;
import static org.apache.hugegraph.metrics.MetricsUtil.replaceSlashInKey;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.store.BackendMetrics;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.metrics.MetricsKeys;
import org.apache.hugegraph.metrics.MetricsModule;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.metrics.ServerReporter;
import org.apache.hugegraph.metrics.SystemMetrics;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.version.ApiVersion;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Singleton
@Path("metrics")
@Tag(name = "MetricsAPI")
public class MetricsAPI extends API {

    private static final Logger LOG = Log.logger(MetricsAPI.class);

    private static final String JSON_STR = "json";

    static {
        JsonUtil.registerModule(new MetricsModule(SECONDS, MILLISECONDS, false));
    }

    private final SystemMetrics systemMetrics;

    public MetricsAPI() {
        this.systemMetrics = new SystemMetrics();
    }

    @GET
    @Timed
    @Path("system")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the system metrics")
    public String system() {
        return JsonUtil.toJson(this.systemMetrics.metrics());
    }

    @GET
    @Timed
    @Path("backend")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the backend metrics")
    public String backend(@Context GraphManager manager) {
        Map<String, Map<String, Object>> results = InsertionOrderUtil.newMap();
        for (String graph : manager.graphs()) {
            HugeGraph g = manager.graph(graph);
            Map<String, Object> metrics = InsertionOrderUtil.newMap();
            metrics.put(BackendMetrics.BACKEND, g.backend());
            try {
                metrics.putAll(g.metadata(null, "metrics"));
            } catch (Throwable e) {
                metrics.put(BackendMetrics.EXCEPTION, e.toString());
                LOG.debug("Failed to get backend metrics", e);
            }
            results.put(graph, metrics);
        }
        return JsonUtil.toJson(results);
    }

    @GET
    @Timed
    @Path("gauges")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the gauges metrics")
    public String gauges() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.gauges());
    }

    @GET
    @Timed
    @Path("counters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the counters metrics")
    public String counters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.counters());
    }

    @GET
    @Timed
    @Path("histograms")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the histograms metrics")
    public String histograms() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.histograms());
    }

    @GET
    @Timed
    @Path("meters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the meters metrics")
    public String meters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.meters());
    }

    @GET
    @Timed
    @Path("timers")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get the timers metrics")
    public String timers() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.timers());
    }

    @GET
    @Timed
    @Produces(APPLICATION_TEXT_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get all base metrics")
    public String all(@Context GraphManager manager,
                      @Parameter(description = "Output format type: 'json' for JSON format, " +
                                               "other values for Prometheus format")
                      @QueryParam("type") String type) {
        if (type != null && type.equals(JSON_STR)) {
            return baseMetricAll();
        } else {
            return baseMetricPrometheusAll();
        }
    }

    @GET
    @Path("statistics")
    @Timed
    @Produces(APPLICATION_TEXT_WITH_CHARSET)
    @RolesAllowed({"space", "$owner= $action=metrics_read"})
    @Operation(summary = "get all statistics metrics")
    public String statistics(
            @Parameter(description = "Output format type: 'json' for JSON format")
            @QueryParam("type") String type) {
        Map<String, Map<String, Object>> metricMap = statistics();

        if (type != null && type.equals(JSON_STR)) {
            return JsonUtil.toJson(metricMap);
        }
        return statisticsProm(metricMap);
    }

    public String baseMetricAll() {
        ServerReporter reporter = ServerReporter.instance();
        Map<String, Map<String, ? extends Metric>> result = new LinkedHashMap<>();
        result.put("gauges", reporter.gauges());
        result.put("counters", reporter.counters());
        result.put("histograms", reporter.histograms());
        result.put("meters", reporter.meters());
        result.put("timers", reporter.timers());
        return JsonUtil.toJson(result);
    }

    private String baseMetricPrometheusAll() {
        StringBuilder promMetric = new StringBuilder();
        ServerReporter reporter = ServerReporter.instance();
        String helpName = PROM_HELP_NAME;
        // build version info
        promMetric.append(STR_HELP)
                  .append(helpName).append(END_LSTR);
        promMetric.append(STR_TYPE)
                  .append(helpName)
                  .append(SPACE_STR + UNTYPED + END_LSTR);
        promMetric.append(helpName)
                  .append(VERSION_STR)
                  .append(ApiVersion.VERSION.toString()).append("\",}")
                  .append(SPACE_STR + "1.0" + END_LSTR);

        // build gauges metric info
        for (String key : reporter.gauges().keySet()) {
            final Gauge<?> gauge
                    = reporter.gauges().get(key);
            if (gauge != null) {
                helpName = replaceDotDashInKey(key);
                promMetric.append(STR_HELP)
                          .append(helpName).append(END_LSTR);
                promMetric.append(STR_TYPE)
                          .append(helpName).append(SPACE_STR + GAUGE_TYPE + END_LSTR);
                promMetric.append(helpName)
                          .append(SPACE_STR + gauge.getValue() + END_LSTR);
            }
        }

        // build histograms metric info
        for (String histogramkey : reporter.histograms().keySet()) {
            final Histogram histogram = reporter.histograms().get(histogramkey);
            if (histogram != null) {
                helpName = replaceDotDashInKey(histogramkey);
                promMetric.append(STR_HELP)
                          .append(helpName).append(END_LSTR);
                promMetric.append(STR_TYPE)
                          .append(helpName)
                          .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promMetric.append(helpName)
                          .append(COUNT_ATTR)
                          .append(histogram.getCount() + END_LSTR);
                promMetric.append(
                        exportSnapshot(helpName, histogram.getSnapshot()));
            }
        }

        // build meters metric info
        for (String meterkey : reporter.meters().keySet()) {
            final Meter metric = reporter.meters().get(meterkey);
            if (metric != null) {
                helpName = replaceDotDashInKey(meterkey);
                promMetric.append(STR_HELP)
                          .append(helpName).append(END_LSTR);
                promMetric.append(STR_TYPE)
                          .append(helpName)
                          .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promMetric.append(helpName)
                          .append(COUNT_ATTR)
                          .append(metric.getCount() + END_LSTR);
                promMetric.append(helpName)
                          .append(MEAN_RATE_ATRR)
                          .append(metric.getMeanRate() + END_LSTR);
                promMetric.append(helpName)
                          .append(ONE_MIN_RATE_ATRR)
                          .append(metric.getOneMinuteRate() + END_LSTR);
                promMetric.append(helpName)
                          .append(FIVE_MIN_RATE_ATRR)
                          .append(metric.getFiveMinuteRate() + END_LSTR);
                promMetric.append(helpName)
                          .append(FIFT_MIN_RATE_ATRR)
                          .append(metric.getFifteenMinuteRate() + END_LSTR);
            }
        }

        // build timer metric info
        for (String timerkey : reporter.timers().keySet()) {
            final com.codahale.metrics.Timer timer = reporter.timers()
                                                             .get(timerkey);
            if (timer != null) {
                helpName = replaceDotDashInKey(timerkey);
                promMetric.append(STR_HELP)
                          .append(helpName).append(END_LSTR);
                promMetric.append(STR_TYPE)
                          .append(helpName)
                          .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promMetric.append(helpName)
                          .append(COUNT_ATTR)
                          .append(timer.getCount() + END_LSTR);
                promMetric.append(helpName)
                          .append(ONE_MIN_RATE_ATRR)
                          .append(timer.getOneMinuteRate() + END_LSTR);
                promMetric.append(helpName)
                          .append(FIVE_MIN_RATE_ATRR)
                          .append(timer.getFiveMinuteRate() + END_LSTR);
                promMetric.append(helpName)
                          .append(FIFT_MIN_RATE_ATRR)
                          .append(timer.getFifteenMinuteRate() + END_LSTR);
                promMetric.append(
                        exportSnapshot(helpName, timer.getSnapshot()));
            }
        }

        MetricsUtil.writePrometheusFormat(promMetric, MetricManager.INSTANCE.getRegistry());

        return promMetric.toString();
    }

    private Map<String, Map<String, Object>> statistics() {
        Map<String, Map<String, Object>> metricsMap = new HashMap<>();
        ServerReporter reporter = ServerReporter.instance();
        for (Map.Entry<String, Histogram> entry : reporter.histograms().entrySet()) {
            // entryKey = path/method/responseTimeHistogram
            String entryKey = entry.getKey();
            String[] split = entryKey.split("/");
            String lastWord = split[split.length - 1];
            if (!lastWord.equals(METRICS_PATH_RESPONSE_TIME_HISTOGRAM)) {
                // original metrics dont report
                continue;
            }
            // metricsName = path/method
            String metricsName =
                    entryKey.substring(0, entryKey.length() - lastWord.length() - 1);

            Counter totalCounter = reporter.counters().get(
                    joinWithSlash(metricsName, METRICS_PATH_TOTAL_COUNTER));
            Counter failedCounter = reporter.counters().get(
                    joinWithSlash(metricsName, METRICS_PATH_FAILED_COUNTER));
            Counter successCounter = reporter.counters().get(
                    joinWithSlash(metricsName, METRICS_PATH_SUCCESS_COUNTER));

            Histogram histogram = entry.getValue();
            Map<String, Object> entryMetricsMap = new HashMap<>();
            entryMetricsMap.put(MetricsKeys.MAX_RESPONSE_TIME.name(),
                                histogram.getSnapshot().getMax());
            entryMetricsMap.put(MetricsKeys.MEAN_RESPONSE_TIME.name(),
                                histogram.getSnapshot().getMean());

            entryMetricsMap.put(MetricsKeys.TOTAL_REQUEST.name(),
                                totalCounter.getCount());

            if (failedCounter == null) {
                entryMetricsMap.put(MetricsKeys.FAILED_REQUEST.name(), 0);
            } else {
                entryMetricsMap.put(MetricsKeys.FAILED_REQUEST.name(),
                                    failedCounter.getCount());
            }

            if (successCounter == null) {
                entryMetricsMap.put(MetricsKeys.SUCCESS_REQUEST.name(), 0);
            } else {
                entryMetricsMap.put(MetricsKeys.SUCCESS_REQUEST.name(),
                                    successCounter.getCount());
            }

            metricsMap.put(metricsName, entryMetricsMap);

        }
        return metricsMap;
    }

    private String statisticsProm(Map<String, Map<String, Object>> metricMap) {
        StringBuilder promMetric = new StringBuilder();

        // build version info
        promMetric.append(STR_HELP)
                  .append(PROM_HELP_NAME).append(END_LSTR);
        promMetric.append(STR_TYPE)
                  .append(PROM_HELP_NAME)
                  .append(SPACE_STR + UNTYPED + END_LSTR);
        promMetric.append(PROM_HELP_NAME)
                  .append(VERSION_STR)
                  .append(ApiVersion.VERSION.toString()).append("\",}")
                  .append(SPACE_STR + "1.0" + END_LSTR);

        for (String methodKey : metricMap.keySet()) {
            String metricName = replaceSlashInKey(methodKey);
            promMetric.append(STR_HELP)
                      .append(metricName).append(END_LSTR);
            promMetric.append(STR_TYPE)
                      .append(metricName).append(SPACE_STR + GAUGE_TYPE + END_LSTR);
            Map<String, Object> itemMetricMap = metricMap.get(methodKey);
            for (String labelName : itemMetricMap.keySet()) {
                promMetric.append(metricName).append(LEFT_NAME_STR).append(labelName)
                          .append(RIGHT_NAME_STR).append(itemMetricMap.get(labelName))
                          .append(END_LSTR);
            }
        }
        return promMetric.toString();
    }

    private String joinWithSlash(String path1, String path2) {
        return String.join("/", path1, path2);
    }
}
