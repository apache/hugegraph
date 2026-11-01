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

package org.apache.hugegraph.unit.util;

import static org.apache.hugegraph.metrics.MetricsUtil.sanitizePrometheusMetricName;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class MetricsUtilTest {

    private static final Pattern VALID_NAME =
            Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");

    @Test
    public void testSanitizeReplacesInvalidChars() {
        Assert.assertEquals("a_b_c_d_e_f",
                            sanitizePrometheusMetricName("a.b-c/d$e~f"));
        Assert.assertEquals("org_apache_Cache_schema_DEFAULT_hugegraph_hits",
                            sanitizePrometheusMetricName(
                                    "org.apache.Cache.schema~DEFAULT-hugegraph.hits"));
        Assert.assertEquals("_graphs_1_x",
                            sanitizePrometheusMetricName("/graphs/1~x"));
    }

    @Test
    public void testSanitizeKeepsValidName() {
        Assert.assertEquals("valid_name:total",
                            sanitizePrometheusMetricName("valid_name:total"));
        Assert.assertEquals("Mixed_Case_09",
                            sanitizePrometheusMetricName("Mixed_Case_09"));
    }

    @Test
    public void testSanitizePrefixesLeadingDigit() {
        Assert.assertEquals("_9cache_hits",
                            sanitizePrometheusMetricName("9cache.hits"));
    }

    @Test
    public void testSanitizeEmptyName() {
        Assert.assertEquals("_", sanitizePrometheusMetricName(""));
    }

    @Test
    public void testWritePrometheusFormatWithTildeInGaugeName() {
        MetricRegistry registry = new MetricRegistry();
        String key = "org.apache.hugegraph.Cache.schema~DEFAULT-hugegraph.hits";
        registry.register(key, (Gauge<Long>) () -> 42L);

        StringBuilder output = new StringBuilder();
        MetricsUtil.writePrometheusFormat(output, registry);
        String text = output.toString();

        String name = "org_apache_hugegraph_Cache_schema_DEFAULT_hugegraph_hits";
        Assert.assertFalse(text, text.contains("~"));
        Assert.assertEquals("# HELP " + name + "\n" +
                            "# TYPE " + name + " gauge\n" +
                            name + " 42\n", text);
    }

    @Test
    public void testWritePrometheusFormatProducesValidNames() {
        MetricRegistry registry = new MetricRegistry();
        registry.register("cache~a.size", (Gauge<Integer>) () -> 1);
        registry.histogram("api/graphs/1~x");
        registry.meter("9meter-rate");
        registry.timer("timer$latency~p");

        StringBuilder output = new StringBuilder();
        MetricsUtil.writePrometheusFormat(output, registry);

        for (String line : output.toString().split("\n")) {
            String name;
            if (line.startsWith("# HELP ") || line.startsWith("# TYPE ")) {
                name = line.substring(7).split(" ")[0];
            } else {
                name = line.split("[{ ]")[0];
            }
            Assert.assertTrue(line, VALID_NAME.matcher(name).matches());
        }
    }

    @Test
    public void testWriteStatisticsPrometheusFormatQuotesLabelValues() {
        Map<String, Object> items = new LinkedHashMap<>();
        items.put("MAX_RESPONSE_TIME", 12L);
        items.put("MEAN_RESPONSE_TIME", 3.5);
        Map<String, Map<String, Object>> metricMap = new LinkedHashMap<>();
        metricMap.put("graphs/hugegraph/graph/vertices/GET", items);

        StringBuilder output = new StringBuilder();
        MetricsUtil.writeStatisticsPrometheusFormat(output, metricMap);

        String name = "graphs_hugegraph_graph_vertices_GET";
        Assert.assertEquals("# HELP " + name + "\n" +
                            "# TYPE " + name + " gauge\n" +
                            name + "{name=\"MAX_RESPONSE_TIME\",} 12\n" +
                            name + "{name=\"MEAN_RESPONSE_TIME\",} 3.5\n",
                            output.toString());
    }
}
