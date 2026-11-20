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
import java.util.List;

import org.apache.hugegraph.store.node.util.HgRegexUtil;
import org.junit.jupiter.api.Test;

/**
 * 2022/3/7
 */
public class JraftMetricsTest {

    @Test
    public void testRefineLabel() {
        String regex = "(replicator)(.+?:\\d+)(.*)";

        String[] sources = {
                "replicator_10.14.139.10:8081_append_entries_times",
                "replicator10.14.139.10:8081appendentriestimes",
                "replicator.10.14.139.10:8081.append.entries.times",
                "replicator-10-14-139-10:8081-append-entries-times",
                "replicator_hg_0_10_14_139_10:8081::100_replicate_inflights_count_min",
                "replicasdf14-13dasfies-times",
                };

        Arrays.stream(sources).forEach(e -> {
            System.out.println("--- " + e + " ---");
            List<String> list = HgRegexUtil.toGroupValues(regex, e);
            if (list != null) {
                list.forEach(System.out::println);
            } else {
                System.out.println("NONE");
            }
            System.out.println();
        });

    }

}
