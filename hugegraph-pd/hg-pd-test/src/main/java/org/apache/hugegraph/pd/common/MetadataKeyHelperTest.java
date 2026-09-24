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

package org.apache.hugegraph.pd.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.junit.Test;

public class MetadataKeyHelperTest {

    @Test
    public void testGetStoreInfoKey() {
        assertThat(MetadataKeyHelper.getStoreInfoKey(0L)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetActiveStoreKey() {
        assertThat(MetadataKeyHelper.getActiveStoreKey(0L)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetActiveStorePrefix() {
        assertThat(MetadataKeyHelper.getActiveStorePrefix()).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetStorePrefix() {
        assertThat(MetadataKeyHelper.getStorePrefix()).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetStoreStatusKey() {
        assertThat(MetadataKeyHelper.getStoreStatusKey(0L)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetShardGroupKey() {
        assertThat(MetadataKeyHelper.getShardGroupKey(0L)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetShardGroupPrefix() {
        assertThat(MetadataKeyHelper.getShardGroupPrefix()).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetPartitionKey() {
        assertThat(MetadataKeyHelper.getPartitionKey("graphName", 0)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetPartitionPrefix() {
        assertThat(MetadataKeyHelper.getPartitionPrefix("graphName")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetShardKey() {
        assertThat(MetadataKeyHelper.getShardKey(0L, 0)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetShardPrefix() {
        assertThat(MetadataKeyHelper.getShardPrefix(0L)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetGraphKey() {
        assertThat(MetadataKeyHelper.getGraphKey("graphName")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetGraphPrefix() {
        assertThat(MetadataKeyHelper.getGraphPrefix()).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetPartitionStatusKey() {
        assertThat(MetadataKeyHelper.getPartitionStatusKey("graphName",
                                                           0)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetPartitionStatusPrefixKey() {
        assertThat(MetadataKeyHelper.getPartitionStatusPrefixKey(
                "graphName")).contains(MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetGraphSpaceKey() {
        assertThat(MetadataKeyHelper.getGraphSpaceKey("graphSpace")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetPdConfigKey() {
        assertThat(MetadataKeyHelper.getPdConfigKey("configKey")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetQueueItemPrefix() {
        assertThat(MetadataKeyHelper.getQueueItemPrefix()).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetQueueItemKey() {
        assertThat(MetadataKeyHelper.getQueueItemKey("itemId")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetSpitTaskKey() {
        assertThat(MetadataKeyHelper.getSplitTaskKey("graphName", 0)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetSpitTaskPrefix() {
        assertThat(MetadataKeyHelper.getSplitTaskPrefix("graph0")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetLogKey() {
        // Setup
        final Metapb.LogRecord record = Metapb.LogRecord.newBuilder()
                                                        .setAction("value")
                                                        .setTimestamp(0L)
                                                        .build();

        // Run the test
        final byte[] result = MetadataKeyHelper.getLogKey(record);

        // Verify the results
        assertThat(result).contains(MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetLogKeyPrefix() {
        assertThat(MetadataKeyHelper.getLogKeyPrefix("action", 0L)).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetKVPrefix() {
        assertThat(MetadataKeyHelper.getKVPrefix("prefix", "key")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetKVTTLPrefix() {
        assertThat(MetadataKeyHelper.getKVTTLPrefix("ttlPrefix", "prefix",
                                                    "key")).contains(
                MetadataKeyHelper.getDelimiter());
    }

    @Test
    public void testGetKVWatchKeyPrefix1() {
        assertThat(
                MetadataKeyHelper.getKVWatchKeyPrefix("key", "watchDelimiter",
                                                      0L)).contains(
                String.valueOf(MetadataKeyHelper.getDelimiter()));
    }

    @Test
    public void testGetKVWatchKeyPrefix2() {
        assertThat(MetadataKeyHelper.getKVWatchKeyPrefix("key",
                                                         "watchDelimiter")).contains(
                String.valueOf(MetadataKeyHelper.getDelimiter()));
    }

    @Test
    public void testGetDelimiter() {
        assertThat(MetadataKeyHelper.getDelimiter()).isEqualTo('/');
    }

    @Test
    public void testGetStringBuilderHelper() {
        try {
            MetadataKeyHelper.getStringBuilderHelper();
        } catch (Exception e) {

        }
    }
}
