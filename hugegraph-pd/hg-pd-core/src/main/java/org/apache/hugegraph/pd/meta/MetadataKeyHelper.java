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

package org.apache.hugegraph.pd.meta;

import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.grpc.Metapb;

public class MetadataKeyHelper {

    public static final char DELIMITER = '/';

    private static final String STORE = "STORE";
    private static final String ACTIVESTORE = "ACTIVESTORE";
    private static final String STORESTATUS = "STORESTATUS";
    private static final String PARTITION = "PARTITION";
    private static final String PARTITION_V36 = "PARTITION_V36";
    private static final String SHARDGROUP = "SHARDGROUP";

    private static final String PARTITION_STATUS = "PARTITION_STATUS";
    private static final String GRAPH = "GRAPH";
    private static final String GRAPHMETA = "GRAPHMETA";
    private static final String GRAPH_SPACE = "GRAPH_SPACE";
    private static final String PD_CONFIG = "PD_CONFIG";
    private static final String TASK_SPLIT = "TASK_SPLIT";
    private static final String TASK_MOVE = "TASK_MOVE";
    private static final String TASK_BUILD_INDEX = "TASK_BI";
    private static final String LOG_RECORD = "LOG_RECORD";

    private static final String QUEUE = "QUEUE";

    public static byte[] getStoreInfoKey(final long storeId) {
        //STORE/{storeId}
        String key = StringBuilderHelper.get()
                                        .append(STORE).append(DELIMITER)
                                        .append(storeId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getActiveStoreKey(final long storeId) {
        //ACTIVESTORE/{storeId}
        String key = StringBuilderHelper.get()
                                        .append(ACTIVESTORE).append(DELIMITER)
                                        .append(storeId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getActiveStorePrefix() {
        //ACTIVESTORE
        String key = StringBuilderHelper.get()
                                        .append(ACTIVESTORE).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getStorePrefix() {
        //STORE
        String key = StringBuilderHelper.get()
                                        .append(STORE).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getStoreStatusKey(final long storeId) {
        //STORESTATUS/{storeId}
        String key = StringBuilderHelper.get()
                                        .append(STORESTATUS).append(DELIMITER)
                                        .append(storeId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getShardGroupKey(final long groupId) {
        //SHARDGROUP/{storeId}
        String key = StringBuilderHelper.get()
                                        .append(SHARDGROUP).append(DELIMITER)
                                        .append(groupId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getShardGroupPrefix() {
        //SHARDGROUP
        String key = StringBuilderHelper.get()
                                        .append(SHARDGROUP).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getPartitionKey(final String graphName, final int partId) {
        //GRAPH/{graphName}/Partition/{partId}
        String key = StringBuilderHelper.get()
                                        .append(GRAPH).append(DELIMITER)
                                        .append(graphName).append(DELIMITER)
                                        .append(PARTITION).append(DELIMITER)
                                        .append(partId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getPartitionV36Key(final String graphName, final int partId) {
        // GRAPH/{graphName}/PartitionV36/{partId}
        String key = StringBuilderHelper.get()
                                        .append(GRAPH).append(DELIMITER)
                                        .append(graphName).append(DELIMITER)
                                        .append(PARTITION_V36).append(DELIMITER)
                                        .append(partId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getPartitionPrefix(final String graphName) {
        //GRAPH/{graph}/Partition
        String key = StringBuilderHelper.get()
                                        .append(GRAPH).append(DELIMITER)
                                        .append(graphName).append(DELIMITER)
                                        .append(PARTITION).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getShardKey(final long storeId, final int partId) {
        //SHARD/{graphName}/{type}
        String key = StringBuilderHelper.get()
                                        .append(SHARDGROUP).append(DELIMITER)
                                        .append(storeId).append(DELIMITER)
                                        .append(partId)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getShardPrefix(final long storeId) {
        //SHARD/{graphName}/{type}
        String key = StringBuilderHelper.get()
                                        .append(SHARDGROUP).append(DELIMITER)
                                        .append(storeId).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getGraphKey(final String graphName) {
        //GRAPHMETA/{graphName}
        String key = StringBuilderHelper.get()
                                        .append(GRAPHMETA).append(DELIMITER)
                                        .append(graphName).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getGraphPrefix() {
        //GRAPHMETA/{
        String key = StringBuilderHelper.get()
                                        .append(GRAPHMETA).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getPartitionStatusKey(String graphName, int id) {
        //PARTITION_STATUS/{
        String key = StringBuilderHelper.get()
                                        .append(PARTITION_STATUS)
                                        .append(DELIMITER)
                                        // .append(graphName).append(DELIMITER)
                                        .append(id).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getPartitionStatusPrefixKey(String graphName) {
        //PARTITION_STATUS/{
        StringBuilder builder = StringBuilderHelper.get().append(PARTITION_STATUS)
                                                   .append(DELIMITER);
        if (!StringUtils.isEmpty(graphName)) {
            builder.append(graphName).append(DELIMITER);
        }
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getGraphSpaceKey(String graphSpace) {
        //GRAPH_SPACE/{
        StringBuilder builder = StringBuilderHelper.get().append(
                GRAPH_SPACE).append(DELIMITER);
        if (!StringUtils.isEmpty(graphSpace)) {
            builder.append(graphSpace).append(DELIMITER);
        }
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getPdConfigKey(String configKey) {
        //PD_CONFIG/{
        StringBuilder builder = StringBuilderHelper.get().append(
                PD_CONFIG).append(DELIMITER);
        if (!StringUtils.isEmpty(configKey)) {
            builder.append(configKey).append(DELIMITER);
        }
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getQueueItemPrefix() {
        //QUEUE
        String key = StringBuilderHelper.get()
                                        .append(QUEUE).append(DELIMITER)
                                        .toString();
        return key.getBytes(Charset.defaultCharset());
    }

    public static byte[] getQueueItemKey(String itemId) {
        //QUEUE
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(QUEUE).append(DELIMITER);
        if (!StringUtils.isEmpty(itemId)) {
            builder.append(itemId).append(DELIMITER);
        }
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getSplitTaskKey(String graphName, int groupId) {
        // TASK_SPLIT/{GraphName}/{partitionID}
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_SPLIT).append(DELIMITER)
                                                   .append(graphName).append(DELIMITER)
                                                   .append(groupId);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getSplitTaskPrefix(String graphName) {
        // TASK_SPLIT/{GraphName}/
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_SPLIT).append(DELIMITER)
                                                   .append(graphName);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getAllSplitTaskPrefix() {
        // TASK_SPLIT/{GraphName}/
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_SPLIT).append(DELIMITER);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getMoveTaskKey(String graphName, int targetGroupId, int groupId) {
        // TASK_MOVE/{GraphName}/to PartitionID/{source partitionID}
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_MOVE).append(DELIMITER)
                                                   .append(graphName).append(DELIMITER)
                                                   .append(targetGroupId).append(DELIMITER)
                                                   .append(groupId);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getMoveTaskPrefix(String graphName) {
        // TASK_MOVE/{graphName}/toPartitionId/
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_MOVE).append(DELIMITER)
                                                   .append(graphName);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getBuildIndexTaskKey(long taskId, int partitionId) {
        // TASK_BI/ task id / partition id
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_BUILD_INDEX).append(DELIMITER)
                                                   .append(taskId).append(DELIMITER)
                                                   .append(partitionId);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getBuildIndexTaskPrefix(long taskId) {
        // TASK_MOVE/{GraphName}/to PartitionID/{source partitionID}
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_BUILD_INDEX).append(DELIMITER)
                                                   .append(taskId);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getAllMoveTaskPrefix(){
        // TASK_MOVE/{graphName}/toPartitionId/
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(TASK_MOVE).append(DELIMITER);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getLogKey(Metapb.LogRecord record) {
        //LOG_RECORD/{action}/{time}/
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(LOG_RECORD)
                                                   .append(DELIMITER)
                                                   .append(record.getAction())
                                                   .append(DELIMITER)
                                                   .append(record.getTimestamp());
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getLogKeyPrefix(String action, long time) {
        //LOG_DATA_SPLIT/{time}/{GraphName}
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(LOG_RECORD)
                                                   .append(DELIMITER)
                                                   .append(action)
                                                   .append(DELIMITER)
                                                   .append(time);
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getKVPrefix(String prefix, String key) {
        //K@/{key}
        StringBuilder builder = StringBuilderHelper.get()
                                                   .append(prefix).append(DELIMITER);
        if (!StringUtils.isEmpty(key)) {
            builder.append(key).append(DELIMITER);
        }
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static byte[] getKVTTLPrefix(String ttlPrefix, String prefix, String key) {
        StringBuilder builder = StringBuilderHelper.get().append(ttlPrefix)
                                                   .append(prefix).append(DELIMITER);
        if (!StringUtils.isEmpty(key)) {
            builder.append(key).append(DELIMITER);
        }
        return builder.toString().getBytes(Charset.defaultCharset());
    }

    public static String getKVWatchKeyPrefix(String key, String watchDelimiter, long clientId) {
        StringBuilder builder = StringBuilderHelper.get();
        builder.append(watchDelimiter).append(DELIMITER);
        builder.append(key == null ? "" : key).append(DELIMITER);
        builder.append(clientId);
        return builder.toString();
    }

    public static String getKVWatchKeyPrefix(String key, String watchDelimiter) {
        StringBuilder builder = StringBuilderHelper.get();
        builder.append(watchDelimiter).append(DELIMITER);
        builder.append(key == null ? "" : key).append(DELIMITER);
        return builder.toString();
    }

    public static char getDelimiter() {
        return DELIMITER;
    }

    public static StringBuilder getStringBuilderHelper() {
        return StringBuilderHelper.get();
    }

    static class StringBuilderHelper {

        private static final int DISCARD_LIMIT = 1024 << 3;     // 8k

        private static final ThreadLocal<StringBuilderHolder> holderThreadLocal = ThreadLocal
                .withInitial(StringBuilderHolder::new);

        public static StringBuilder get() {
            final StringBuilderHolder holder = holderThreadLocal.get();
            return holder.getStringBuilder();
        }

        public static void truncate() {
            final StringBuilderHolder holder = holderThreadLocal.get();
            holder.truncate();
        }

        private static class StringBuilderHolder {

            private final StringBuilder buf = new StringBuilder();

            private StringBuilder getStringBuilder() {
                truncate();
                return buf;
            }

            private void truncate() {
                buf.setLength(0);
            }
        }
    }

}
