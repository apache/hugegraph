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

package org.apache.hugegraph.pd;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class StoreMonitorDataService {

    private static final String MONITOR_DATA_PREFIX = "SMD";
    private PDConfig pdConfig;
    private KvService kvService;
    /**
     * the last timestamp of the store monitor data,
     * used for determine the gap of store's heartbeat.
     */
    private Map<Long, Long> lastStoreStateTimestamp;

    public StoreMonitorDataService(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
        this.kvService = new KvService(pdConfig);
        this.lastStoreStateTimestamp = new HashMap<>();
    }

    /**
     * save the store stats
     *
     * @param storeStats
     */
    public void saveMonitorData(Metapb.StoreStats storeStats) throws PDException {
        long storeId = storeStats.getStoreId();
        /**
         * load the latest store timestamp when start up or alter leader
         */
        if (!lastStoreStateTimestamp.containsKey(storeId)) {
            long lastTimestamp = getLatestStoreMonitorDataTimeStamp(storeId);
            log.debug("store id : {}, last timestamp :{}", storeId, lastTimestamp);
            lastStoreStateTimestamp.put(storeId, lastTimestamp);
        }

        long current = System.currentTimeMillis() / 1000;
        long interval = this.pdConfig.getStore().getMonitorInterval();

        // exceed the interval
        if (current - lastStoreStateTimestamp.getOrDefault(storeId, 0L) >= interval) {
            saveMonitorDataToDb(storeStats, current);
            log.debug("store id: {}, system info:{}", storeId,
                      debugMonitorInfo(storeStats.getSystemMetricsList()));
            lastStoreStateTimestamp.put(storeId, current);
        }
    }

    /**
     * save the snapshot of store status
     *
     * @param storeStats store status
     * @param ts,        timestamp
     * @return store status
     * @throws PDException
     */
    private void saveMonitorDataToDb(Metapb.StoreStats storeStats, long ts) throws PDException {
        String key = getMonitorDataKey(storeStats.getStoreId(), ts);
        log.debug("store id: {}, save monitor data info, ts:{}, my key:{}", storeStats.getStoreId(),
                  ts, key);
        kvService.put(key, extractMetricsFromStoreStatus(storeStats));
    }

    public String debugMonitorInfo(List<Metapb.RecordPair> systemInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Metapb.RecordPair pair : systemInfo) {
            sb.append(pair.getKey());
            sb.append(":");
            sb.append(pair.getValue());
            sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * get the historical monitor data by store id, by range(start, end)
     *
     * @param storeId store id
     * @param start   range start
     * @param end     range end
     * @return list of store stats
     */
    public Map<String, String> getStoreMonitorData(long storeId, long start, long end) throws
                                                                                       PDException {
        log.debug("get monitor data, store id:{}, start{}, end:{}",
                  storeId,
                  getMonitorDataKey(storeId, start),
                  getMonitorDataKey(storeId, end));
        return kvService.scanRange(getMonitorDataKey(storeId, start),
                                   getMonitorDataKey(storeId, end));
    }

    /**
     * for api service
     *
     * @param storeId
     * @return
     * @throws PDException
     */
    public List<Map<String, Long>> getStoreMonitorData(long storeId) throws PDException {
        List<Map<String, Long>> result = new LinkedList<>();
        long current = System.currentTimeMillis() / 1000;
        long start = current - this.pdConfig.getStore().getRetentionPeriod();

        try {
            for (Map.Entry<String, String> entry : getStoreMonitorData(storeId, start,
                                                                       current).entrySet()) {
                String[] arr =
                        entry.getKey().split(String.valueOf(MetadataKeyHelper.getDelimiter()));
                Map<String, Long> map = new HashMap();
                long timestamp = Long.parseLong(arr[arr.length - 1]);
                map.put("ts", timestamp);
                for (String pair : entry.getValue().split(",")) {
                    String[] p = pair.split(":");
                    if (p.length == 2) {
                        map.put(p[0], Long.parseLong(p[1]));
                    }
                }
                result.add(map);
            }
            result.sort((o1, o2) -> o1.get("ts").compareTo(o2.get("ts")));
        } catch (PDException e) {
            log.error(e.getMessage());
        }
        return result;
    }

    /**
     * for api service, export txt
     *
     * @param storeId
     * @return
     * @throws PDException
     */
    public String getStoreMonitorDataText(long storeId) throws PDException {

        List<Map<String, Long>> result = getStoreMonitorData(storeId);
        StringBuilder sb = new StringBuilder();
        if (result.size() > 0) {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            Map<String, Long> lastRow = result.get(result.size() - 1);
            List<String> columns = new ArrayList<>();
            // construct columns, ts + sorted keys
            columns.add("ts");
            columns.addAll(lastRow.keySet().stream()
                                  .filter(x -> !"ts".equals(x))
                                  .sorted()
                                  .collect(Collectors.toList()));
            sb.append(String.join(",", columns).replace("\"", "")).append("\r\n");
            for (Map<String, Long> row : result) {
                for (String key : columns) {
                    // ts + , + ...
                    if ("ts".equals(key)) {
                        // format ts
                        sb.append(dtf.format(
                                LocalDateTime.ofInstant(Instant.ofEpochSecond(row.get(key)),
                                                        ZoneId.systemDefault())));
                        continue;
                    } else {
                        sb.append(",").append(row.getOrDefault(key, 0L));
                    }
                }
                sb.append("\r\n");
            }
        }
        return sb.toString();
    }

    /**
     * remove the monitor data of the store that before till(not include)
     *
     * @param storeId store id
     * @param till    expire time
     * @return affect rows
     */
    public int removeExpiredMonitorData(long storeId, long till) throws PDException {
        String keyStart = getMonitorDataKey(storeId, 1);
        String keyEnd = getMonitorDataKey(storeId, till);
        int records = 0;
        for (String key : kvService.scanRange(keyStart, keyEnd).keySet()) {
            kvService.delete(key);
            log.debug("remove monitor data, key: {}", key);
            records += 1;
        }
        return records;
    }

    /**
     * get the latest timestamp of the store monitor data
     *
     * @param storeId
     * @return timestamp(by seconds)
     */
    public long getLatestStoreMonitorDataTimeStamp(long storeId) {
        long maxId = 0L;
        long current = System.currentTimeMillis() / 1000;
        long start = current - this.pdConfig.getStore().getMonitorInterval();
        String keyStart = getMonitorDataKey(storeId, start);
        String keyEnd = getMonitorDataKey(storeId, current);
        try {
            for (String key : kvService.scanRange(keyStart, keyEnd).keySet()) {
                String[] arr = key.split(String.valueOf(MetadataKeyHelper.getDelimiter()));
                maxId = Math.max(maxId, Long.parseLong(arr[arr.length - 1]));
            }
        } catch (PDException e) {
        }
        return maxId;
    }

    private String getMonitorDataKey(long storeId, long ts) {
        StringBuilder builder = new StringBuilder();
        builder.append(MONITOR_DATA_PREFIX)
               .append(MetadataKeyHelper.getDelimiter())
               .append(storeId)
               .append(MetadataKeyHelper.getDelimiter())
               .append(String.format("%010d", ts));
        return builder.toString();
    }

    private String extractMetricsFromStoreStatus(Metapb.StoreStats storeStats) {
        List<String> list = new ArrayList<>();
        for (Metapb.RecordPair pair : storeStats.getSystemMetricsList()) {
            list.add("\"" + pair.getKey() + "\":" + pair.getValue());
        }
        return String.join(",", list);
    }
}
