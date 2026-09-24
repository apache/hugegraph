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

package org.apache.hugegraph.store.metric;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.HgStoreEngine;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import com.sun.management.OperatingSystemMXBean;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetricService {

    private static final long MIB = 1024 * 1024;
    private final Deque<Map<String, List<Long>>> deque = new LinkedList<>();
    HgStoreEngine storeEngine;

    public void setStoreEngine(HgStoreEngine hgStoreEngine) {
        this.storeEngine = hgStoreEngine;
    }

    public HgStoreEngine getStorageEngine() {
        return this.storeEngine;
    }

    public Map<String, Long> getSystemMetrics() {
        Map<String, Long> systemMetrics = new HashMap<>();
        try {
            // cpu
            loadCpuInfo(systemMetrics);

            // memory
            loadMemInfo(systemMetrics);

            // disk
            loadDiskInfo(systemMetrics);

            // disk io
            //loadDiskIo(systemMetrics);
            //
            // network
            //loadNetFlowInfo(systemMetrics);

            // rocksdb
            loadRocksDbInfo(systemMetrics);
        } catch (Exception e) {
            log.error("get system metric failed, {}", e.toString());
        }

        return systemMetrics;
    }

    private void loadCpuInfo(Map<String, Long> map) {
        OperatingSystemMXBean osBean =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        Double cpuLoad = osBean.getSystemLoadAverage();
        map.put("cpu.load", cpuLoad.longValue());
    }

    private void loadMemInfo(Map<String, Long> map) {
        OperatingSystemMXBean osBean =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        map.put("mem.physical_total", osBean.getTotalPhysicalMemorySize());
        map.put("mem.physical_free", osBean.getFreePhysicalMemorySize());
        map.put("mem.swap_total", osBean.getTotalSwapSpaceSize());
        map.put("mem.swap_free", osBean.getFreeSwapSpaceSize());

        Runtime runtime = Runtime.getRuntime();
        map.put("mem.heap_total", runtime.totalMemory());
        map.put("mem.heap_used", runtime.totalMemory() - runtime.freeMemory());

        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        map.put("mem.non_heap_total", memoryUsage.getCommitted());
        map.put("mem.non_heap_used", memoryUsage.getUsed());
    }

    private void loadDiskInfo(Map<String, Long> map) {
        // sum up all disk space
        File[] rootDrive = File.listRoots();
        long total = 0;
        long free = 0;
        long usable = 0;
        if (rootDrive != null) {
            for (File d : rootDrive) {
                total += d.getTotalSpace();
                free += d.getFreeSpace();
                usable += d.getUsableSpace();
            }
        }
        map.put("disk.total_size", total / MIB);
        map.put("disk.free_size", free / MIB);
        map.put("disk.usable_size", usable / MIB);
    }

    //private void loadDiskIo(Map<String, Long> map) {
    //    for (Map.Entry<String, Float> entry : getDiskIoData().entrySet()) {
    //        map.put(entry.getKey(), entry.getValue().longValue());
    //    }
    //}
    //
    //private void loadNetFlowInfo(Map<String, Long> map) {
    //    for (Map.Entry<String, List<Long>> entry : getTraffic().entrySet()) {
    //        // exclude none-functional network interface
    //        map.put("network." + entry.getKey() + ".sent_bytes", entry.getValue().get(0) / 1024
    //        / 1024);
    //        map.put("network." + entry.getKey() + ".recv_bytes", entry.getValue().get(1) / 1024
    //        / 1024);
    //        map.put("network." + entry.getKey() + ".sent_rates", entry.getValue().get(2) / 1024
    //        / 1024);
    //        map.put("network." + entry.getKey() + ".recv_rates", entry.getValue().get(3) / 1024
    //        / 1024);
    //    }
    //}

    private void loadRocksDbInfo(Map<String, Long> map) {
        Map<MemoryUsageType, Long> dbMem =
                storeEngine.getBusinessHandler().getApproximateMemoryUsageByType(null);
        map.put("rocksdb.table.reader.total", dbMem.get(MemoryUsageType.kTableReadersTotal));
        map.put("rocksdb.mem.table.total", dbMem.get(MemoryUsageType.kMemTableTotal));
        map.put("rocksdb.cache.total", dbMem.get(MemoryUsageType.kCacheTotal));
        map.put("rocksdb.mem.table.un_flushed", dbMem.get(MemoryUsageType.kMemTableUnFlushed));

        RocksDBFactory dbFactory = RocksDBFactory.getInstance();
        Set<String> names = dbFactory.getGraphNames();
        if (names != null) {
            for (String name : names) {
                try {
                    RocksDBSession session = dbFactory.queryGraphDB(name);
                    Statistics statistics = session.getRocksDbStats();
                    map.put(
                            "rocksdb.graph." + name + "." +
                            TickerType.NUMBER_KEYS_WRITTEN.name().toLowerCase(),
                            statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN));
                    map.put(
                            "rocksdb.graph." + name + "." +
                            TickerType.NUMBER_KEYS_READ.name().toLowerCase(),
                            statistics.getTickerCount(TickerType.NUMBER_KEYS_READ));
                    map.put(
                            "rocksdb.graph." + name + "." +
                            TickerType.NUMBER_KEYS_UPDATED.name().toLowerCase(),
                            statistics.getTickerCount(TickerType.NUMBER_KEYS_UPDATED));
                    map.put(
                            "rocksdb.graph." + name + "." +
                            TickerType.BYTES_WRITTEN.name().toLowerCase(),
                            statistics.getTickerCount(TickerType.BYTES_WRITTEN));
                    map.put(
                            "rocksdb.graph." + name + "." +
                            TickerType.BYTES_READ.name().toLowerCase(),
                            statistics.getTickerCount(TickerType.BYTES_READ));
                } catch (Exception e) {

                }
            }
        }
    }

    /**
     * get all network interface traffic(delta from last invoke).
     * -sent bytes
     * -receive bytes
     * -in rates
     * -out rates
     *
     * @return
     */
    //private Map<String, List<Long>> getTraffic() {
    //    deque.add(loadTrafficData());
    //
    //    if (deque.size() < 2) {
    //        return new HashMap<>();
    //    }
    //    // keep 2 copies
    //    while (deque.size() > 2) {
    //        deque.removeFirst();
    //    }
    //
    //    // compare
    //    Map<String, List<Long>> result = new HashMap<>();
    //    Map<String, List<Long>> currentFlows = deque.getLast();
    //    Map<String, List<Long>> preFlows = deque.getFirst();
    //
    //    for (Map.Entry<String, List<Long>> entry : currentFlows.entrySet()) {
    //        if (preFlows.containsKey(entry.getKey())) {
    //            List<Long> prev = preFlows.get(entry.getKey());
    //            List<Long> now = preFlows.get(entry.getKey());
    //            // no traffic
    //            if (now.get(0) == 0) {
    //                continue;
    //            }
    //            long diff = now.get(2) - prev.get(2);
    //            diff = diff > 0 ? diff : 1L;
    //            result.put(
    //                entry.getKey(),
    //                Arrays.asList(
    //                    now.get(0) - prev.get(0),
    //                    now.get(1) - prev.get(1),
    //                    // rate rate
    //                    (now.get(0) - prev.get(0)) / diff,
    //                    // recv rate
    //                    (now.get(1) - prev.get(1)) / diff));
    //        }
    //    }
    //    return result;
    //}

    /**
     * load traffic according to os, now only support mac os and linux
     *
     * @return
     */
    //private Map<String, List<Long>> loadTrafficData() {
    //    String osName = System.getProperty("os.name").toLowerCase();
    //    if (osName.startsWith("linux")) {
    //        return loadLinuxTrafficData();
    //    } else if (osName.startsWith("mac")) {
    //        return loadMacOsTrafficData();
    //    }
    //    return new HashMap<>();
    //}

    /**
     * read the result of "netstat -ib". (lo is ignored)
     *
     * @return
     */
    //private Map<String, List<Long>> loadMacOsTrafficData() {
    //    Map<String, List<Long>> flows = new HashMap<>();
    //    Long current = System.currentTimeMillis() / 1000;
    //    for (String line : executeCmd("netstat -ib")) {
    //        if (line.startsWith("Name") || line.startsWith("lo")) {
    //            // first table header line
    //            continue;
    //        }
    //
    //        List<String> arr = Arrays.stream(line.split(" ")).filter(x -> x.length() > 0)
    //        .collect(Collectors.toList());
    //
    //        long sentBytes = Long.parseLong(arr.get(arr.size() - 2));
    //        long recvBytes = Long.parseLong(arr.get(arr.size() - 5));
    //        String name = arr.get(0);
    //        // log.debug("mac: {}, -> {},{},{}", line, sentBytes, recvBytes, name);
    //        if (sentBytes > 0 && recvBytes > 0) {
    //            flows.put(name, Arrays.asList(sentBytes, recvBytes, current));
    //        }
    //    }
    //
    //    return flows;
    //}

    /**
     * read the statistics file for network interface
     * cat /sys/class/net/NETWORK_INTERFACE_NAME/statistics/tx_bytes
     * cat /sys/class/net/NETWORK_INTERFACE_NAME/statistics/rx_bytes
     *
     * @return
     */
    private Map<String, List<Long>> loadLinuxTrafficData() {
        Long current = System.currentTimeMillis() / 1000;
        Map<String, List<Long>> flows = new HashMap<>();
        try {
            for (String name : getAllNetworkInterfaces()) {
                long sentBytes = getUnsignedLongFromFile(
                        String.format("/sys/class/net/%s/statistics/tx_bytes", name));
                long recvBytes = getUnsignedLongFromFile(
                        String.format("/sys/class/net/%s/statistics/rx_bytes", name));
                flows.put(name, Arrays.asList(sentBytes, recvBytes, current));
            }
        } catch (Exception e) {

        }
        return flows;
    }

    /**
     * read file and parse to long
     *
     * @param filename
     * @return
     */
    private long getUnsignedLongFromFile(String filename) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
        if (!lines.isEmpty()) {
            return Long.parseLong(lines.get(0));
        }
        return 0L;
    }

    /**
     * get all network interface names. (lo is ignored)
     *
     * @return
     * @throws SocketException
     */
    private List<String> getAllNetworkInterfaces() throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        List<String> names = new ArrayList<>();
        while (interfaces.hasMoreElements()) {
            String name = interfaces.nextElement().getName();
            if (!"lo".equals(name)) {
                names.add(name);
            }
        }
        return names;
    }

    //private Map<String, Float> getDiskIoData() {
    //    String osName = System.getProperty("os.name").toLowerCase();
    //    if (osName.startsWith("linux")) {
    //        return loadLinuxDiskIoData();
    //    } else if (osName.startsWith("mac")) {
    //        return loadMacDiskIoData();
    //    }
    //    return new HashMap<>();
    //}

    /**
     * get io data using iostat -d -x -k
     *
     * @return
     */
    //private Map<String, Float> loadLinuxDiskIoData() {
    //    Map<String, Float> result = new HashMap<>();
    //    boolean contentFlag = false;
    //    for (String line : executeCmd("iostat -d -x -k")) {
    //        // header
    //        if (line.startsWith("Device")) {
    //            contentFlag = true;
    //            continue;
    //        }
    //
    //        if (contentFlag) {
    //            List<String> arr =
    //                Arrays.stream(line.split(" ")).filter(x -> x.length() > 0).collect
    //                (Collectors.toList());
    //            try {
    //                // util%
    //                result.put("disk.io." + arr.get(0) + ".util", Float.valueOf(arr.get(arr
    //                .size() - 1)) * 100);
    //                // wait
    //                result.put("disk.io." + arr.get(0) + ".wait", Float.valueOf(arr.get(arr
    //                .size() - 5)) * 100);
    //            } catch (Exception e) {
    //                log.debug("error get disk io data {}", line);
    //            }
    //        }
    //    }
    //    return result;
    //}

    /**
     * get io data using iostat
     *
     * @return
     */
    //private Map<String, Float> loadMacDiskIoData() {
    //
    //    Map<String, Float> result = new HashMap<>();
    //    List<String> lines = executeCmd("iostat -oK");
    //    // disks
    //    List<String> disks =
    //        Arrays.stream(lines.get(0).split(" "))
    //            .filter(x -> x.length() > 0 && x.startsWith("disk"))
    //            .collect(Collectors.toList());
    //    // datas
    //    List<String> data =
    //        Arrays.stream(lines.get(2).split(" ")).filter(x -> x.length() > 0).collect
    //        (Collectors.toList());
    //    // zip data
    //    for (int i = 0; i < disks.size(); i++) {
    //        try {
    //            // msps
    //            result.put("disk.io." + disks.get(i) + ".wait", Float.valueOf(data.get(i * 3 +
    //            2)) * 100);
    //            // no such value
    //            result.put("disk.io." + disks.get(i) + ".util", 0.0F);
    //        } catch (Exception e) {
    //            log.debug("error get io data {}", data.get(i));
    //        }
    //    }
    //    return result;
    //}

    /**
     * execute cmd and get the output
     *
     * @param cmd
     * @return
     */
    //private List<String> executeCmd(String cmd) {
    //    List<String> result = new ArrayList<>();
    //    try {
    //        Process pr = Runtime.getRuntime().exec(cmd);
    //        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
    //        String line;
    //        while ((line = in.readLine()) != null) {
    //            if (line.length() > 0) {
    //                result.add(line);
    //            }
    //        }
    //        pr.waitFor();
    //        in.close();
    //    } catch (IOException | InterruptedException e) {
    //    }
    //    return result;
    //}
}
