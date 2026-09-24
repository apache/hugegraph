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

package org.apache.hugegraph.store.meta;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.base.GlobalMetaStore;
import org.apache.hugegraph.store.options.MetadataOptions;
import org.apache.hugegraph.store.util.HgStoreException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StoreMetadata extends GlobalMetaStore {

    protected final static int Store_DataFmt_Version = 1;
    private final List<String> dataLocations;
    private final List<String> raftLocations;
    private Store store = null;

    public StoreMetadata(MetadataOptions options) {
        super(options);
        dataLocations = Arrays.asList(options.getDataPath().split(","));
        raftLocations = Arrays.asList(options.getRaftPath().split(","));
    }

    public List<String> getDataLocations() {
        return dataLocations;
    }

    public List<String> getRaftLocations() {
        return raftLocations;
    }

    public Store load() {
        // For the case of multi-directory storage, pre-create folders to facilitate pd-side file
        // storage statistics.
        dataLocations.forEach(path -> {
            String strPath = Paths.get(path).toAbsolutePath().toString();
            File dbFile = new File(strPath);
            if (!dbFile.exists()) {
                dbFile.mkdir();
            }
        });

        raftLocations.forEach(path -> {
            String strPath = Paths.get(path).toAbsolutePath().toString();
            File dbFile = new File(strPath);
            if (!dbFile.exists()) {
                dbFile.mkdir();
            }
        });

        byte[] key = MetadataKeyHelper.getStoreKey();
        byte[] value = get(key);
        if (value != null) {
            try {
                Metapb.Store protoObj = Metapb.Store.parseFrom(value);
                if (protoObj != null) {
                    store = new Store(protoObj);
                }
            } catch (Exception e) {
                throw new HgStoreException(HgStoreException.EC_FAIL, e);
            }
        }
        if (store == null) {
            store = new Store(Store_DataFmt_Version);
        }
        checkDataFmtCompatible();
        return store;
    }

    public Store getStore() {
        return store;
    }

    public void save(Store store) {
        byte[] key = MetadataKeyHelper.getStoreKey();
        put(key, store.getProtoObj().toByteArray());
        this.store = store;
    }

    public void checkDataFmtCompatible() {
        if (store == null || store.getDataVersion() != Store_DataFmt_Version) {
            throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                       String.format(
                                               "Incompatible data format, data format version is " +
                                               "%d, supported version is %d",
                                               store.getDataVersion(), Store_DataFmt_Version));
        }
    }

    public Metapb.PartitionStore getPartitionStore(int partitionId) {
        byte[] key = MetadataKeyHelper.getPartitionStoreKey(partitionId);
        return get(Metapb.PartitionStore.parser(), key);
    }

    /**
     * Delete the storage metadata corresponding to the specified partition.
     *
     * @param partitionId Partition ID.
     */
    public void removePartitionStore(int partitionId) {
        byte[] key = MetadataKeyHelper.getPartitionStoreKey(partitionId);
        delete(key);
    }

    public List<Metapb.PartitionStore> getPartitionStores() {
        byte[] key = MetadataKeyHelper.getPartitionStorePrefix();
        return scan(Metapb.PartitionStore.parser(), key);
    }

    public void savePartitionStore(Metapb.PartitionStore partitionStore) {
        byte[] key = MetadataKeyHelper.getPartitionStoreKey(partitionStore.getPartitionId());
        put(key, partitionStore.toByteArray());
    }

    public Metapb.PartitionRaft getPartitionRaft(int partitionId) {
        byte[] key = MetadataKeyHelper.getPartitionRaftKey(partitionId);
        return get(Metapb.PartitionRaft.parser(), key);
    }

    public List<Metapb.PartitionRaft> getPartitionRafts() {
        byte[] key = MetadataKeyHelper.getPartitionRaftPrefix();
        return scan(Metapb.PartitionRaft.parser(), key);
    }

    public void savePartitionRaft(Metapb.PartitionRaft partitionRaft) {
        byte[] key = MetadataKeyHelper.getPartitionRaftKey(partitionRaft.getPartitionId());
        put(key, partitionRaft.toByteArray());
    }

    private String getMinDataLocation() {
        var counter = stateLocByFreeSpace();
        if (counter.isEmpty()) {
            counter = stateLocByPartitionCount();
            log.info("allocate db path using partition count: db count stats: {}", counter);
        } else {
            log.info("allocate db path using free space: db size stats: {}", counter);
        }

        int min = Integer.MAX_VALUE;
        String location = "";
        for (String k : counter.keySet()) {
            if (counter.get(k) < min) {
                min = counter.get(k);
                location = k;
            }
        }
        return location;
    }

    /**
     * get location count by allocated db count
     *
     * @return loc -> db count
     */
    private Map<String, Integer> stateLocByPartitionCount() {
        Map<String, Integer> counter = new HashMap<>();
        dataLocations.forEach(l -> counter.put(l, 0));

        getPartitionStores().forEach(ptStore -> {
            if (counter.containsKey(ptStore.getStoreLocation())) {
                counter.put(ptStore.getStoreLocation(),
                            counter.get(ptStore.getStoreLocation()) + 1);
            }
        });
        return counter;
    }

    /**
     * get location count by free space
     *
     * @return location -> free space, return null when disk usage greater than 20%
     */
    private Map<String, Integer> stateLocByFreeSpace() {
        Map<String, Integer> counter = new HashMap<>();
        double maxRate = 0;
        for (String loc : dataLocations) {
            var file = new File(loc);
            if (!file.exists()) {
                file.mkdirs();
            }

            // Estimated Size
            long left = (file.getFreeSpace() - getLocDbSizeDelta(loc)) / 1024 / 1024 * -1;

            var dbSizeRate = FileUtils.sizeOfDirectory(file) / file.getTotalSpace();
            // log.info("loc: {}, dir size {}, total size: {}, rate :{}", loc, FileUtils
            // .sizeOfDirectory(file),
            //        file.getTotalSpace(), dbSizeRate);
            if (dbSizeRate > maxRate) {
                maxRate = dbSizeRate;
            }
            counter.put(loc, (int) left);
        }
        // log.info("max rate: {}",  maxRate);

        if (maxRate < 0.2) {
            counter.clear();
        }
        return counter;
    }

    /**
     * db file delta by dbs, considering new db
     *
     * @param path
     * @return
     */
    private long getLocDbSizeDelta(String path) {
        File file = new File(path + "/db");
        if (!file.exists()) {
            return 0;
        }

        long max = 0;
        int n = 0;
        int sum = 0;
        File[] fs = file.listFiles();
        if (fs != null) {
            for (File sub : fs) {
                if (sub.isDirectory()) {
                    continue;
                }

                long size = FileUtils.sizeOfDirectory(sub);
                if (size > max) {
                    max = size;
                }
                n += 1;
            }
        }

        return max * n - sum;
    }

    private String getMinRaftLocation() {
        Map<String, Integer> counter = new HashMap<>();
        raftLocations.forEach(l -> {
            counter.put(l, Integer.valueOf(0));
        });

        getPartitionRafts().forEach(ptRaft -> {
            if (counter.containsKey(ptRaft.getRaftLocation())) {
                counter.put(ptRaft.getRaftLocation(), counter.get(ptRaft.getRaftLocation()) + 1);
            }
        });

        int min = Integer.MAX_VALUE;
        String location = "";
        for (String k : counter.keySet()) {
            if (counter.get(k) < min) {
                min = counter.get(k);
                location = k;
            }
        }
        return location;
    }

    /**
     * Get the location of the partitioned data storage, if distributed data does not exist,
     * automatically create a new location.
     *
     * @param partitionId
     * @return
     */
    public String getPartitionStoreLocation(int partitionId, String dbName) {
        Metapb.PartitionStore location = getPartitionStore(partitionId);
        if (location == null) {
            synchronized (this) {
                location = getPartitionStore(partitionId);
                if (location == null) {
                    // Find the storage with the least number of partitions
                    location = Metapb.PartitionStore.newBuilder()
                                                    .setPartitionId(partitionId)
                                                    .setStoreLocation(getMinDataLocation())
                                                    .build();
                    // TODO: Select the path with the least number of partitions.
                    savePartitionStore(location);
                }
            }
        }
        return location.getStoreLocation();
    }

    public String getPartitionRaftLocation(int partitionId) {
        Metapb.PartitionRaft location = getPartitionRaft(partitionId);
        if (location == null) {
            synchronized (this) {
                location = getPartitionRaft(partitionId);
                if (location == null) {
                    // Find the storage with the least number of partitions
                    location = Metapb.PartitionRaft.newBuilder()
                                                   .setPartitionId(partitionId)
                                                   .setRaftLocation(getMinRaftLocation())
                                                   .build();
                    // TODO: Select the path with the fewest partitions.
                    savePartitionRaft(location);
                }
            }
        }
        return location.getRaftLocation();
    }
}
