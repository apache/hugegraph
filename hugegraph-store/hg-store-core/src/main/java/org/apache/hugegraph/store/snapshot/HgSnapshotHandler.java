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

package org.apache.hugegraph.store.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.util.HgStoreException;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;

import lombok.extern.slf4j.Slf4j;

@Deprecated
@Slf4j
public class HgSnapshotHandler {

    private static final String SHOULD_NOT_LOAD = "should_not_load";
    private static final String SNAPSHOT_DATA_PATH = "data";

    private final PartitionEngine partitionEngine;
    private final BusinessHandler businessHandler;

    public HgSnapshotHandler(PartitionEngine partitionEngine) {
        this.partitionEngine = partitionEngine;
        this.businessHandler = partitionEngine.getStoreEngine().getBusinessHandler();
    }

    public static String trimStartPath(String str, String prefix) {
        if (!prefix.endsWith(File.separator)) {
            prefix = prefix + File.separator;
        }
        if (str.startsWith(prefix)) {
            return (str.substring(prefix.length()));
        }
        return str;
    }

    public static void findFileList(File dir, File rootDir, List<String> files) {
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        File[] fs = dir.listFiles();
        if (fs != null) {
            for (File f : fs) {
                if (f.isFile()) {
                    files.add(trimStartPath(dir.getPath(), rootDir.getPath()) + File.separator +
                              f.getName());
                } else {
                    findFileList(f, rootDir, files);
                }
            }
        }
    }

    public Map<String, Partition> getPartitions() {
        return partitionEngine.getPartitions();
    }

    /**
     * create rocksdb checkpoint
     */
    public void onSnapshotSave(final SnapshotWriter writer) throws HgStoreException {
        final String snapshotDir = writer.getPath();

        if (partitionEngine != null) {
            // rocks db snapshot
            final String graphSnapshotDir = snapshotDir + File.separator + SNAPSHOT_DATA_PATH;
            businessHandler.saveSnapshot(graphSnapshotDir, "", partitionEngine.getGroupId());

            List<String> files = new ArrayList<>();
            File dir = new File(graphSnapshotDir);
            File rootDirFile = new File(writer.getPath());
            // add all files in data dir
            findFileList(dir, rootDirFile, files);

            // load snapshot by learner ??
            for (String file : files) {
                String checksum = calculateChecksum(writer.getPath() + File.separator + file);
                if (checksum.length() != 0) {
                    LocalFileMetaOutter.LocalFileMeta meta =
                            LocalFileMetaOutter.LocalFileMeta.newBuilder()
                                                             .setChecksum(checksum)
                                                             .build();
                    writer.addFile(file, meta);
                } else {
                    writer.addFile(file);
                }
            }
            // should_not_load wound not sync to learner
            markShouldNotLoad(writer, true);
        }
    }

    private String calculateChecksum(String path) {
        // only calculate .sst and .log(wal file) file
        final String emptyString = "";
        if (path.endsWith(".sst") || path.endsWith(".log")) {
            final int maxFullCheckLength = 8192;
            final int checkLength = 4096;
            try {
                File file = new File(path);
                long length = file.length();
                Checksum checksum = new CRC64();
                try (final RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                    byte[] buf = new byte[checkLength];
                    if (length <= maxFullCheckLength) {
                        int totalReadLen = 0;
                        while (totalReadLen < length) {
                            int readLen = raf.read(buf);
                            checksum.update(buf, 0, readLen);
                            totalReadLen += readLen;
                        }
                    } else {
                        // head
                        int readLen = raf.read(buf);
                        checksum.update(buf, 0, readLen);
                        // tail
                        raf.seek(length - checkLength);
                        readLen = raf.read(buf);
                        checksum.update(buf, 0, readLen);
                    }
                }
                // final checksum = crc checksum + file length
                return Long.toHexString(checksum.getValue()) + "_" + Long.toHexString(length);
            } catch (IOException e) {
                log.error("Failed to calculateChecksum for file {}. {}", path, e);
                return emptyString;
            }
        } else {
            return emptyString;
        }
    }

    public void onSnapshotLoad(final SnapshotReader reader, long committedIndex) throws
                                                                                 HgStoreException {
        final String snapshotDir = reader.getPath();

        // Locally saved snapshots do not need to be loaded
        if (shouldNotLoad(reader)) {
            log.info("skip to load snapshot because of should_not_load flag");
            return;
        }

        // Directly use snapshot
        final String graphSnapshotDir = snapshotDir + File.separator + SNAPSHOT_DATA_PATH;
        log.info("Raft {} begin loadSnapshot, {}", partitionEngine.getGroupId(), graphSnapshotDir);
        businessHandler.loadSnapshot(graphSnapshotDir, "", partitionEngine.getGroupId(),
                                     committedIndex);
        log.info("Raft {} end loadSnapshot.", partitionEngine.getGroupId());

        for (Metapb.Partition snapPartition : partitionEngine.loadPartitionsFromLocalDb()) {
            log.info("onSnapshotLoad loaded partition from local db. Partition: {}", snapPartition);
            partitionEngine.loadPartitionFromSnapshot(new Partition(snapPartition));

            Partition partition = partitionEngine.getPartition(snapPartition.getGraphName());
            if (partition == null) {
                log.warn("skip to load snapshot for {}-{}, it is not belong to this node",
                         snapPartition.getGraphName(), snapPartition.getId());
                continue;
            }

            var taskManager = partitionEngine.getTaskManager();
            // async tasks
            for (var task : taskManager.scanAsyncTasks(partitionEngine.getGroupId(),
                                                       snapPartition.getGraphName())) {
                task.handleTask();
            }
        }

        // mark snapshot has been loaded
        markShouldNotLoad(reader, false);
    }

    private boolean shouldNotLoad(final Snapshot snapshot) {
        String shouldNotLoadPath = getShouldNotLoadPath(snapshot);
        return new File(shouldNotLoadPath).exists();
    }

    private void markShouldNotLoad(final Snapshot snapshot, boolean saveSnapshot) {
        String shouldNotLoadPath = getShouldNotLoadPath(snapshot);
        try {
            FileUtils.writeStringToFile(new File(shouldNotLoadPath),
                                        saveSnapshot ? "saved snapshot" : "loaded snapshot",
                                        Charset.defaultCharset());
        } catch (IOException e) {
            log.error("Failed to create snapshot should not load flag file {}. {}",
                      shouldNotLoadPath, e);
        }
    }

    private String getShouldNotLoadPath(final Snapshot snapshot) {
        return snapshot.getPath() + File.separator + SHOULD_NOT_LOAD;
    }

}
