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

package org.apache.hugegraph.store.cli.cmd;

import java.util.List;

import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.cli.util.HgMetricX;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/18
 **/
@Slf4j
public class ScanTable extends Command {

    public static final byte[] EMPTY_BYTES = new byte[0];
    private final HgStoreClient storeClient;

    public ScanTable(String pd) {
        super(pd);
        storeClient = HgStoreClient.create(config);
    }

    @Override
    public void action(String[] params) throws PDException {
        String graphName = params[0];
        String tableName = params[1];
        PDClient pdClient = storeClient.getPdClient();
        List<Metapb.Partition> partitions = pdClient.getPartitions(0, graphName);
        HgStoreSession session = storeClient.openSession(graphName);
        int count = 0;
        byte[] position = null;
        HgMetricX metricX = HgMetricX.ofStart();
        for (Metapb.Partition partition : partitions) {
            while (true) {
                try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName,
                                                                             (int) (partition.getStartKey()),
                                                                             (int) (partition.getEndKey()),
                                                                             HgKvStore.SCAN_HASHCODE,
                                                                             EMPTY_BYTES)) {
                    if (position != null) {
                        iterator.seek(position);
                    }
                    while (iterator.hasNext()) {
                        iterator.next();
                        count++;
                        if (count % 3000 == 0) {
                            if (iterator.hasNext()) {
                                iterator.next();
                                position = iterator.position();
                                log.info("count is {}", count);
                            } else {
                                position = null;
                            }
                            break;
                        }
                    }
                    if (!iterator.hasNext()) {
                        position = null;
                        break;
                    }
                }
            }
        }
        metricX.end();
        log.info("*************************************************");
        log.info("*************  Scanning Completed  **************");
        log.info("Graph: {}", graphName);
        log.info("Table: {}", tableName);
        log.info("Keys: {}", count);
        log.info("Total: {} seconds.", metricX.past() / 1000);
        log.info("*************************************************");
    }

}
