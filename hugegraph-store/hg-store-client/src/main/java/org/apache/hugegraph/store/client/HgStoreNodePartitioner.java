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

package org.apache.hugegraph.store.client;

import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;

/**
 * created on 2021/10/12
 *
 * @version 1.0.0
 */
public interface HgStoreNodePartitioner {

    /**
     * The partition algorithm implementation, that specialized by user.
     *
     * @param builder   The builder of HgNodePartitionerBuilder. It's supposed to be invoked
     *                  directly by user.
     *                  <b>e.g. builder.add(nodeId,address,partitionId);</b>
     * @param graphName
     * @param startKey
     * @param endKey
     * @return status:
     * <ul>
     *     <li>0: The partitioner is OK.</li>
     *     <li>10: The partitioner is not work.</li>
     * </ul>
     */
    int partition(HgNodePartitionerBuilder builder, String graphName, byte[] startKey,
                  byte[] endKey);

    /**
     * @param builder
     * @param graphName
     * @param startCode hash code
     * @param endCode   hash code
     * @return
     */
    default int partition(HgNodePartitionerBuilder builder, String graphName, int startCode,
                          int endCode) {
        return this.partition(builder, graphName
                , HgStoreClientConst.ALL_PARTITION_OWNER
                , HgStoreClientConst.ALL_PARTITION_OWNER);
    }

    default int partition(HgNodePartitionerBuilder builder, String graphName, int partitionId) {
        return this.partition(builder, graphName
                , HgStoreClientConst.ALL_PARTITION_OWNER
                , HgStoreClientConst.ALL_PARTITION_OWNER);
    }

    default String partition(String graphName, byte[] startKey) throws PDException {
        return null;
    }

    default String partition(String graphName, int code) throws PDException {
        return null;
    }

    default List<String> getStores(String graphName) throws PDException {
        return null;
    }

}
