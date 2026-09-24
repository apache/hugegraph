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

package org.apache.hugegraph.store.cmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

public class HgCmdBase {

    public static final byte GET_STORE_INFO = 0x01;
    public static final byte BATCH_PUT = 0x02;
    public static final byte CLEAN_DATA = 0x03;
    public static final byte RAFT_UPDATE_PARTITION = 0x04;
    public static final byte ROCKSDB_COMPACTION = 0x05;
    public static final byte CREATE_RAFT = 0x06;
    public static final byte DESTROY_RAFT = 0x07;
    public static final byte TTL_CLEAN = 0x08;
    public static final byte BLANK_TASK = 0x09;

    public static final byte REDIRECT_RAFT_TASK = 0x10;

    @Data
    public abstract static class BaseRequest implements Serializable {

        private String graphName;
        private int partitionId;

        public abstract byte magic();
    }

    @Data
    public abstract static class BaseResponse implements Serializable {

        List<PartitionLeader> partitionLeaders;
        private HgCmdProcessor.Status status;

        public synchronized BaseResponse addPartitionLeader(PartitionLeader ptLeader) {
            if (partitionLeaders == null) {
                partitionLeaders = new ArrayList<>();
            }
            partitionLeaders.add(ptLeader);
            return this;
        }

        public static class PartitionLeader implements Serializable {

            private final Integer partId;
            private final Long storeId;

            public PartitionLeader(Integer partId, Long storeId) {
                this.partId = partId;
                this.storeId = storeId;
            }

            public Long getStoreId() {
                return storeId;
            }

            public Integer getPartId() {
                return partId;
            }
        }
    }
}
