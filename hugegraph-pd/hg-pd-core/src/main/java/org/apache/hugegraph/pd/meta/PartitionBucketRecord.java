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

import java.nio.charset.StandardCharsets;

import lombok.Getter;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;

/**
 * Durable partition-to-bucket binding under lease fencing.
 */
@Getter
public class PartitionBucketRecord {

    private static final String SEP = "\t";

    private final String graphName;
    private final int partitionId;
    private final long ownerStoreId;
    private final long leaseEpoch;
    private final String bucket;
    private final long updateTimestamp;

    public PartitionBucketRecord(String graphName, int partitionId, long ownerStoreId,
                                 long leaseEpoch, String bucket, long updateTimestamp) {
        this.graphName = graphName;
        this.partitionId = partitionId;
        this.ownerStoreId = ownerStoreId;
        this.leaseEpoch = leaseEpoch;
        this.bucket = bucket;
        this.updateTimestamp = updateTimestamp;
    }

    public byte[] toBytes() {
        String data = String.join(SEP,
                                  graphName,
                                  String.valueOf(partitionId),
                                  String.valueOf(ownerStoreId),
                                  String.valueOf(leaseEpoch),
                                  bucket,
                                  String.valueOf(updateTimestamp));
        return data.getBytes(StandardCharsets.UTF_8);
    }

    public static PartitionBucketRecord fromBytes(byte[] bytes) throws PDException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        String raw = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = raw.split(SEP, -1);
        if (parts.length != 6) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE,
                                  "invalid partition bucket record format");
        }
        try {
            return new PartitionBucketRecord(parts[0],
                                             Integer.parseInt(parts[1]),
                                             Long.parseLong(parts[2]),
                                             Long.parseLong(parts[3]),
                                             parts[4],
                                             Long.parseLong(parts[5]));
        } catch (RuntimeException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE,
                                  "invalid partition bucket record value", e);
        }
    }
}

