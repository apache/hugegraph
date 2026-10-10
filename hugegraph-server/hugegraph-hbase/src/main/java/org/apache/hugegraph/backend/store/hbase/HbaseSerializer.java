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

package org.apache.hugegraph.backend.store.hbase;

import java.util.Arrays;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class HbaseSerializer extends BinarySerializer {

    private static final Logger LOG = Log.logger(HbaseSerializer.class);
    private final short vertexLogicPartitions;
    private final short edgeLogicPartitions;

    public HbaseSerializer(HugeConfig config) {
        super(false, true, config.get(HbaseOptions.HBASE_ENABLE_PARTITION));
        this.vertexLogicPartitions = config.get(HbaseOptions.HBASE_VERTEX_PARTITION).shortValue();
        this.edgeLogicPartitions = config.get(HbaseOptions.HBASE_EDGE_PARTITION).shortValue();
        LOG.debug("vertexLogicPartitions: " + vertexLogicPartitions);
    }

    @Override
    protected short getPartition(HugeType type, Id id) {
        int hashcode = Arrays.hashCode(id.asBytes());
        short partition = 1;
        if (type.isEdge()) {
            partition = (short) (hashcode % this.edgeLogicPartitions);
        } else if (type.isVertex()) {
            partition = (short) (hashcode % this.vertexLogicPartitions);
        }
        return partition > 0 ? partition : (short) -partition;
    }
}
