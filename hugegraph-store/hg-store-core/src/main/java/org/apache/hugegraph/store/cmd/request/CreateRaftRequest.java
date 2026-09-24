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

package org.apache.hugegraph.store.cmd.request;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.cmd.HgCmdBase;

import com.alipay.sofa.jraft.conf.Configuration;
import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRaftRequest extends HgCmdBase.BaseRequest {

    List<byte[]> values = new ArrayList<>();
    String peers;

    public List<Metapb.Partition> getPartitions() {
        try {
            List<Metapb.Partition> partitions = new ArrayList<>();
            for (byte[] partition : values) {
                partitions.add(Metapb.Partition.parseFrom(partition));
            }
            return partitions;
        } catch (InvalidProtocolBufferException e) {
            log.error("CreateRaftNodeProcessor parse partition exception }", e);
        }
        return new ArrayList<>();
    }

    public void addPartition(Metapb.Partition partition) {
        values.add(partition.toByteArray());
    }

    public Configuration getConf() {
        Configuration conf = null;
        if (peers != null) {
            conf = new Configuration();
            conf.parse(this.peers);
        }
        return conf;
    }

    public void setConf(Configuration conf) {
        if (conf != null) {
            this.peers = conf.toString();
        }
    }

    @Override
    public byte magic() {
        return HgCmdBase.CREATE_RAFT;
    }
}
