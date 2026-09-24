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

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;

import lombok.Data;

@Data
public class Partition implements Cloneable {

    // region id
    private int id;
    private String graphName;
    // Region key range [startKey, endKey)
    private long startKey;
    private long endKey;
    private long version;
    // shardlist version, shardlist increments by 1 each time it changes
    // private long confVer;

    private Metapb.PartitionState workState;
    // private PartitionRole role;
    // private List<Metapb.Shard> shardsList;
    // exclusive

    public Partition() {
        workState = Metapb.PartitionState.PState_Normal;
    }

    public Partition(Metapb.Partition protoObj) {
        id = protoObj.getId();
        graphName = protoObj.getGraphName();
        startKey = protoObj.getStartKey();
        endKey = protoObj.getEndKey();
        // shardsList = protoObj.getShardsList();
        workState = protoObj.getState();
        version = protoObj.getVersion();
        // confVer = protoObj.getConfVer();
        if (workState == Metapb.PartitionState.UNRECOGNIZED ||
            workState == Metapb.PartitionState.PState_None) {
            workState = Metapb.PartitionState.PState_Normal;
        }
    }

    public boolean isLeader() {
        PartitionEngine engine = HgStoreEngine.getInstance().getPartitionEngine(id);
        return engine != null && engine.isLeader();
    }

    public Metapb.Partition getProtoObj() {
        return Metapb.Partition.newBuilder()
                               .setId(id)
                               .setVersion(version)
                               // .setConfVer(confVer)
                               .setGraphName(graphName)
                               .setStartKey(startKey)
                               .setEndKey(endKey)
                               .setState(workState)
                               // .addAllShards(shardsList)
                               .build();
    }

    @Override
    public Partition clone() {
        Partition obj = null;
        try {
            obj = (Partition) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return obj;
    }

    @Override
    public String toString() {
        return getProtoObj().toString();
    }
}
