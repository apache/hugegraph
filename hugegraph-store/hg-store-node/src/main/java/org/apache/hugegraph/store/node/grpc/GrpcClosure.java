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

package org.apache.hugegraph.store.node.grpc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.raft.RaftClosure;

import io.grpc.stub.StreamObserver;

public abstract class GrpcClosure<V> implements RaftClosure {

    private final Map<Integer, Long> leaderMap = new HashMap<>();
    private V result;

    /**
     * Set the output result to raftClosure, for Follower, raftClosure is empty.
     */
    public static <V> void setResult(RaftClosure raftClosure, V result) {
        GrpcClosure closure = (GrpcClosure) raftClosure;
        if (closure != null) {
            closure.setResult(result);
        }
    }

    public static <V> RaftClosure newRaftClosure(StreamObserver<V> observer) {
        BatchGrpcClosure<V> wrap = new BatchGrpcClosure<>(0);
        return wrap.newRaftClosure(s -> {
            wrap.waitFinish(observer, r -> {
                return (V) wrap.selectError((List<FeedbackRes>) r);
            }, 0);
        });
    }

    public V getResult() {
        return result;
    }

    public void setResult(V result) {
        this.result = result;
    }

    public Map<Integer, Long> getLeaderMap() {
        return leaderMap;
    }

    @Override
    public void onLeaderChanged(Integer partId, Long storeId) {
        leaderMap.put(partId, storeId);
    }
}
