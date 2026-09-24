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

package org.apache.hugegraph.store.node.task.ttl;

import com.alipay.sofa.jraft.Status;
import com.google.protobuf.ByteString;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @date 2024/5/7
 **/
public abstract class TaskSubmitter {

    protected BusinessHandler handler;
    protected HgStoreNodeService service;

    public TaskSubmitter(HgStoreNodeService service, BusinessHandler handler) {
        this.service = service;
        this.handler = handler;
    }

    public abstract Status submitClean(Integer id, String graph, String table,
                                       LinkedList<ByteString> all,
                                       AtomicBoolean state, AtomicLong tableCounter,
                                       AtomicLong partitionCounter);

    public abstract Status submitCompaction(Integer id);
}
