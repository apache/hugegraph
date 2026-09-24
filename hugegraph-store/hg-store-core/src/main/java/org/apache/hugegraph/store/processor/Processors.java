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

package org.apache.hugegraph.store.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/10
 **/
@Slf4j
public class Processors {

    private final HgStoreEngine engine;
    private final Map<Class, CommandProcessor> processors = new ConcurrentHashMap<>(16);

    public Processors(HgStoreEngine engine) {
        register(new BuildIndexProcessor(engine));
        register(new ChangeShardProcessor(engine));
        register(new CleanPartitionProcessor(engine));
        register(new DbCompactionProcessor(engine));
        register(new MovePartitionProcessor(engine));
        register(new PartitionRangeChangeProcessor(engine));
        register(new SplitPartitionProcessor(engine));
        register(new TransferLeaderProcessor(engine));

        this.engine = engine;
    }

    public void register(CommandProcessor processor) {
        processors.put(processor.getClass(), processor);
    }

    public CommandProcessor get(Class clazz) {
        return processors.get(clazz);
    }

    public void process(PartitionHeartbeatResponse instruct,
                        Consumer<Integer> consumer) {
        int partitionId = instruct.getPartition().getId();
        PartitionEngine engine = this.engine.getPartitionEngine(partitionId);
        if (engine == null || !engine.isLeader()) {
            return;
        }

        //consumer.accept(0);
        int errorCount = 0;
        for (var entry : this.processors.entrySet()) {
            try {
                entry.getValue().executeInstruct(instruct);
            } catch (Exception e) {
                errorCount++;
                log.error("execute instruct {} error: ", instruct, e);
            }
        }
        if (errorCount > 0) {
            log.warn("Processing completed with {} errors out of {} processors",
                     errorCount, this.processors.size());
            consumer.accept(errorCount); // Return error count
        } else {
            consumer.accept(0); // Success
        }
    }
}
