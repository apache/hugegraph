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

package org.apache.hugegraph.vector;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class VectorIndexManager<Id> {

    private static final Logger LOG = Log.logger(VectorIndexManager.class);

    private final VectorIndexStateStore<Id> stateStore;
    private final VectorIndexRuntime<Id> runtime;
    private final VectorTaskScheduler scheduler;

    public VectorIndexManager(VectorIndexStateStore<Id> stateStore,
                              VectorIndexRuntime<Id> runtime,
                              VectorTaskScheduler scheduler) {
        this.stateStore = stateStore;
        this.runtime = runtime;
        this.scheduler = scheduler;
    }

    public void init() {
        this.runtime.init();
    }

    public void stop() throws IOException {
        this.runtime.stop();
        this.stateStore.stop();
    }

    public void signal(Id indexLableId) {
        this.scheduler.execute(() -> this.processIndex(indexLableId));
    }

    private void processIndex(Id indexLableId) {

        long currentSequence = this.runtime.getCurrentWaterMark(indexLableId);
        LOG.debug("processIndex start: ilId={}, waterMark={}",
                  indexLableId, currentSequence);

        Iterator<VectorRecord> it =
                stateStore.scanDeltas(indexLableId, currentSequence < 0 ? 0 : currentSequence)
                          .iterator();

        this.runtime.update(indexLableId, it);

        LOG.debug("processIndex complete: ilId={}", indexLableId);
    }

    public Set<Id> searchVector(Id indexLableId, float[] vector, int topK) {
        Set<Id> result = null;
        result = stateStore.getVertex(indexLableId, runtime.search(indexLableId, vector, topK));
        return result;
    }

    private void initMetaData(Id indexLableId) {
        if (!runtime.isUpdateMetaData(indexLableId)) {
            int currentMaxVectorId = Math.max(runtime.getNextVectorId(indexLableId), 0);
            long currentSequence = Math.max(runtime.getNextSequence(indexLableId), 0L);
            runtime.updateMetaData(indexLableId,
                                   stateStore.getCurrentMaxVectorId(indexLableId,
                                                                    currentMaxVectorId) + 1,
                                   stateStore.getCurrentMaxSequence(indexLableId,
                                                                    currentSequence) + 1);
        }
    }

    public int getNextVectorId(Id indexLableId) {
        // Initialize from stateStore on first access
        initMetaData(indexLableId);
        // Always increment and return
        int id = runtime.getNextVectorId(indexLableId);
        runtime.updateMetaData(indexLableId, id + 1,
                               runtime.getNextSequence(indexLableId));
        return id;
    }

    public long getNextSequence(Id indexLableId) {
        // Initialize from stateStore on first access
        initMetaData(indexLableId);
        // Always increment and return
        long seq = runtime.getNextSequence(indexLableId);
        runtime.updateMetaData(indexLableId,
                               runtime.getNextVectorId(indexLableId), seq + 1);
        return seq;
    }

}
