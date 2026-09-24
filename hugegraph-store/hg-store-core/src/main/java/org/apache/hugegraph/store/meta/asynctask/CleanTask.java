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

package org.apache.hugegraph.store.meta.asynctask;

import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanTask extends AbstractAsyncTask {

    public CleanTask(int partitionId, String graphName, AsyncTaskState state, Object attach) {
        super(partitionId, graphName, state, attach);
    }

    @Override
    public String getType() {
        return "CLEAN_TYPE";
    }

    @Override
    protected void onError() {
        cleanTask();
    }

    @Override
    protected void onNotFinished() {
        cleanTask();
    }

    private void cleanTask() {
        log.info("CleanTask begin to run:{}", this);
        var storeEngine = HgStoreEngine.getInstance();
        if (storeEngine != null) {
            if (getExtra() != null) {
                CleanDataRequest request = (CleanDataRequest) getExtra();
                var partition = storeEngine.getPartitionManager()
                                           .getPartition(getGraphName(), getPartitionId());
                // Only allow cleaning data outside of this partition. Tasks such as shrinking
                // can cause interference, and the partition cannot be deleted.
                if (request.getKeyEnd() == partition.getStartKey() &&
                    request.getKeyEnd() == partition.getEndKey() &&
                    request.getCleanType() == CleanType.CLEAN_TYPE_EXCLUDE_RANGE &&
                    !request.isDeletePartition()) {
                    storeEngine.getBusinessHandler()
                               .cleanPartition(getGraphName(), getPartitionId(),
                                               request.getKeyStart(), request.getKeyEnd(),
                                               request.getCleanType());
                }
            } else {
                storeEngine.getBusinessHandler().cleanPartition(getGraphName(), getPartitionId());
            }

            storeEngine.getPartitionEngine(getPartitionId()).getTaskManager()
                       .updateAsyncTaskState(getPartitionId(), getGraphName(), getId(),
                                             AsyncTaskState.SUCCESS);
        }
    }
}
