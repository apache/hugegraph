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

package org.apache.hugegraph.store.node.grpc.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.stages.EarlyStopException;

public class QueryPlan {

    private final List<QueryStage> stages;

    public QueryPlan() {
        stages = new LinkedList<>();
    }

    public void addStage(QueryStage pipeline) {
        this.stages.add(pipeline);
    }

    public boolean onlyStopStage() {
        return stages.size() == 1 && "STOP_STAGE".equals(stages.get(0).getName());
    }

    /**
     * Judge if there is aggregation stage
     *
     * @return return false if not
     */
    public boolean containsAggStage() {
        return stages.stream().anyMatch(stage -> stage.getName().equals("AGG_STAGE"));
    }

    /**
     * execute pipeline
     *
     * @param data the input data
     * @return null when filtered or limited, iterator when encounter an iterator stage, or
     * element when plain pipeline
     * @throws EarlyStopException throws early stop exception when reach the limit of limit stage
     */
    public Object execute(PipelineResult data) throws EarlyStopException {
        if (data == null || this.stages.isEmpty()) {
            return data;
        }

        List<Object> current = new ArrayList<>();
        List<Object> next = new ArrayList<>();

        next.add(data);

        for (QueryStage stage : stages) {
            current.clear();
            current.addAll(next);
            next.clear();
            for (var item : current) {
                if (item instanceof Iterator) {
                    var itr = (Iterator<PipelineResult>) item;
                    while (itr.hasNext()) {
                        callStage(stage, next, itr.next());
                    }
                } else {
                    callStage(stage, next, (PipelineResult) item);
                }
            }
        }

        if (next.isEmpty()) {
            return null;
        }

        if (next.get(0) instanceof Iterator || next.size() == 1) {
            return next.get(0);
        }

        return next.iterator();
    }

    private void callStage(QueryStage stage, List<Object> list, PipelineResult pre) throws
                                                                                    EarlyStopException {
        Object ret;
        if (stage.isIterator()) {
            ret = stage.handleIterator(pre);
        } else {
            ret = stage.handle(pre);
        }

        if (ret != null) {
            list.add(ret);
        }
    }

    @Override
    public String toString() {
        var names = String.join(", ", stages.stream().map(QueryStage::getName)
                                            .collect(Collectors.toList()));
        return "QueryPlan{" + "stages=[" + names + "]}";
    }

    public void clear() {
        for (var stage : stages) {
            stage.close();
        }
        this.stages.clear();
    }

    public boolean isEmpty() {
        return this.stages.isEmpty();
    }

    public boolean hasIteratorResult() {
        return this.stages.stream().anyMatch(QueryStage::isIterator);
    }
}
