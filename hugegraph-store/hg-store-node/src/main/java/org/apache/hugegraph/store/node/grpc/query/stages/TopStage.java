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

package org.apache.hugegraph.store.node.grpc.query.stages;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hugegraph.store.business.itrv2.TypeTransIterator;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;
import org.apache.hugegraph.store.query.BaseElementComparator;
import org.apache.hugegraph.structure.BaseElement;

import com.google.protobuf.ByteString;

public class TopStage implements QueryStage {

    private PriorityBlockingQueue<BaseElement> queue;

    private BaseElementComparator comparator;
    private boolean isAsc;

    private int limit;

    // todo: check concurrency
    @Override
    public void init(Object... objects) {
        this.limit = (int) objects[0];
        this.isAsc = (boolean) objects[2];

        // Need to build a reverse heap
        this.comparator =
                new BaseElementComparator(QueryUtil.fromStringBytes((List<ByteString>) objects[1]),
                                          !isAsc);
        this.queue = new PriorityBlockingQueue<>(limit, this.comparator);
    }

    @Override
    public boolean isIterator() {
        return true;
    }

    @Override
    public Iterator<PipelineResult> handleIterator(PipelineResult result) {
        if (result == null) {
            return null;
        }

        if (result.isEmpty()) {

            this.comparator.reverseOrder();
            var reverseQueue = new PriorityQueue<>(this.comparator);
            reverseQueue.addAll(this.queue);
            queue.clear();

            return new TypeTransIterator<>(new Iterator<BaseElement>() {
                @Override
                public boolean hasNext() {
                    return reverseQueue.size() > 0;
                }

                @Override
                public BaseElement next() {
                    return reverseQueue.poll();
                }
            }, PipelineResult::new, () -> PipelineResult.EMPTY).toIterator();
        }

        if (result.getResultType() == PipelineResultType.HG_ELEMENT) {
            if (this.queue.size() < this.limit) {
                this.queue.add(result.getElement());
            } else {
                var top = this.queue.peek();
                var element = result.getElement();
                if (this.comparator.compare(element, top) > 0) {
                    this.queue.poll();
                    this.queue.add(result.getElement());
                }
            }
        }

        return null;
    }

    @Override
    public String getName() {
        return "TOP_STAGE";
    }

    @Override
    public void close() {
        this.queue.clear();
    }
}
