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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.store.business.itrv2.TypeTransIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.node.grpc.query.MultiKeyComparator;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;
import org.apache.hugegraph.store.query.BaseElementComparator;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.store.util.SortShuffle;
import org.apache.hugegraph.structure.BaseElement;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

/**
 * Sorting
 */
@Slf4j
public class OrderByStage implements QueryStage {

    private SortShuffle sortShuffle;

    private Iterator iterator;

    private boolean isAsc;

    private PipelineResultType resultType = PipelineResultType.HG_ELEMENT;

    @Override
    public void init(Object... objects) {
        var orderBys = QueryUtil.fromStringBytes((List<ByteString>) objects[0]);
        var groupBys = QueryUtil.fromStringBytes((List<ByteString>) objects[1]);
        this.isAsc = (boolean) objects[3];

        // agg
        if ((Boolean) objects[2]) {
            if (orderBys == null) {
                sortShuffle = new SortShuffle<>(MultiKv::compareTo,
                                                SortShuffleSerializer.ofMultiKvSerializer());
            } else {
                List<Integer> orders = new ArrayList<>();
                for (Id id : orderBys) {
                    orders.add(groupBys.indexOf(id));
                }
                sortShuffle = new SortShuffle<>(new MultiKeyComparator(orders),
                                                SortShuffleSerializer.ofMultiKvSerializer());
            }
            resultType = PipelineResultType.MKV;
        } else {
            sortShuffle = new SortShuffle<>(new BaseElementComparator(orderBys, this.isAsc),
                                            SortShuffleSerializer.ofBaseElementSerializer());
            resultType = PipelineResultType.HG_ELEMENT;
        }

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
        if (!result.isEmpty()) {
            try {
                if (result.getResultType() == PipelineResultType.MKV) {
                    sortShuffle.append(result.getKv());
                } else if (result.getResultType() == PipelineResultType.HG_ELEMENT) {
                    sortShuffle.append(result.getElement());
                }
                return null;
            } catch (Exception e) {
                log.info("GROUP_BY_STAGE, append: ", e);
            }
        } else {
            // last empty flag
            try {
                sortShuffle.finish();
                iterator = sortShuffle.getIterator();
            } catch (Exception e) {
                log.error("GROUP_BY_STAGE:", e);
            }
        }

        return new TypeTransIterator<PipelineResult, PipelineResult>(new Iterator<>() {

            private boolean closeFlag = false;

            @Override
            public boolean hasNext() {
                var ret = iterator.hasNext();
                if (!ret) {
                    sortShuffle.close();
                    // sort shuffle close，will clear list，causing size and cursor are not
                    // consistent  true
                    // Only for small data scenarios that do not use file
                    closeFlag = true;
                }
                return ret && !closeFlag;
            }

            @Override
            public PipelineResult next() {
                if (resultType == PipelineResultType.HG_ELEMENT) {
                    return new PipelineResult((BaseElement) iterator.next());
                } else {
                    return new PipelineResult((MultiKv) iterator.next());
                }
            }
        }, r -> r, () -> PipelineResult.EMPTY).toIterator();

    }

    @Override
    public String getName() {
        return "ORDER_BY_STAGE";
    }

    @Override
    public void close() {
        this.sortShuffle.close();
    }
}
