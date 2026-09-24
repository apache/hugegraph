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

package org.apache.hugegraph.store.client.query;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.query.func.AggregationFunction;
import org.apache.hugegraph.store.query.func.AggregationFunctionParam;
import org.apache.hugegraph.store.query.func.AggregationFunctions;
import org.apache.hugegraph.structure.KvElement;

public class StreamFinalAggregationIterator<E> implements HgKvIterator<E> {

    private final HgKvIterator iterator;

    private final List<AggregationFunctionParam> aggregationParams;

    private List<AggregationFunction> functions;

    private KvElement prev = null;

    private KvElement data = null;

    public StreamFinalAggregationIterator(HgKvIterator iterator,
                                          List<AggregationFunctionParam> aggregations) {
        this.iterator = iterator;
        this.aggregationParams = aggregations;
    }

    @Override
    public byte[] key() {
        return this.iterator.key();
    }

    @Override
    public byte[] value() {
        return this.iterator.value();
    }

    @Override
    public void close() {
        this.iterator.close();
    }

    @Override
    public byte[] position() {
        return this.iterator.position();
    }

    @Override
    public void seek(byte[] position) {
        this.iterator.seek(position);
    }

    @Override
    public boolean hasNext() {

        while (iterator.hasNext()) {
            var next = (KvElement) iterator.next();
            if (prev == null) {
                // first element, initial
                prev = next;
                functions = getAggregationList();
                merge(prev.getValues());
                continue;
            }

            if (keyEquals(next.getKeys(), prev.getKeys())) {
                merge(next.getValues());
            } else {
                // Generate result
                data = KvElement.of(prev.getKeys(), term());
                prev = next;
                functions = getAggregationList();
                merge(prev.getValues());
                break;
            }
        }

        // Consume the last prev element
        if (!iterator.hasNext() && prev != null && data == null) {
            data = KvElement.of(prev.getKeys(), term());
            prev = null;
        }

        return data != null;
    }

    private void merge(List<Object> values) {
        for (int i = 0; i < functions.size(); i++) {
            functions.get(i).merge(values.get(i));
        }
    }

    private List<Object> term() {
        var values = functions.stream().map(f -> f.reduce()).collect(Collectors.toList());
        functions.clear();
        return values;
    }

    @Override
    public E next() {
        var rst = data;
        data = null;
        return (E) rst;
    }

    private boolean keyEquals(List l1, List l2) {
        if (l1 == null && l2 == null) {
            return true;
        }
        if (l1 != null && l2 == null || l1 == null || l1.size() != l2.size()) {
            return false;
        }

        for (int i = 0; i < l1.size(); i++) {
            if (!l1.get(i).equals(l2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private List<AggregationFunction> getAggregationList() {
        return this.aggregationParams.stream()
                                     .map((Function<AggregationFunctionParam,
                                             AggregationFunction>) param -> {
                                         var filedType = param.getFieldType().getGenericType();
                                         switch (param.getFunctionType()) {
                                             case SUM:
                                                 return new AggregationFunctions.SumFunction(
                                                         param.getField(), getSupplier(filedType));
                                             case MIN:
                                                 return new AggregationFunctions.MinFunction(
                                                         param.getField(), getSupplier(filedType));
                                             case MAX:
                                                 return new AggregationFunctions.MaxFunction(
                                                         param.getField(), getSupplier(filedType));
                                             case AVG:
                                                 return new AggregationFunctions.AvgFunction(
                                                         getSupplier(filedType));
                                             case COUNT:
                                                 return new AggregationFunctions.CountFunction();
                                             default:
                                                 throw new RuntimeException(
                                                         "unsupported function type: " +
                                                         param.getFunctionType());
                                         }
                                     })
                                     .collect(Collectors.toList());
    }

    private Supplier getSupplier(String type) {
        return AggregationFunctions.getAggregationBufferSupplier(type);
    }
}
