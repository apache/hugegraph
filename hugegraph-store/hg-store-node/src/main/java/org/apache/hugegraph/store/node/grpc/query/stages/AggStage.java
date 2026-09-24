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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hugegraph.store.business.itrv2.FileObjectIterator;
import org.apache.hugegraph.store.business.itrv2.TypeTransIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.grpc.query.AggregationType;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;
import org.apache.hugegraph.store.query.Tuple2;
import org.apache.hugegraph.store.query.func.AggregationFunction;
import org.apache.hugegraph.store.query.func.AggregationFunctions;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.store.util.SortShuffle;

/**
 * Aggregation calculation
 */
public class AggStage implements QueryStage {

    private static final Integer MAP_SIZE = 10000;

    private final Map<List<Object>, List<AggregationFunction>> maps = new ConcurrentHashMap<>();

    private List<Tuple2<AggregationType, String>> funcMetas = new ArrayList<>();

    private Integer functionSize;

    private String file;

    private String path;

    @Override
    public boolean isIterator() {
        return true;
    }

    /**
     * Initialization method for initializing aggregation function metadata list and path.
     *
     * @param objects parameter array, the first parameter is the list of aggregation function metadata.
     */
    @Override
    public void init(Object... objects) {
        this.funcMetas = (List<Tuple2<AggregationType, String>>) objects[0];
        functionSize = funcMetas.size();
        path = SortShuffle.getBasePath() + "agg_tmp_" + Thread.currentThread().getId() + "/";
        new File(path).mkdirs();
    }

    /**
     * Process data in the iterator and return the result iterator
     *
     * @param result data result object
     * @return return the processed iterator
     */
    @Override
    public Iterator<PipelineResult> handleIterator(PipelineResult result) {
        if (result.getResultType() == PipelineResultType.MKV) {
            var kv = result.getKv();
            if (!maps.containsKey(kv.getKeys())) {
                maps.putIfAbsent(kv.getKeys(), generateFunctions());
            }

            for (int i = 0; i < functionSize; i++) {
                var function = maps.get(kv.getKeys()).get(i);
                Object value = kv.getValues().get(i);
                if (function instanceof AggregationFunctions.AvgFunction) {
                    var avgFunction = (AggregationFunctions.AvgFunction) function;
                    value = transValue(avgFunction.getFiledClassType(), value);
                }
                function.iterate(value);
            }
        }

        if (maps.size() > MAP_SIZE) {
            // write to local buffer
            synchronized (this.maps) {
                if (maps.size() > MAP_SIZE) {
                    writeToFile(changeToList());
                }
            }
        }

        if (result.isEmpty()) {
            var list = changeToList();
            if (this.file == null) {
                return new TypeTransIterator<>(list.iterator(), PipelineResult::new,
                                               () -> PipelineResult.EMPTY).toIterator();
            } else {
                writeToFile(list);
                return new TypeTransIterator<>(
                        new FileObjectIterator<>(this.file,
                                                 SortShuffleSerializer.ofBackendColumnSerializer()),
                        PipelineResult::new, () -> PipelineResult.EMPTY
                ).toIterator();
            }
        }

        return null;
    }

    /**
     * Implicit conversion for avg function
     *
     * @param clz   the class type of the value
     * @param value value
     * @return Double value
     */
    private Double transValue(Class clz, Object value) {
        Double retValue = null;

        if (clz.equals(Integer.class)) {
            retValue = (double) (int) value;
        } else if (clz.equals(Long.class)) {
            retValue = (double) (long) value;
        } else if (clz.equals(Double.class)) {
            retValue = (double) value;
        } else if (clz.equals(Float.class)) {
            retValue = (double) (float) value;
        } else if (clz.equals(String.class)) {
            retValue = Double.valueOf((String) value);
        }

        return retValue;
    }

    @Override
    public String getName() {
        return "AGG_STAGE";
    }

    /**
     * Generate function list.
     *
     * @return aggregation function list.
     */
    private List<AggregationFunction> generateFunctions() {

        List<AggregationFunction> result = new ArrayList<>();

        for (var funcMeta : funcMetas) {
            result.add(QueryUtil.createFunc(funcMeta.getV1(), funcMeta.getV2()));
        }
        return result;
    }

    private List<MultiKv> changeToList() {
        List<MultiKv> result = new ArrayList<>();
        for (var entry : this.maps.entrySet()) {
            result.add(new MultiKv(entry.getKey(),
                                   entry.getValue().stream()
                                        .map(x -> x.getBuffer())
                                        .collect(Collectors.toList())));
        }

        result.sort(MultiKv::compareTo);
        this.maps.clear();
        return result;
    }

    private void writeToFile(List<MultiKv> list) {
        if (this.file == null) {
            file = path + System.currentTimeMillis() % 10000 + ".dat";
        }

        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(this.file, true));
            for (var item : list) {
                oos.writeObject(item);
            }
            this.maps.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        this.maps.clear();
        this.funcMetas.clear();
    }
}
