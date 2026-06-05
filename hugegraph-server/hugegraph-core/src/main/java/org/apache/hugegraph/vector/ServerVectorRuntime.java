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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtilCommon;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.SimpleMappedReader;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

public class ServerVectorRuntime extends AbstractVectorRuntime<Id> {

    private static final Logger LOG = Log.logger(ServerVectorRuntime.class);
    private HugeGraphParams graphParams = null;

    public ServerVectorRuntime(String basePath, HugeGraphParams graphParams) {
        super(basePath);
        this.graphParams = graphParams;
    }

    @Override
    public void update(Id vectorIndexLableId, Iterator<VectorRecord> records) {
        IndexContext<Id> context = obtainContext(vectorIndexLableId);

        // deal with the records
        while (records.hasNext()) {
            VectorRecord record = records.next();
            if (record.isDeleted()) {
                handleDelete(record, context);
                continue;
            }
            handleBuilding(record, context);
        }
    }

    @Override
    public Iterator<Integer> search(Id indexLabelId, float[] queryVector, int topK) {
        IndexContext<Id> context = obtainContext(indexLabelId);
        LOG.debug("search: ilId={}, graphSize={}, topK={}",
                  indexLabelId, context.graphView().size(), topK);
        VectorTypeSupport vectorTypeSupport =
                VectorizationProvider.getInstance().getVectorTypeSupport();
        VectorFloat<?> vector = vectorTypeSupport.createFloatVector(queryVector);

        RandomAccessVectorValues ravv = context.vectors;

        SearchResult sr = GraphSearcher.search(vector,
                                               topK, // return top k result
                                               ravv,
                                               context.similarityFunction,
                                               context.graphView(),
                                               Bits.ALL);

        return Arrays.stream(sr.getNodes()).mapToInt(c -> c.node).iterator();
    }

    @Override
    public boolean isUpdateMetaData(Id indexLabelId) {
        IndexContext<Id> context = obtainContext(indexLabelId);
        return context.metaData.isUpdateFromLog();
    }

    private void handleDelete(VectorRecord record, IndexContext<Id> context) {
        // now just mark the record deleted
        // TODO： add the counter to save the deleted count
        // can be cleanup before flush and until count meet the threshold.
        if(context.builder.getGraph().containsNode(record.getVectorId())){
            context.builder.markNodeDeleted(record.getVectorId());
        }
    }

    private void handleBuilding(VectorRecord record, IndexContext<Id> context) {

        if (context.vectors.getVector(record.getVectorId()) != null) {
            LOG.debug("handleBuilding: vectorId={} already exists, skip",
                      record.getVectorId());
            return;
        }

        VectorTypeSupport vectorTypeSupport =
                VectorizationProvider.getInstance().getVectorTypeSupport();
        VectorFloat<?> vector = vectorTypeSupport.createFloatVector(record.getVectorData());

        context.vectors.addNode(record.getVectorId(), vector);
        context.builder.addGraphNode(record.getVectorId(), vector);

        LOG.debug("handleBuilding: added vectorId={}, graphSize={}",
                  record.getVectorId(), context.builder.getGraph().size());
    }

    @Override
    public IndexContext<Id> createNewContext(Id indexLabelId) {

        if (!checkPathValid(indexLabelId)) {
            IndexContext<Id> ctx = getNewContext(indexLabelId, null);
            vectorMap.put(indexLabelId, ctx);
            return ctx;
        }
        // construct the dataPath to read the index and sequence
        Path currentPathDir = null;
        try {
            currentPathDir = getOnDiskIndexDirPath(indexLabelId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to resolve index dir for " + indexLabelId.asString(), e);
        }
        // get the index and json
        try (ReaderSupplier rs = new SimpleMappedReader.Supplier(currentPathDir.resolve(INDEX_FILE_NAME));
             OnDiskGraphIndex index = OnDiskGraphIndex.load(rs)) {
            RandomAccessVectorValues ravv = index.getView();
            IndexContext<Id> context = getNewContext(indexLabelId, ravv);
            Path currentJsonPath = currentPathDir.resolve(META_FILE_NAME);
            String jsonMetaData = Files.readString(currentJsonPath);
            // get the metaData from disk but not update from rocksdb
            IndexContext.IndexContextMetaData metaData = JsonUtilCommon.fromJson(jsonMetaData,
                                                              IndexContext.IndexContextMetaData.class);
            metaData.setUpdateFromLog(false);
            context.setMetaData(metaData);
            vectorMap.put(indexLabelId, context);
            return context;
        } catch (FileNotFoundException e) {
            System.err.println("Index file not found: " + currentPathDir);
            IndexContext<Id> empty = getNewContext(indexLabelId, null);
            vectorMap.put(indexLabelId, empty);
            return empty;
        } catch (IOException e) {
            throw new RuntimeException("Read index failed: " + currentPathDir, e);
        }
    }

    @Override
    public String idToString(Id id) {
        return id.asString();
    }

    IndexContext<Id> getNewContext(Id vectorIndexLableId, RandomAccessVectorValues ravv) {
        // if indexLabelId invalid , will throw error
        IndexLabel il = graphParams.graph().indexLabel(vectorIndexLableId);
        Map<String, Object> userData = il.userdata();
        E.checkArgument(userData.containsKey("similarityFunction"),
                        "The similarityFunction can't be" + " " + "empty");
        E.checkArgument(userData.containsKey("dimension"), "The dimension can't be empty");

        int dimension = (int) userData.get("dimension");
        VectorSimilarityFunction similarityFunction =
                getSimilarityFunction((String) userData.get("similarityFunction"));
        /* use the magic number first
        * TODO: use the config to set */
        int M = userData.containsKey("M") ? (int) userData.get("M") : 16;
        int beamWidth = userData.containsKey("beamWidth") ? (int) userData.get("beamWidthM") : 100;
        float neighborOverflow = userData.containsKey("neighborOverflow") ?
                                 (float) userData.get("neighborOverflow") : (float) 1.2;
        float alpha = userData.containsKey("alpha") ? (float) userData.get("alpha") : (float) 1.2;

        RandomAccessVectorValues vectorValueMap = ravv != null ? ravv:
                new UpdatableRandomAccessVectorValues(new HashMap<>(), dimension);

        GraphIndexBuilder builder = new GraphIndexBuilder(vectorValueMap, similarityFunction,
                                                          M, beamWidth, neighborOverflow, alpha);
        // need to test the vectorValueMap is updatable
        return new IndexContext<Id>(vectorIndexLableId, (UpdatableRandomAccessVectorValues) vectorValueMap, builder,0,
                                    dimension, similarityFunction);
    }

    public VectorSimilarityFunction getSimilarityFunction(String similarityFunction) {
        similarityFunction = similarityFunction.toUpperCase();
        switch (similarityFunction) {
            case "EUCLIDEAN":
                return VectorSimilarityFunction.EUCLIDEAN;
            case "COSINE":
                return VectorSimilarityFunction.COSINE;
            case "DOT_PRODUCT":
                return VectorSimilarityFunction.DOT_PRODUCT;
            default:
                throw new AssertionError(
                        "Unsupported similarity function: {}" + similarityFunction);
        }
    }

}
