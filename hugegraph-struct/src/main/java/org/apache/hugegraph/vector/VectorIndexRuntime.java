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

// this module implement the logic of managing the vector index structure in memory
public interface VectorIndexRuntime<Id> {

    void update(Id indexLabelId, Iterator<VectorRecord> records);

    void init();

    void stop() throws IOException;

    void flush(Id indexLabelId) throws IOException;

    Iterator<Integer> search(Id indexLabelId, float[] queryVector, int topK);

    long getCurrentWaterMark(Id indexLabelId);

    int getNextVectorId(Id indexLabelId);

    long getNextSequence(Id indexLabelId);

    boolean isUpdateMetaData(Id indexLabelId);

    void updateMetaData(Id indexLabelId, int vectorId, long sequence);
}
