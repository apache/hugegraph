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

package org.apache.hugegraph.structure;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.type.define.IndexVectorState;
import org.apache.hugegraph.util.E;

public class HugeVectorIndexMap extends HugeIndex{

    private static final AtomicLong VECTOR_INDEX_SEQUENCE = new AtomicLong(0);
    private long vector_sequence;
    private IndexVectorState vectorState;
    private IndexVectorState dirtyPrefix;

    public HugeVectorIndexMap(HugeGraph graph, IndexLabel indexLabel){
        super(graph, indexLabel);
        this.vectorState = IndexVectorState.BUILDING;
        vector_sequence = getGlobalNextSequence();
    }

    public HugeVectorIndexMap(HugeGraph graph, IndexLabel indexLabel, boolean removed){
        super(graph, indexLabel);
        this.vectorState = removed ? IndexVectorState.DELETING : IndexVectorState.BUILDING;
        vector_sequence = getGlobalNextSequence();
    }

    public HugeVectorIndexMap(HugeGraph graph, IndexLabel indexLabel, IndexVectorState state){
        super(graph, indexLabel);
        this.vectorState = state;
        vector_sequence = getGlobalNextSequence();
    }

    public long sequence(){
        return this.vector_sequence;
    }

    public void sequence(long sequence){
        this.vector_sequence = sequence;
    }

    public long globalSequence(){
        return VECTOR_INDEX_SEQUENCE.get();
    }

    public static long getGlobalNextSequence(){
        VECTOR_INDEX_SEQUENCE.incrementAndGet();
        return VECTOR_INDEX_SEQUENCE.get();
    }

    public IndexVectorState vectorState(){
        return this.vectorState;
    }

    public void setVectorState(IndexVectorState vectorState) { this.vectorState = vectorState; }

    // return the sequence index id
    public Id sequenceId(){
        return formatSequenceId(indexLabelId(), this.sequence());
    }

    public Id dirtyLabelId(){
        return formatDirtyLabelId(indexLabelId());
    }

    public static Id formatSequenceId(Id indexLabelId, long sequence){
        // notDirtyPrefix(1byte) + indexlabelId(4byte) + sequence(8byte)
        int length = 1 + 4 + 8;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.write(0);
        buffer.writeInt(SchemaElement.schemaId(indexLabelId));
        buffer.writeLong(sequence);
        return buffer.asId();
    }

    public static HugeVectorIndexMap parseSequenceId(HugeGraph graph, byte[] id){

        final int prefixLength = 1;
        final int labelLength = 4;
        final int sequenceLength = 8;
        BytesBuffer buffer = BytesBuffer.wrap(id);

        E.checkState(id.length == prefixLength + labelLength + sequenceLength,
                     "Invalid sequence " + "index id");
        E.checkState(buffer.read() == 0, "Invalid sequence index id");

        Id label = IdGenerator.of(buffer.readInt());
        IndexLabel indexLabel = IndexLabel.label(graph, label);

        long sequence = buffer.readLong();
        HugeVectorIndexMap indexMap = new HugeVectorIndexMap(graph, indexLabel);
        indexMap.sequence(sequence);
        return indexMap;
    }

    public Id formatDirtyLabelId(Id indexLabelId){
        int length = 1 + 4;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.write(IndexVectorState.DIRTY_PREFIX.code());
        buffer.writeInt(SchemaElement.schemaId(indexLabelId));
        return buffer.asId();
    }


}
