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

package org.apache.hugegraph.unit.vectorIndex;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.vector.VectorRecord;
import org.junit.Test;

public class VectorRecordTest extends BaseUnitTest {

    @Test
    public void testConstructorAndGetters() {
        int vectorId = 42;
        float[] vector = new float[]{1.0f, 2.0f, 3.0f};
        boolean deleted = false;
        long sequence = 100L;

        VectorRecord record = new VectorRecord(vectorId, vector, deleted, sequence);

        Assert.assertEquals(vectorId, record.getVectorId());
        Assert.assertArrayEquals(vector, record.getVectorData(), 0.0001f);
        Assert.assertEquals(deleted, record.isDeleted());
        Assert.assertEquals(sequence, record.getSequence());
    }

    @Test
    public void testDeletedRecord() {
        int vectorId = 99;
        float[] vector = new float[]{0.5f, 0.5f};
        boolean deleted = true;
        long sequence = 200L;

        VectorRecord record = new VectorRecord(vectorId, vector, deleted, sequence);

        Assert.assertTrue(record.isDeleted());
        Assert.assertEquals(vectorId, record.getVectorId());
        Assert.assertEquals(sequence, record.getSequence());
    }

    @Test
    public void testEmptyVector() {
        int vectorId = 1;
        float[] vector = new float[]{};
        boolean deleted = false;
        long sequence = 1L;

        VectorRecord record = new VectorRecord(vectorId, vector, deleted, sequence);

        Assert.assertEquals(0, record.getVectorData().length);
        Assert.assertFalse(record.isDeleted());
    }

    @Test
    public void testHighDimensionVector() {
        int vectorId = 1000;
        int dimension = 128;
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = i * 0.01f;
        }
        boolean deleted = false;
        long sequence = 999999L;

        VectorRecord record = new VectorRecord(vectorId, vector, deleted, sequence);

        Assert.assertEquals(dimension, record.getVectorData().length);
        Assert.assertEquals(0.0f, record.getVectorData()[0], 0.0001f);
        Assert.assertEquals(1.27f, record.getVectorData()[127], 0.0001f);
    }

    @Test
    public void testNullVector() {
        int vectorId = 1;
        float[] vector = null;
        boolean deleted = false;
        long sequence = 1L;

        VectorRecord record = new VectorRecord(vectorId, vector, deleted, sequence);

        Assert.assertNull(record.getVectorData());
    }

    @Test
    public void testZeroSequence() {
        VectorRecord record = new VectorRecord(0, new float[]{1.0f}, false, 0L);

        Assert.assertEquals(0, record.getVectorId());
        Assert.assertEquals(0L, record.getSequence());
    }

    @Test
    public void testNegativeSequence() {
        VectorRecord record = new VectorRecord(1, new float[]{1.0f}, false, -1L);

        Assert.assertEquals(-1L, record.getSequence());
    }

    @Test
    public void testMaxSequence() {
        VectorRecord record = new VectorRecord(1, new float[]{1.0f}, false, Long.MAX_VALUE);

        Assert.assertEquals(Long.MAX_VALUE, record.getSequence());
    }
}

