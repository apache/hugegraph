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

package org.apache.hugegraph.serializer;

import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.junit.Assert;
import org.junit.Test;

public class BinaryElementSerializerEdgeLabelTest {

    private static EdgeId edgeId(long label, long subLabel) {
        return new EdgeId(IdGenerator.of(1L), Directions.OUT, IdGenerator.of(label),
                          IdGenerator.of(subLabel), "", IdGenerator.of(2L));
    }

    @Test
    public void testEdgeLabelWithoutSchemaKeepsNormalLabel() {
        EdgeLabel label = BinaryElementSerializer.getInstance()
                                                 .edgeLabel(null, edgeId(5L, 5L), null);

        Assert.assertEquals(IdGenerator.of(5L), label.id());
        Assert.assertFalse(label.hasFather());
        Assert.assertEquals(EdgeLabelType.NORMAL, label.edgeLabelType());
    }

    @Test
    public void testEdgeLabelWithoutSchemaMarksSubLabel() {
        EdgeLabel label = BinaryElementSerializer.getInstance()
                                                 .edgeLabel(null, edgeId(5L, 6L), null);

        Assert.assertEquals(IdGenerator.of(6L), label.id());
        Assert.assertTrue(label.hasFather());
        Assert.assertEquals(IdGenerator.of(5L), label.fatherId());
        Assert.assertEquals(EdgeLabelType.SUB, label.edgeLabelType());
    }
}
