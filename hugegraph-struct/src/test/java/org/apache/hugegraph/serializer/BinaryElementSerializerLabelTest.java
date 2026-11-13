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

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.define.Directions;
import org.junit.Assert;
import org.junit.Test;

public class BinaryElementSerializerLabelTest {

    private final BinaryElementSerializer serializer = BinaryElementSerializer.getInstance();

    @Test
    public void testParseLabelFromVertexColumnReadsLeadingLabelId() {
        byte[] value = new BytesBuffer().writeId(IdGenerator.of(7L))
                                        .writeId(IdGenerator.of(99L))
                                        .bytes();
        BackendColumn col = BackendColumn.of(new byte[]{1}, value);

        Assert.assertEquals(IdGenerator.of(7L), this.serializer.parseLabelFromCol(col, true));
    }

    @Test
    public void testParseLabelFromEdgeColumnReadsSubLabelId() {
        EdgeId id = new EdgeId(IdGenerator.of("alice"), Directions.OUT, IdGenerator.of(5L),
                               IdGenerator.of(6L), "", IdGenerator.of("bob"));
        byte[] name = new BytesBuffer().writeEdgeId(id).bytes();
        BackendColumn col = BackendColumn.of(name, null);

        Assert.assertEquals(IdGenerator.of(6L), this.serializer.parseLabelFromCol(col, false));
    }

    @Test
    public void testParseLabelFromEdgeColumnWithoutTypeFails() {
        byte[] name = new BytesBuffer().writeId(IdGenerator.of("alice")).bytes();
        BackendColumn col = BackendColumn.of(name, null);

        Assert.assertThrows(IllegalStateException.class,
                            () -> this.serializer.parseLabelFromCol(col, false));
    }
}
