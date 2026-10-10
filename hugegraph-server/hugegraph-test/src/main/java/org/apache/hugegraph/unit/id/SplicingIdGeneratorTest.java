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

package org.apache.hugegraph.unit.id;

import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.IdStrategy;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class SplicingIdGeneratorTest {

    @Test
    public void testGenerate() {
        VertexLabel vertexLabel = new VertexLabel(null, IdGenerator.of(1L), "fake");
        vertexLabel.idStrategy(IdStrategy.PRIMARY_KEY);
        BaseVertex vertex = new BaseVertex(IdGenerator.of("1:marko"), vertexLabel);
        Id vid = SplicingIdGenerator.instance().generate(vertex);
        Assert.assertEquals(IdGenerator.of("1:marko"), vid);
    }

    @Test
    public void testConcatIds() {
        Assert.assertEquals("a>b>c>d",
                            SplicingIdGenerator.concat("a", "b", "c", "d"));
        Assert.assertEquals("a>`>>c>`>",
                            SplicingIdGenerator.concat("a", ">", "c", ">"));
        Assert.assertEquals("a>b`>c>d",
                            SplicingIdGenerator.concat("a", "b>c", "d"));
    }

    @Test
    public void testSplitIds() {
        Assert.assertArrayEquals(new String[]{"a", "b", "c", "d"},
                                 SplicingIdGenerator.split("a>b>c>d"));
        Assert.assertArrayEquals(new String[]{"a", ">", "c", ">"},
                                 SplicingIdGenerator.split("a>`>>c>`>"));
        Assert.assertArrayEquals(new String[]{"a", "b>c", "d"},
                                 SplicingIdGenerator.split("a>b`>c>d"));
    }

    @Test
    public void testConcatValues() {
        Assert.assertEquals("a!1!c!d",
                            SplicingIdGenerator.concatValues("a", 1, 'c', "d"));
        Assert.assertEquals("a!`!!1!d",
                            SplicingIdGenerator.concatValues("a", "!", 1, 'd'));
        Assert.assertEquals("a!b`!c!d",
                            SplicingIdGenerator.concatValues("a", "b!c", "d"));

        List<Object> values = ImmutableList.of("a", 1, 'c', "d");
        Assert.assertEquals("a!1!c!d",
                            SplicingIdGenerator.concatValues(values));
        values = ImmutableList.of("a", "!", 1, 'd');
        Assert.assertEquals("a!`!!1!d",
                            SplicingIdGenerator.concatValues(values));
        values = ImmutableList.of("a", "b!c", "d");
        Assert.assertEquals("a!b`!c!d",
                            SplicingIdGenerator.concatValues(values));
    }

    @Test
    public void testSplicing() {
        Assert.assertEquals(IdGenerator.of("1:marko"),
                            SplicingIdGenerator.splicing("1", "marko"));
        Assert.assertEquals(IdGenerator.of("book:c:2020"),
                            SplicingIdGenerator.splicing("book", "c", "2020"));
    }

    @Test
    public void testParse() {
        Assert.assertArrayEquals(new String[]{"1", "marko"},
                                 SplicingIdGenerator.parse(
                                         IdGenerator.of("1:marko")));
        Assert.assertArrayEquals(new String[]{"book", "c", "2020"},
                                 SplicingIdGenerator.parse(
                                         IdGenerator.of("book:c:2020")));
    }
}
