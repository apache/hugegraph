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

package org.apache.hugegraph.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class SchemaCoreTest extends BaseCoreTest {

    /**
     * Utils method to init some property keys
     */
    protected void initPropertyKeys() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("weight").asDouble().create();
        schema.propertyKey("tags").asText().valueSet().create();
        schema.propertyKey("score").asInt().valueSet().create();
    }

    protected void assertVLEqual(String label, Id id) {
        VertexLabel vertexLabel = graph().vertexLabel(label);
        Assert.assertEquals(id, vertexLabel.id());
    }

    protected void assertELEqual(String label, Id id) {
        EdgeLabel edgeLabel = graph().edgeLabel(label);
        Assert.assertEquals(id, edgeLabel.id());
    }

    protected void assertContainsPk(Collection<Id> ids, String... keys) {
        for (String key : keys) {
            PropertyKey pkey = graph().propertyKey(key);
            Assert.assertTrue(ids.contains(pkey.id()));
        }
    }

    protected void assertNotContainsPk(Collection<Id> ids, String... keys) {
        for (String key : keys) {
            Assert.assertNull(graph().existsPropertyKey(key));
        }
    }

    protected void assertContainsIl(Collection<Id> ids, String... labels) {
        for (String label : labels) {
            IndexLabel indexLabel = graph().indexLabel(label);
            Assert.assertTrue(ids.contains(indexLabel.id()));
        }
    }

    protected void assertNotContainsIl(Collection<Id> ids, String... labels) {
        for (String label : labels) {
            Assert.assertFalse(graph().existsIndexLabel(label));
        }
    }

    protected void initDataWithoutLabelIndex() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        initPropertyKeys();

        schema.propertyKey("date")
              .asDate()
              .ifNotExist()
              .create();

        schema.vertexLabel("reader").properties("name", "city", "age")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .ifNotExist()
              .create();

        schema.indexLabel("readerByCity")
              .onV("reader")
              .by("city")
              .secondary()
              .ifNotExist()
              .create();

        schema.vertexLabel("book").properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .ifNotExist()
              .create();

        schema.edgeLabel("read")
              .sourceLabel("reader")
              .targetLabel("book")
              .properties("date")
              .enableLabelIndex(false)
              .ifNotExist()
              .create();

        schema.indexLabel("readByDate")
              .onE("read")
              .by("date")
              .range()
              .ifNotExist()
              .create();

        String[] cities = {"Beijing Haidian", "Beijing Chaoyang",
                           "Shanghai", "Nanjing", "Hangzhou"};
        List<Vertex> sources = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            String city = cities[i / 10];
            sources.add(graph.addVertex(T.label, "reader",
                                        "name", "source" + i,
                                        "city", city, "age", 21));
        }

        List<Vertex> targets = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            targets.add(graph.addVertex(T.label, "book",
                                        "name", "java-book" + i));
        }

        String[] dates = {"2019-12-30 11:00:00", "2019-12-30 12:00:00",
                          "2019-12-30 13:00:00", "2019-12-30 14:00:00",
                          "2019-12-30 15:00:00"};
        for (int i = 0; i < 50; i++) {
            String date = dates[i / 10];
            sources.get(i).addEdge("read", targets.get(i), "date", date);
        }

        graph.tx().commit();
    }
}
