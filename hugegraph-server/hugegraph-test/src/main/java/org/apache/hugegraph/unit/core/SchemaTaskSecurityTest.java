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

package org.apache.hugegraph.unit.core;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.tx.SchemaTransactionV2;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SchemaTaskSecurityTest {

    @Test
    public void testV2SchemaTasksCanCreateWorkerFromGremlin() throws Exception {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        SchemaTransactionV2 transaction = Mockito.mock(
                SchemaTransactionV2.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(graph).when(transaction).graph();
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Id taskId = IdGenerator.of(1L);
        Mockito.when(graph.getNextId(Mockito.any())).thenReturn(taskId);
        Mockito.when(graph.option(CoreOptions.TASK_SYNC_DELETION)).thenReturn(false);
        AtomicInteger workers = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            // Exercise the cold executor's thread-group and thread checks,
            // without starting a worker or running a background schema job.
            Thread worker = Executors.defaultThreadFactory().newThread(() -> { });
            Assert.assertEquals(Thread.State.NEW, worker.getState());
            workers.incrementAndGet();
            return null;
        }).when(scheduler).schedule(Mockito.any());
        SimpleBindings bindings = new SimpleBindings();
        bindings.put("tx", transaction);
        Id schemaId = IdGenerator.of(2L);
        IndexLabel index = new IndexLabel(graph, schemaId, "by_age");
        Mockito.doReturn(new VertexLabel(graph, schemaId, "person"))
               .when(transaction).getVertexLabel(schemaId);
        Mockito.doReturn(new EdgeLabel(graph, schemaId, "knows"))
               .when(transaction).getEdgeLabel(schemaId);
        Mockito.doReturn(index).when(transaction).getIndexLabel(schemaId);
        bindings.put("index", index);
        bindings.put("schemaId", schemaId);
        String[] scripts = {"tx.rebuildIndex(index)", "tx.removeVertexLabel(schemaId)",
                            "tx.removeEdgeLabel(schemaId)", "tx.removeIndexLabel(schemaId)"};
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        // Load Groovy/mock metadata before restricting filesystem access.
        // Each schedule call still has to create a fresh worker Thread.
        for (String script : scripts) {
            engine.eval(script, bindings);
        }
        engine.eval("new Thread()");
        engine.eval("System.getProperties()");
        workers.set(0);
        SecurityManager previous = System.getSecurityManager();
        String name = Thread.currentThread().getName();
        Thread.currentThread().setName("gremlin-server-exec-schema-security-test");
        System.setSecurityManager(new HugeSecurityManager());
        try {
            for (String script : scripts) {
                Assert.assertEquals(taskId, engine.eval(script, bindings));
            }
            Assert.assertEquals(scripts.length, workers.get());
            ScriptException denied = Assert.assertThrows(ScriptException.class,
                    () -> engine.eval("new Thread()"));
            Assert.assertTrue(denied.getMessage().contains(
                    "Not allowed to access thread group via Gremlin"));
            denied = Assert.assertThrows(ScriptException.class,
                    () -> engine.eval("System.getProperties()"));
            Assert.assertTrue(denied.getMessage().contains(
                    "Not allowed to access system properties via Gremlin"));
        } finally {
            System.setSecurityManager(previous);
            Thread.currentThread().setName(name);
        }
    }
}
