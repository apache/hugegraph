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

package org.apache.hugegraph.store.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.node.controller.HgStoreStatusController;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeState;
import org.apache.hugegraph.store.node.listener.StoreProcessLifecycle;
import org.junit.Test;
import org.lognet.springboot.grpc.context.GRpcServerInitializedEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Server;

public class StoreProcessLifecycleTest {

    @Test
    public void endpointReportsOnlyThisProcessLifecycle() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(StoreProcessLifecycle.class);
        context.refresh();
        StoreProcessLifecycle lifecycle = context.getBean(StoreProcessLifecycle.class);
        HgStoreStatusController controller = new HgStoreStatusController(lifecycle);
        ObjectMapper mapper = new ObjectMapper();
        Server server = mock(Server.class);
        HgStoreNodeState.goStarting();
        try {
            JsonNode initial = mapper.valueToTree(controller.getLifecycle());
            assertEquals(0, initial.get("status").asInt());
            assertEquals("STARTING", initial.get("text").asText());
            context.publishEvent(new GRpcServerInitializedEvent(context, server));
            assertEquals("STARTING", controller.getLifecycle().getText());
            context.publishEvent(new ApplicationReadyEvent(new SpringApplication(),
                                                           new String[0], context));
            assertEquals("STARTED", mapper.valueToTree(controller.getLifecycle())
                                          .get("text").asText());
            // The existing state endpoint remains independent of process startup.
            assertEquals("STARTING", controller.getState().getText());
            HgStoreNodeState.goOnline();
            assertEquals("ONLINE", controller.getState().getText());
            assertEquals("STARTED", controller.getLifecycle().getText());
            when(server.isShutdown()).thenReturn(true);
            assertEquals("UNAVAILABLE", controller.getLifecycle().getText());
        } finally {
            HgStoreNodeState.goStarting();
            context.close();
        }
        assertEquals("STOPPING", controller.getLifecycle().getText());
        // Late startup callbacks cannot resurrect a stopping process.
        lifecycle.onGrpcStarted(new GRpcServerInitializedEvent(context, server));
        lifecycle.onApplicationReady(new ApplicationReadyEvent(new SpringApplication(),
                                                               new String[0], context));
        assertEquals("STOPPING", controller.getLifecycle().getText());
    }

    @Test
    public void ignoresEventsFromAnotherContext() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        AnnotationConfigApplicationContext other = new AnnotationConfigApplicationContext();
        try {
            context.register(StoreProcessLifecycle.class);
            context.refresh();
            other.refresh();
            StoreProcessLifecycle lifecycle = context.getBean(StoreProcessLifecycle.class);
            lifecycle.onGrpcStarted(new GRpcServerInitializedEvent(other, mock(Server.class)));
            lifecycle.onApplicationReady(new ApplicationReadyEvent(new SpringApplication(),
                                                                   new String[0], other));
            assertEquals("STARTING", lifecycle.getState());
            other.close();
            assertEquals("STARTING", lifecycle.getState());
        } finally {
            other.close();
            context.close();
        }
    }

    @Test
    public void startedRequiresBothSignalsInEitherOrder() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        try {
            StoreProcessLifecycle lifecycle = new StoreProcessLifecycle(context);
            Server server = mock(Server.class);
            lifecycle.onApplicationReady(new ApplicationReadyEvent(new SpringApplication(),
                                                                   new String[0], context));
            assertEquals("STARTING", lifecycle.getState());
            lifecycle.onGrpcStarted(new GRpcServerInitializedEvent(context, server));
            assertEquals("STARTED", lifecycle.getState());
            when(server.isTerminated()).thenReturn(true);
            assertEquals("UNAVAILABLE", lifecycle.getState());
        } finally {
            context.close();
        }
    }

    @Test
    public void concurrentStartupCannotOverrideShutdown() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                StoreProcessLifecycle lifecycle = new StoreProcessLifecycle(context);
                CountDownLatch start = new CountDownLatch(1);
                Future<?> ready = executor.submit(() -> {
                    start.await();
                    lifecycle.onGrpcStarted(new GRpcServerInitializedEvent(context, mock(Server.class)));
                    lifecycle.onApplicationReady(new ApplicationReadyEvent(new SpringApplication(),
                                                                           new String[0], context));
                    return null;
                });
                Future<?> stop = executor.submit(() -> {
                    start.await();
                    lifecycle.onContextClosed(new ContextClosedEvent(context));
                    return null;
                });
                start.countDown();
                ready.get();
                stop.get();
                assertEquals("STOPPING", lifecycle.getState());
            }
        } finally {
            executor.shutdownNow();
            context.close();
        }
    }

    @Test(timeout = 5000)
    public void stoppingIsVisibleBeforeOtherShutdownListenersFinish() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        CountDownLatch draining = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            context.register(StoreProcessLifecycle.class);
            context.addApplicationListener((ApplicationListener<ContextClosedEvent>) event -> {
                draining.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            context.refresh();
            StoreProcessLifecycle lifecycle = context.getBean(StoreProcessLifecycle.class);
            lifecycle.onGrpcStarted(new GRpcServerInitializedEvent(context, mock(Server.class)));
            lifecycle.onApplicationReady(new ApplicationReadyEvent(new SpringApplication(),
                                                                   new String[0], context));
            Future<?> close = executor.submit(context::close);
            assertTrue(draining.await(2, TimeUnit.SECONDS));
            assertEquals("STOPPING", lifecycle.getState());
            release.countDown();
            close.get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
            context.close();
        }
    }
}
