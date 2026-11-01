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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.LoadingMode;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assert;
import org.junit.Test;

public class LoadingModeTest {

    @Test
    public void testOwnerAuditRecordsOnlySuccessfulChangesWithoutTokens() {
        List<LogEvent> events = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("LoadingModeTestAppender", null, null,
                                                        true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration configuration = context.getConfiguration();
        String loggerName = LoadingMode.class.getName();
        LoggerConfig previous = configuration.getLoggers().get(loggerName);
        LoggerConfig logger = new LoggerConfig(loggerName, Level.INFO, false);
        logger.addAppender(appender, Level.INFO, null);
        configuration.addLogger(loggerName, logger);
        context.updateLoggers();
        try {
            AtomicReference<String> graphName = new AtomicReference<>("test-space-test-graph");
            LoadingMode mode = new LoadingMode(graphName::get);
            Map<String, Object> capability = mode.capability();
            String first = token();
            String second = token();
            update(mode, capability, first, "acquire");
            update(mode, capability, first, "acquire");
            update(mode, capability, second, "acquire");
            Assert.assertThrows(LoadingMode.ConflictException.class,
                                () -> mode.update("release", (String) capability.get("instance"),
                                                  0, first, "other-user"));
            update(mode, capability, first, "release");
            update(mode, capability, first, "release");
            update(mode, capability, second, "release");
            update(mode, capability, first, "acquire");
            Assert.assertEquals(4, events.size());
            Assert.assertTrue(message(events, 0).contains("mode NONE -> LOADING, epoch 0, activeOwners 1"));
            Assert.assertTrue(message(events, 1).contains("mode LOADING -> LOADING, epoch 0, activeOwners 2"));
            Assert.assertTrue(message(events, 2).contains("activeOwners 1, clearedOwners 1"));
            Assert.assertTrue(message(events, 3).contains("mode LOADING -> NONE, epoch 0, activeOwners 0"));
            update(mode, capability, token(), "acquire");
            update(mode, capability, token(), "acquire");
            Map<String, Object> current = mode.capability();
            mode.recover((String) current.get("instance"), 0, ((Number) current.get("revision")).longValue());
            Assert.assertTrue(message(events, 6).contains("mode LOADING -> NONE, epoch 1, " +
                                                        "activeOwners 0, clearedOwners 2"));
            Assert.assertThrows(LoadingMode.ConflictException.class,
                                () -> mode.recover((String) capability.get("instance"), 0, 0));
            current = mode.capability();
            mode.recover((String) current.get("instance"), 1, ((Number) current.get("revision")).longValue());
            Assert.assertEquals(8, events.size());
            Assert.assertTrue(message(events, 7).contains("mode NONE -> NONE, epoch 2, " +
                                                        "activeOwners 0, clearedOwners 0"));
            for (LogEvent event : events) {
                Assert.assertEquals(Level.INFO, event.getLevel());
                Assert.assertTrue(event.getMessage().getFormattedMessage().contains(
                        "graph 'test-space-test-graph' instance " + capability.get("instance")));
                Assert.assertFalse(event.getMessage().getFormattedMessage().contains(first));
                Assert.assertFalse(event.getMessage().getFormattedMessage().contains(second));
                Assert.assertFalse(event.getMessage().getFormattedMessage().contains("other-user"));
            }
            graphName.set("renamed-space-test-graph");
            recover(mode, mode.capability());
            Assert.assertEquals(9, events.size());
            Assert.assertTrue(message(events, 8).contains("graph 'renamed-space-test-graph' instance " +
                                                        capability.get("instance")));
        } finally {
            configuration.removeLogger(loggerName);
            if (previous != null) {
                configuration.addLogger(loggerName, previous);
            }
            context.updateLoggers();
            appender.stop();
        }
    }

    private static String message(List<LogEvent> events, int index) {
        return events.get(index).getMessage().getFormattedMessage();
    }

    @Test
    public void testTwoOwnersAndIdempotentRetry() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String a = token();
        String b = token();
        Assert.assertEquals("active", update(mode, capability, a, "acquire").get("state"));
        update(mode, capability, a, "acquire");
        update(mode, capability, b, "acquire");
        update(mode, capability, a, "release");
        update(mode, capability, a, "release");
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        Assert.assertEquals("released", update(mode, capability, a, "acquire").get("state"));
        update(mode, capability, b, "release");
        Assert.assertEquals(GraphMode.NONE, mode.mode());
    }

    @Test
    public void testUnknownReleaseCancelsLateAcquire() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String token = token();
        update(mode, capability, token, "release");
        Assert.assertEquals("released", update(mode, capability, token, "acquire").get("state"));
        Assert.assertEquals(GraphMode.NONE, mode.mode());
    }

    @Test
    public void testTerminalCompactionKeepsOldActiveAndRejectsLateAcquire() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> original = mode.capability();
        String b = token();
        String cancelled = token();
        update(mode, original, b, "acquire");
        update(mode, original, cancelled, "release");
        for (int i = 0; i < 2200; i++) {
            update(mode, mode.capability(), token(), "release");
        }
        Assert.assertTrue(((Number) mode.capability().get("epoch")).longValue() >= 2);
        Assert.assertEquals("released", update(mode, original, cancelled, "acquire").get("state"));
        Assert.assertEquals("released", update(mode, original, token(), "acquire").get("state"));
        Assert.assertEquals("active", update(mode, original, b, "acquire").get("state"));
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        String c = token();
        Map<String, Object> current = mode.capability();
        update(mode, current, c, "acquire");
        update(mode, original, b, "release");
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        update(mode, current, c, "release");
        Assert.assertEquals(GraphMode.NONE, mode.mode());
    }

    @Test
    public void testLegacySetterCannotOverwriteOwnersOrAdoptManualMode() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String a = token();
        for (GraphMode manual : GraphMode.values()) {
            if (manual == GraphMode.NONE) {
                continue;
            }
            mode.mode(manual);
            Assert.assertThrows(IllegalStateException.class, () -> update(mode, capability, a, "acquire"));
            Assert.assertEquals(manual, mode.mode());
        }
        mode.mode(GraphMode.NONE);
        update(mode, capability, a, "acquire");
        for (GraphMode legacy : GraphMode.values()) {
            Assert.assertThrows(IllegalStateException.class, () -> mode.mode(legacy));
            Assert.assertEquals(GraphMode.LOADING, mode.mode());
        }
        update(mode, capability, a, "release");
        mode.mode(GraphMode.NONE);
        Assert.assertThrows(NullPointerException.class, () -> mode.mode(null));
    }

    @Test
    public void testInstanceAndPrincipalBoundaries() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String a = token();
        update(mode, capability, a, "acquire");
        String instance = (String) capability.get("instance");
        Assert.assertThrows(IllegalStateException.class,
                            () -> mode.update("release", instance, 0, a, "another-user"));
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> mode.update("acquire", token(), 0, a, "user"));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> mode.update("acquire", instance, 1, a, "user"));
        update(mode, capability, a, "release");
    }

    @Test
    public void testRecoveryRetiresOwnersAndLateRequests() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> before = mode.capability();
        String lost = token();
        update(mode, before, lost, "acquire");

        Map<String, Object> recovered = recover(mode, mode.capability());
        Assert.assertEquals("recovered", recovered.get("state"));
        Assert.assertEquals(1L, recovered.get("epoch"));
        Assert.assertFalse(recovered.containsKey("token"));
        Assert.assertEquals(GraphMode.NONE, mode.mode());
        Assert.assertEquals("released", update(mode, before, lost, "acquire").get("state"));
        Assert.assertEquals("released", update(mode, before, token(), "acquire").get("state"));
        update(mode, before, lost, "release");

        Map<String, Object> current = mode.capability();
        update(mode, current, token(), "acquire");
        Assert.assertThrows(IllegalStateException.class,
                            () -> recover(mode, before));
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        Assert.assertEquals(1L, mode.capability().get("epoch"));
    }

    @Test
    public void testRecoveryCannotClearOwnerAddedAfterInspection() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String first = token();
        String second = token();
        update(mode, capability, first, "acquire");
        Map<String, Object> inspected = mode.capability();
        update(mode, inspected, second, "acquire");
        Assert.assertThrows(IllegalStateException.class,
                            () -> recover(mode, inspected));
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        update(mode, capability, first, "release");
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        update(mode, capability, second, "release");
        Assert.assertEquals(GraphMode.NONE, mode.mode());
    }

    @Test
    public void testRecoveryAllowsConfirmedManualModeReset() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        mode.mode(GraphMode.RESTORING);
        Map<String, Object> before = mode.capability();
        recover(mode, before);
        Assert.assertEquals(GraphMode.NONE, mode.mode());
        Assert.assertEquals("active", update(mode, mode.capability(), token(), "acquire").get("state"));
    }

    @Test
    public void testSuccessfulSameModeWriteInvalidatesRecoverySnapshot() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        for (GraphMode manual : GraphMode.values()) {
            mode.mode(manual);
            Map<String, Object> inspected = mode.capability();
            mode.mode(manual);
            Assert.assertEquals(((Number) inspected.get("revision")).longValue() + 1,
                                mode.capability().get("revision"));
            Assert.assertThrows(LoadingMode.ConflictException.class, () -> recover(mode, inspected));
            Assert.assertEquals(manual, mode.mode());
        }
    }

    @Test
    public void testOwnerAdmissionLimitPreservesExistingOwnersAndRecovery() {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String first = token();
        update(mode, capability, first, "acquire");
        for (int i = 1; i < 1024; i++) {
            update(mode, capability, token(), "acquire");
        }
        Map<String, Object> full = mode.capability();
        String rejected = token();
        LoadingMode.ConflictException failure = Assert.assertThrows(LoadingMode.ConflictException.class,
                () -> update(mode, capability, rejected, "acquire"));
        Assert.assertTrue(failure.getMessage().contains("1024"));
        Assert.assertEquals(full, mode.capability());
        Assert.assertEquals(GraphMode.LOADING, mode.mode());
        Assert.assertEquals("active", update(mode, capability, first, "acquire").get("state"));
        Assert.assertEquals(full, mode.capability());
        Assert.assertEquals("released", update(mode, capability, first, "release").get("state"));
        Assert.assertEquals("active", update(mode, capability, rejected, "acquire").get("state"));
        Map<String, Object> recovered = recover(mode, mode.capability());
        Assert.assertEquals(GraphMode.NONE, recovered.get("mode"));
        Assert.assertEquals("active", update(mode, mode.capability(), token(), "acquire").get("state"));
    }

    @Test
    public void testConcurrentAcquireAndLegacySetterAreAtomic() throws Exception {
        LoadingMode mode = new LoadingMode(() -> "test-space-test-graph");
        Map<String, Object> capability = mode.capability();
        String a = token();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> acquire = executor.submit(() -> {
                Assert.assertTrue(start.await(5, TimeUnit.SECONDS));
                try {
                    update(mode, capability, a, "acquire");
                    return true;
                } catch (IllegalStateException expected) {
                    return false;
                }
            });
            Future<Boolean> legacy = executor.submit(() -> {
                Assert.assertTrue(start.await(5, TimeUnit.SECONDS));
                try {
                    mode.mode(GraphMode.RESTORING);
                    return true;
                } catch (IllegalStateException expected) {
                    return false;
                }
            });
            start.countDown();
            boolean acquired = acquire.get(10, TimeUnit.SECONDS);
            Assert.assertNotEquals(acquired, legacy.get(10, TimeUnit.SECONDS));
            Assert.assertEquals(acquired ? GraphMode.LOADING : GraphMode.RESTORING, mode.mode());
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static String token() {
        return UUID.randomUUID().toString();
    }

    private static Map<String, Object> recover(LoadingMode mode, Map<String, Object> capability) {
        return mode.recover((String) capability.get("instance"),
                            ((Number) capability.get("epoch")).longValue(),
                            ((Number) capability.get("revision")).longValue());
    }

    private static Map<String, Object> update(LoadingMode mode, Map<String, Object> capability,
                                               String token, String action) {
        return mode.update(action, (String) capability.get("instance"),
                           ((Number) capability.get("epoch")).longValue(), token, "user");
    }
}
