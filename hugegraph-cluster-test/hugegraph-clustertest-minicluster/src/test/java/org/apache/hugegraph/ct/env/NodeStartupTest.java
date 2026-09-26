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

package org.apache.hugegraph.ct.env;

import java.util.Arrays;

import org.apache.hugegraph.ct.node.AbstractNodeWrapper;
import org.apache.hugegraph.ct.node.BaseNodeWrapper;
import org.junit.Assert;
import org.junit.Test;

public class NodeStartupTest {

    @Test
    public void testLiveReadyNode() {
        AbstractEnv.awaitStarted(new TestNode(true, true), 100);
    }

    @Test
    public void testExitedNodeCannotPassWithOldReadyMarker() {
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> AbstractEnv.awaitStarted(new TestNode(false, true), 100));
        Assert.assertTrue(failure.getMessage().contains("process exited"));
        Assert.assertTrue(failure.getMessage().contains("test-node"));
        Assert.assertTrue(failure.getMessage().contains("test-start.log"));
    }

    @Test(timeout = 5000)
    public void testMissingReadyMarkerTimesOut() {
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> AbstractEnv.awaitStarted(new TestNode(true, false), 10));
        Assert.assertTrue(failure.getMessage().contains("timed out"));
    }

    @Test
    public void testInterruptIsPreserved() {
        Thread.currentThread().interrupt();
        try {
            IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                    () -> AbstractEnv.awaitStarted(new TestNode(true, false), 100));
            Assert.assertTrue(failure.getCause() instanceof InterruptedException);
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testCleanupContinuesAfterFailureAndPreservesDirectories() {
        StopNode first = new StopNode(true);
        StopNode second = new StopNode(false);
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> AbstractEnv.stopNodes(Arrays.asList(first, second), false));
        Assert.assertEquals(1, failure.getSuppressed().length);
        Assert.assertTrue(first.stopped);
        Assert.assertTrue(second.stopped);
        Assert.assertFalse(first.deleteData);
        Assert.assertFalse(second.deleteData);
    }

    @Test
    public void testCleanupPreservesInterruptAfterStoppingEveryNode() {
        StopNode first = new StopNode(false);
        StopNode second = new StopNode(false);
        Thread.currentThread().interrupt();
        try {
            AbstractEnv.stopNodes(Arrays.asList(first, second), false);
            Assert.assertTrue(first.stopped);
            Assert.assertTrue(second.stopped);
            Assert.assertFalse(first.interruptedAtStop);
            Assert.assertFalse(second.interruptedAtStop);
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private static class StopNode extends AbstractNodeWrapper {

        private final boolean fail;
        private boolean stopped;
        private boolean deleteData;
        private boolean interruptedAtStop;

        StopNode(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getID() {
            return "stop-node";
        }

        @Override
        public void stop(boolean deleteData) {
            this.stopped = true;
            this.deleteData = deleteData;
            this.interruptedAtStop = Thread.currentThread().isInterrupted();
            if (this.fail) {
                throw new IllegalStateException("test stop failure");
            }
        }
    }

    private static class TestNode implements BaseNodeWrapper {

        private final boolean alive;
        private final boolean ready;

        TestNode(boolean alive, boolean ready) {
            this.alive = alive;
            this.ready = ready;
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stop() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAlive() {
            return this.alive;
        }

        @Override
        public boolean isStarted() {
            return this.ready;
        }

        @Override
        public String getID() {
            return "test-node";
        }

        @Override
        public String getNodePath() {
            return "test-node/";
        }

        @Override
        public String getLogPath() {
            return "test-node/test-start.log";
        }

        @Override
        public void updateWorkPath(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateConfigPath(String path) {
            throw new UnsupportedOperationException();
        }
    }
}
