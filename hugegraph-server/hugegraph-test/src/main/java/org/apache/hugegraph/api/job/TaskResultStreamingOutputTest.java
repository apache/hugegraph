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

package org.apache.hugegraph.api.job;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.task.TaskResultStreamException;
import org.apache.hugegraph.task.TaskResultStreamException.Reason;
import org.apache.hugegraph.testutil.Assert;
import org.glassfish.grizzly.Connection;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

public class TaskResultStreamingOutputTest {

    private static final String METRIC_PREFIX =
            "org.apache.hugegraph.api.job.TaskAPI.task-result-stream-";

    @Test
    public void testApplyTransportTimeoutAndRecordSuccess() throws Exception {
        Connection<?> connection = connection(5000L);
        Semaphore limiter = new Semaphore(0);
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long active = counter("active").getCount();
        long success = meter("success").getCount();
        long written = histogram("written-bytes").getCount();
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection, committed(true), 1, limiter, trace, 17,
                (output, deadline) -> {
                    Mockito.verify(connection).setWriteTimeout(
                            1000L, TimeUnit.MILLISECONDS);
                    Assert.assertEquals(active + 1L,
                                        counter("active").getCount());
                    output.write("ok".getBytes(StandardCharsets.UTF_8));
                });

        streaming.write(target);

        Assert.assertEquals("ok", target.toString(StandardCharsets.UTF_8));
        Assert.assertEquals(1, limiter.availablePermits());
        Assert.assertEquals(active, counter("active").getCount());
        Assert.assertEquals(success + 1L, meter("success").getCount());
        Assert.assertEquals(written + 1L,
                            histogram("written-bytes").getCount());
        Mockito.verify(connection).setWriteTimeout(
                5000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRefreshTransportTimeoutWithRemainingDeadline()
            throws Exception {
        Connection<?> connection = connection(5000L);
        Semaphore limiter = new Semaphore(0);
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection, committed(true), 2, limiter, trace(), 17,
                (output, deadline) -> {
                    try {
                        Thread.sleep(250L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted before writing", e);
                    }
                    output.write(1);
                });

        streaming.write(new ByteArrayOutputStream());

        ArgumentCaptor<Long> timeouts = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(connection, Mockito.atLeast(3)).setWriteTimeout(
                timeouts.capture(), Mockito.eq(TimeUnit.MILLISECONDS));
        Assert.assertTrue(timeouts.getAllValues().stream().anyMatch(
                timeout -> timeout > 0L && timeout < 1900L));
        Assert.assertEquals(1, limiter.availablePermits());
        Mockito.verify(connection).setWriteTimeout(
                5000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testClassifyClientDisconnectAndReleasePermit() {
        Semaphore limiter = new Semaphore(0);
        Connection<?> connection = connection(5000L);
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long active = counter("active").getCount();
        long disconnected = meter("disconnected").getCount();
        long postCommit = meter("postcommit-failed").getCount();
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection, committed(true), 1, limiter, trace, 17,
                (output, deadline) -> output.write(1));

        Assert.assertThrows(IOException.class,
                            () -> streaming.write(failingOutput()));

        Assert.assertEquals(1, limiter.availablePermits());
        Assert.assertEquals(active, counter("active").getCount());
        Assert.assertEquals(disconnected + 1L,
                            meter("disconnected").getCount());
        Assert.assertEquals(postCommit + 1L,
                            meter("postcommit-failed").getCount());
        Mockito.verify(connection).setWriteTimeout(
                5000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testClassifyTransportWriteTimeoutBeforeDeadline() {
        Semaphore limiter = new Semaphore(0);
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long timeout = meter("timeout").getCount();
        long disconnected = meter("disconnected").getCount();
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection(100L), committed(true), 1, limiter, trace, 17,
                (output, deadline) -> output.write(1));

        Assert.assertThrows(IOException.class,
                            () -> streaming.write(timedOutOutput()));

        Assert.assertEquals(timeout + 1L, meter("timeout").getCount());
        Assert.assertEquals(disconnected,
                            meter("disconnected").getCount());
        Assert.assertEquals(1, limiter.availablePermits());
    }

    @Test
    public void testClassifyFailureBeforeCommitAndReleasePermit() {
        Semaphore limiter = new Semaphore(0);
        Connection<?> connection = connection(5000L);
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long active = counter("active").getCount();
        long timeout = meter("timeout").getCount();
        long preCommit = meter("precommit-failed").getCount();
        long postCommit = meter("postcommit-failed").getCount();
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection, committed(false), 1, limiter, trace, 17,
                (output, deadline) -> {
                    throw new TaskResultStreamException(
                            Reason.TIMEOUT, "expected timeout");
                });

        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> streaming.write(new ByteArrayOutputStream()));

        Assert.assertEquals(Reason.TIMEOUT, exception.reason());
        Assert.assertEquals(1, limiter.availablePermits());
        Assert.assertEquals(active, counter("active").getCount());
        Assert.assertEquals(timeout + 1L, meter("timeout").getCount());
        Assert.assertEquals(preCommit + 1L,
                            meter("precommit-failed").getCount());
        Assert.assertEquals(postCommit,
                            meter("postcommit-failed").getCount());
        Mockito.verify(connection).setWriteTimeout(
                5000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testContainerCommitStateOverridesWrittenByteCount() {
        Semaphore limiter = new Semaphore(0);
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long preCommit = meter("precommit-failed").getCount();
        long postCommit = meter("postcommit-failed").getCount();
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection(5000L), committed(false), 1, limiter, trace, 17,
                (output, deadline) -> {
                    output.write(1);
                    throw new TaskResultStreamException(
                            Reason.INVALID_JSON, "expected failure");
                });

        Assert.assertThrows(TaskResultStreamException.class,
                            () -> streaming.write(
                                    new ByteArrayOutputStream()));

        Assert.assertEquals(preCommit + 1L,
                            meter("precommit-failed").getCount());
        Assert.assertEquals(postCommit,
                            meter("postcommit-failed").getCount());
        Assert.assertEquals(1, limiter.availablePermits());
    }

    @Test
    public void testRecordPreCommitFailureSeparately() {
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long preCommit = meter("precommit-failed").getCount();
        long postCommit = meter("postcommit-failed").getCount();

        trace.preCommitFailure(new IllegalArgumentException("invalid"));

        Assert.assertEquals(preCommit + 1L,
                            meter("precommit-failed").getCount());
        Assert.assertEquals(postCommit,
                            meter("postcommit-failed").getCount());
    }

    @Test
    public void testRecordUnexpectedErrorAndReleasePermit() {
        Semaphore limiter = new Semaphore(0);
        TaskResultStreamMetrics.RequestTrace trace = trace();
        long active = counter("active").getCount();
        long postCommit = meter("postcommit-failed").getCount();
        TaskResultStreamingOutput streaming = new TaskResultStreamingOutput(
                connection(5000L), committed(true), 1, limiter, trace, 17,
                (output, deadline) -> {
                    throw new AssertionError("unexpected");
                });

        Assert.assertThrows(AssertionError.class,
                            () -> streaming.write(
                                    new ByteArrayOutputStream()));

        Assert.assertEquals(1, limiter.availablePermits());
        Assert.assertEquals(active, counter("active").getCount());
        Assert.assertEquals(postCommit + 1L,
                            meter("postcommit-failed").getCount());
    }

    @SuppressWarnings("unchecked")
    private static Connection<?> connection(long writeTimeoutMillis) {
        Connection<?> connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getWriteTimeout(TimeUnit.MILLISECONDS))
               .thenReturn(writeTimeoutMillis);
        return connection;
    }

    private static TaskResultStreamMetrics.RequestTrace trace() {
        TaskResultStreamMetrics.RequestTrace trace =
                TaskResultStreamMetrics.request(
                        "DEFAULT", "hugegraph", 123L,
                        TaskResultStreamMetrics.Mode.COMPLETE);
        trace.backend("memory");
        trace.offset(0L);
        return trace;
    }

    private static java.util.function.BooleanSupplier committed(
            boolean committed) {
        return () -> committed;
    }

    private static OutputStream failingOutput() {
        return new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw new IOException("client disconnected");
            }
        };
    }

    private static OutputStream timedOutOutput() {
        return new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw new IOException(
                        "Write timeout exceeded when trying to flush the data");
            }
        };
    }

    private static Counter counter(String name) {
        return MetricsUtil.REGISTRY.getCounters().get(METRIC_PREFIX + name);
    }

    private static Meter meter(String name) {
        return MetricsUtil.REGISTRY.getMeters().get(METRIC_PREFIX + name);
    }

    private static Histogram histogram(String name) {
        return MetricsUtil.REGISTRY.getHistograms()
                                  .get(METRIC_PREFIX + name);
    }
}
