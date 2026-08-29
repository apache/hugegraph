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

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.zip.GZIPOutputStream;

import org.apache.hugegraph.task.TaskResultStreamException;
import org.apache.hugegraph.task.TaskResultStreamException.Reason;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.glassfish.grizzly.Connection;
import org.slf4j.Logger;

import jakarta.ws.rs.core.StreamingOutput;

final class TaskResultStreamingOutput implements StreamingOutput {

    private static final Logger LOG =
            Log.logger(TaskResultStreamingOutput.class);
    private static final String GRIZZLY_WRITE_TIMEOUT_MESSAGE =
            "Write timeout exceeded when trying to flush the data";

    private final Connection<?> connection;
    private final BooleanSupplier responseCommitted;
    private final int timeoutSeconds;
    private final long requestDeadlineNanos;
    private final Semaphore limiter;
    private final TaskResultStreamMetrics.RequestTrace requestTrace;
    private final int compressedBytes;
    private final Writer writer;

    TaskResultStreamingOutput(
            Connection<?> connection, BooleanSupplier responseCommitted,
            int timeoutSeconds, Semaphore limiter,
            TaskResultStreamMetrics.RequestTrace requestTrace,
            int compressedBytes, Writer writer) {
        this(connection, responseCommitted, timeoutSeconds, Long.MAX_VALUE,
             limiter, requestTrace, compressedBytes, writer);
    }

    TaskResultStreamingOutput(
            Connection<?> connection, BooleanSupplier responseCommitted,
            int timeoutSeconds, long requestDeadlineNanos,
            Semaphore limiter,
            TaskResultStreamMetrics.RequestTrace requestTrace,
            int compressedBytes, Writer writer) {
        E.checkArgument(timeoutSeconds > 0,
                        "The stream timeout must be positive");
        E.checkNotNull(limiter, "task result stream limiter");
        E.checkNotNull(requestTrace, "task result request trace");
        E.checkNotNull(writer, "task result stream writer");
        E.checkArgument(compressedBytes >= 0,
                        "The compressed bytes must not be negative");
        this.connection = connection;
        this.responseCommitted = responseCommitted;
        this.timeoutSeconds = timeoutSeconds;
        this.requestDeadlineNanos = requestDeadlineNanos;
        this.limiter = limiter;
        this.requestTrace = requestTrace;
        this.compressedBytes = compressedBytes;
        this.writer = writer;
    }

    @Override
    public void write(OutputStream output) throws IOException {
        long deadlineNanos = Math.min(deadline(this.timeoutSeconds),
                                      this.requestDeadlineNanos);
        TaskResultStreamMetrics.StreamTrace streamTrace =
                this.requestTrace.stream(this.compressedBytes);
        ClientOutputStream clientOutput =
                new ClientOutputStream(output, deadlineNanos);
        WriteTimeoutScope timeout = WriteTimeoutScope.NONE;
        try {
            checkDeadline(deadlineNanos);
            timeout = WriteTimeoutScope.open(this.connection,
                                             this.timeoutSeconds);
            clientOutput.timeout(timeout);
            this.writer.write(clientOutput, deadlineNanos);
            checkDeadline(deadlineNanos);
            clientOutput.finish();
            streamTrace.success(clientOutput.count());
        } catch (ClientWriteException e) {
            TaskResultStreamMetrics.Stage stage =
                    this.stage(clientOutput.count());
            TaskResultStreamMetrics.Outcome outcome =
                    deadlineReached(deadlineNanos) ||
                    isTransportTimeout(e) ?
                    TaskResultStreamMetrics.Outcome.TIMEOUT :
                    TaskResultStreamMetrics.Outcome.DISCONNECTED;
            streamTrace.failure(stage, outcome,
                                 e, clientOutput.count(),
                                 this.compressedBytes);
            this.abortConnection(stage);
            throw e;
        } catch (TaskResultStreamException e) {
            TaskResultStreamMetrics.Stage stage =
                    this.stage(clientOutput.count());
            TaskResultStreamMetrics.Outcome outcome =
                    e.reason() == Reason.TIMEOUT ?
                    TaskResultStreamMetrics.Outcome.TIMEOUT :
                    TaskResultStreamMetrics.Outcome.FAILURE;
            streamTrace.failure(stage, outcome,
                                 e, clientOutput.count(),
                                 this.compressedBytes);
            this.abortConnection(stage);
            throw e;
        } catch (IOException e) {
            TaskResultStreamMetrics.Stage stage =
                    this.stage(clientOutput.count());
            streamTrace.failure(
                    stage,
                    TaskResultStreamMetrics.Outcome.FAILURE, e,
                    clientOutput.count(), this.compressedBytes);
            this.abortConnection(stage);
            throw e;
        } catch (RuntimeException | Error e) {
            TaskResultStreamMetrics.Stage stage =
                    this.stage(clientOutput.count());
            streamTrace.failure(
                    stage,
                    TaskResultStreamMetrics.Outcome.FAILURE, e,
                    clientOutput.count(), this.compressedBytes);
            this.abortConnection(stage);
            throw e;
        } finally {
            try {
                timeout.close();
            } finally {
                this.limiter.release();
            }
        }
    }

    private static long deadline(int timeoutSeconds) {
        return System.nanoTime() +
               TimeUnit.SECONDS.toNanos(timeoutSeconds);
    }

    private static boolean deadlineReached(long deadlineNanos) {
        return System.nanoTime() - deadlineNanos >= 0L;
    }

    private static void checkDeadline(long deadlineNanos) {
        if (Thread.currentThread().isInterrupted() ||
            deadlineReached(deadlineNanos)) {
            throw new TaskResultStreamException(
                    Reason.TIMEOUT, "Streaming the task result timed out");
        }
    }

    private static boolean isTransportTimeout(Throwable throwable) {
        for (Throwable cause = throwable;
             cause != null; cause = cause.getCause()) {
            if (cause instanceof TimeoutException ||
                cause instanceof SocketTimeoutException) {
                return true;
            }
            String message = cause.getMessage();
            if (message != null &&
                message.contains(GRIZZLY_WRITE_TIMEOUT_MESSAGE)) {
                return true;
            }
        }
        return false;
    }

    private TaskResultStreamMetrics.Stage stage(long writtenBytes) {
        if (this.responseCommitted != null) {
            try {
                return this.responseCommitted.getAsBoolean() ?
                       TaskResultStreamMetrics.Stage.POST_COMMIT :
                       TaskResultStreamMetrics.Stage.PRE_COMMIT;
            } catch (RuntimeException e) {
                LOG.warn("Failed to inspect task result response commit " +
                         "state", e);
            }
        }
        return writtenBytes > 0L ?
               TaskResultStreamMetrics.Stage.POST_COMMIT :
               TaskResultStreamMetrics.Stage.PRE_COMMIT;
    }

    private void abortConnection(TaskResultStreamMetrics.Stage stage) {
        if (stage != TaskResultStreamMetrics.Stage.POST_COMMIT ||
            this.connection == null) {
            return;
        }
        try {
            this.connection.terminateSilently();
        } catch (RuntimeException e) {
            LOG.warn("Failed to terminate the task result connection after " +
                     "a post-commit streaming failure", e);
        }
    }

    @FunctionalInterface
    interface Writer {

        void write(OutputStream output, long deadlineNanos)
                   throws IOException;
    }

    private static final class WriteTimeoutScope implements AutoCloseable {

        private static final WriteTimeoutScope NONE =
                new WriteTimeoutScope(null, 0L, 0L, false);

        private final Connection<?> connection;
        private final long previousMillis;
        private long appliedMillis;
        private boolean changed;

        private WriteTimeoutScope(Connection<?> connection,
                                  long previousMillis, long appliedMillis,
                                  boolean changed) {
            this.connection = connection;
            this.previousMillis = previousMillis;
            this.appliedMillis = appliedMillis;
            this.changed = changed;
        }

        private static WriteTimeoutScope open(Connection<?> connection,
                                              int timeoutSeconds) {
            if (connection == null) {
                return NONE;
            }
            long previous = connection.getWriteTimeout(TimeUnit.MILLISECONDS);
            long configured = TimeUnit.SECONDS.toMillis(timeoutSeconds);
            long effective = previous > 0L ?
                             Math.min(previous, configured) : configured;
            if (effective == previous) {
                return new WriteTimeoutScope(connection, previous,
                                             effective, false);
            }
            connection.setWriteTimeout(effective, TimeUnit.MILLISECONDS);
            return new WriteTimeoutScope(connection, previous,
                                         effective, true);
        }

        private void beforeWrite(long deadlineNanos) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                checkDeadline(deadlineNanos);
            }
            if (this.connection == null) {
                return;
            }
            long remainingMillis =
                    TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            if (remainingMillis == 0L ||
                TimeUnit.MILLISECONDS.toNanos(remainingMillis) <
                remainingNanos) {
                remainingMillis++;
            }
            long effective = this.previousMillis > 0L ?
                             Math.min(this.previousMillis,
                                      remainingMillis) :
                             remainingMillis;
            if (effective == this.appliedMillis) {
                return;
            }
            this.connection.setWriteTimeout(effective,
                                            TimeUnit.MILLISECONDS);
            this.appliedMillis = effective;
            this.changed = true;
        }

        @Override
        public void close() {
            if (!this.changed) {
                return;
            }
            try {
                this.connection.setWriteTimeout(this.previousMillis,
                                                TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
                LOG.warn("Failed to restore task result connection write " +
                         "timeout after streaming", e);
            }
        }
    }

    private static final class ClientOutputStream extends OutputStream {

        private final OutputStream output;
        private final long deadlineNanos;
        private WriteTimeoutScope timeout;
        private long count;

        private ClientOutputStream(OutputStream output, long deadlineNanos) {
            E.checkNotNull(output, "task result output");
            this.output = output;
            this.deadlineNanos = deadlineNanos;
            this.timeout = WriteTimeoutScope.NONE;
            this.count = 0L;
        }

        private void timeout(WriteTimeoutScope timeout) {
            this.timeout = timeout;
        }

        private long count() {
            return this.count;
        }

        private void finish() throws IOException {
            this.timeout.beforeWrite(this.deadlineNanos);
            try {
                if (this.output instanceof GZIPOutputStream) {
                    ((GZIPOutputStream) this.output).finish();
                }
                this.output.flush();
            } catch (IOException e) {
                throw new ClientWriteException(e);
            }
            checkDeadline(this.deadlineNanos);
        }

        @Override
        public void write(int value) throws IOException {
            this.timeout.beforeWrite(this.deadlineNanos);
            try {
                this.output.write(value);
                this.count++;
            } catch (IOException e) {
                throw new ClientWriteException(e);
            }
            checkDeadline(this.deadlineNanos);
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                          throws IOException {
            this.timeout.beforeWrite(this.deadlineNanos);
            try {
                this.output.write(bytes, offset, length);
                this.count += length;
            } catch (IOException e) {
                throw new ClientWriteException(e);
            }
            checkDeadline(this.deadlineNanos);
        }

        @Override
        public void flush() throws IOException {
            this.timeout.beforeWrite(this.deadlineNanos);
            try {
                this.output.flush();
            } catch (IOException e) {
                throw new ClientWriteException(e);
            }
            checkDeadline(this.deadlineNanos);
        }
    }

    private static final class ClientWriteException extends IOException {

        private static final long serialVersionUID = -1532858150323589311L;

        private ClientWriteException(IOException cause) {
            super("Failed to write the task result to the client", cause);
        }
    }
}
