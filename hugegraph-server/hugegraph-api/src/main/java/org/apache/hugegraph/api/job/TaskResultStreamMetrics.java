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

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.task.TaskResultStreamException;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import jakarta.ws.rs.WebApplicationException;

final class TaskResultStreamMetrics {

    private static final Logger LOG =
            Log.logger(TaskResultStreamMetrics.class);

    private static final Counter ACTIVE = counter("active");
    private static final Meter REQUESTS = meter("requests");
    private static final Meter COMPLETE_REQUESTS =
            meter("complete-requests");
    private static final Meter PAGE_REQUESTS = meter("page-requests");
    private static final Meter SUCCESS = meter("success");
    private static final Meter PRECOMMIT_FAILED =
            meter("precommit-failed");
    private static final Meter POSTCOMMIT_FAILED =
            meter("postcommit-failed");
    private static final Meter DISCONNECTED = meter("disconnected");
    private static final Meter TIMEOUT = meter("timeout");
    private static final Histogram DURATION = histogram("duration-ms");
    private static final Histogram COMPRESSED_BYTES =
            histogram("compressed-bytes");
    private static final Histogram WRITTEN_BYTES =
            histogram("written-bytes");
    private static final Histogram SCAN_OFFSET = histogram("scan-offset");

    private TaskResultStreamMetrics() {
    }

    static RequestTrace request(String graphSpace, String graph, long taskId,
                                Mode mode) {
        REQUESTS.mark();
        if (mode == Mode.PAGE) {
            PAGE_REQUESTS.mark();
        } else {
            COMPLETE_REQUESTS.mark();
        }
        return new RequestTrace(graphSpace, graph, taskId, mode);
    }

    private static Counter counter(String name) {
        return MetricsUtil.registerCounter(
                TaskAPI.class, "task-result-stream-" + name);
    }

    private static Meter meter(String name) {
        return MetricsUtil.registerMeter(
                TaskAPI.class, "task-result-stream-" + name);
    }

    private static Histogram histogram(String name) {
        return MetricsUtil.registerHistogram(
                TaskAPI.class, "task-result-stream-" + name);
    }

    enum Mode {
        COMPLETE("complete"),
        PAGE("page");

        private final String text;

        Mode(String text) {
            this.text = text;
        }
    }

    enum Outcome {
        FAILURE,
        DISCONNECTED,
        TIMEOUT
    }

    enum Stage {
        PRE_COMMIT("pre_commit"),
        POST_COMMIT("post_commit");

        private final String text;

        Stage(String text) {
            this.text = text;
        }
    }

    static final class RequestTrace {

        private final String graphSpace;
        private final String graph;
        private final long taskId;
        private final Mode mode;
        private final long startNanos;
        private String backend;
        private long offset;

        private RequestTrace(String graphSpace, String graph, long taskId,
                             Mode mode) {
            this.graphSpace = graphSpace;
            this.graph = graph;
            this.taskId = taskId;
            this.mode = mode;
            this.startNanos = System.nanoTime();
            this.backend = "unknown";
            this.offset = 0L;
        }

        void backend(String backend) {
            if (backend != null && !backend.isEmpty()) {
                this.backend = backend;
            }
        }

        void offset(long offset) {
            this.offset = offset;
        }

        void preCommitFailure(Throwable error) {
            PRECOMMIT_FAILED.mark();
            DURATION.update(elapsedMillis(this.startNanos));
            this.logFailure(Stage.PRE_COMMIT.text, reason(error), 0L, 0,
                            error);
        }

        StreamTrace stream(int compressedBytes) {
            ACTIVE.inc();
            COMPRESSED_BYTES.update(compressedBytes);
            SCAN_OFFSET.update(this.offset);
            return new StreamTrace(this);
        }

        private void logFailure(String stage, String reason,
                                long writtenBytes, int compressedBytes,
                                Throwable error) {
            LOG.warn("Task result stream failed: graphspace={}, graph={}, " +
                     "backend={}, task_id={}, mode={}, offset={}, stage={}, " +
                     "reason={}, compressed_bytes={}, written_bytes={}, " +
                     "error_type={}",
                     this.graphSpace, this.graph, this.backend, this.taskId,
                     this.mode.text, this.offset, stage, reason,
                     compressedBytes, writtenBytes,
                     error.getClass().getSimpleName());
        }
    }

    static final class StreamTrace {

        private final RequestTrace request;
        private final long startNanos;
        private boolean finished;

        private StreamTrace(RequestTrace request) {
            this.request = request;
            this.startNanos = System.nanoTime();
            this.finished = false;
        }

        synchronized void success(long writtenBytes) {
            if (!this.finish(writtenBytes)) {
                return;
            }
            SUCCESS.mark();
        }

        synchronized void failure(Stage stage, Outcome outcome,
                                  Throwable error,
                                  long writtenBytes, int compressedBytes) {
            if (!this.finish(writtenBytes)) {
                return;
            }
            if (stage == Stage.PRE_COMMIT) {
                PRECOMMIT_FAILED.mark();
            } else {
                POSTCOMMIT_FAILED.mark();
            }
            if (outcome == Outcome.DISCONNECTED) {
                DISCONNECTED.mark();
            } else if (outcome == Outcome.TIMEOUT) {
                TIMEOUT.mark();
            }
            this.request.logFailure(stage.text, reason(outcome, error),
                                    writtenBytes, compressedBytes, error);
        }

        private boolean finish(long writtenBytes) {
            if (this.finished) {
                return false;
            }
            this.finished = true;
            ACTIVE.dec();
            DURATION.update(elapsedMillis(this.startNanos));
            WRITTEN_BYTES.update(writtenBytes);
            return true;
        }
    }

    private static String reason(Outcome outcome, Throwable error) {
        if (outcome == Outcome.DISCONNECTED) {
            return "client_disconnect";
        }
        if (outcome == Outcome.TIMEOUT) {
            return "timeout";
        }
        return reason(error);
    }

    private static String reason(Throwable error) {
        if (error instanceof TaskResultException) {
            return ((TaskResultException) error).metricReason();
        }
        if (error instanceof TaskResultStreamException) {
            TaskResultStreamException streamError =
                    (TaskResultStreamException) error;
            return streamError.reason().name().toLowerCase(Locale.ROOT);
        }
        if (error instanceof WebApplicationException) {
            WebApplicationException webError = (WebApplicationException) error;
            return "http_" + webError.getResponse().getStatus();
        }
        if (error instanceof IllegalArgumentException) {
            return "invalid_argument";
        }
        return error.getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    private static long elapsedMillis(long startNanos) {
        long elapsed = System.nanoTime() - startNanos;
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, elapsed));
    }
}
