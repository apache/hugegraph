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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.groovy.util.Maps;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.api.job.TaskResultPageTokenCodec.Token;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskResultPageCursor;
import org.apache.hugegraph.task.TaskResultPageCursor.RootType;
import org.apache.hugegraph.task.TaskResultSnapshot;
import org.apache.hugegraph.task.TaskResultStreamException;
import org.apache.hugegraph.task.TaskResultStreamException.Reason;
import org.apache.hugegraph.task.TaskResultStreamer;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

@Path("graphspaces/{graphspace}/graphs/{graph}/tasks")
@Singleton
@Tag(name = "TaskAPI")
public class TaskAPI extends API {

    private static final Logger LOG = Log.logger(TaskAPI.class);
    private static final long NO_LIMIT = -1L;

    public static final String ACTION_CANCEL = "cancel";

    private volatile Semaphore resultStreams;
    private volatile int resultStreamLimit;

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @Parameter(description = "The graphspace name")
                                    @PathParam("graphspace") String graphSpace,
                                    @Parameter(description = "The graph name")
                                    @PathParam("graph") String graph,
                                    @Parameter(description = "The task status to filter")
                                    @QueryParam("status") String status,
                                    @Parameter(description = "The task ids to filter")
                                    @QueryParam("ids") List<Long> ids,
                                    @Parameter(description = "The maximum number of tasks")
                                    @QueryParam("limit")
                                    @DefaultValue("100") long limit,
                                    @Parameter(description = "The page token for pagination")
                                    @QueryParam("page") String page) {
        LOG.debug("Graph [{}] list tasks with status {}, ids {}, " +
                  "limit {}, page {}", graph, status, ids, limit, page);

        TaskScheduler scheduler =
                graph(manager, graphSpace, graph).taskScheduler();

        Iterator<HugeTask<Object>> iter;

        if (!ids.isEmpty()) {
            E.checkArgument(status == null,
                            "Not support status when query task by ids, " +
                            "but got status='%s'", status);
            E.checkArgument(page == null,
                            "Not support page when query task by ids, " +
                            "but got page='%s'", page);
            // Set limit to NO_LIMIT to ignore limit when query task by ids
            limit = NO_LIMIT;
            List<Id> idList = ids.stream().map(IdGenerator::of)
                                 .collect(Collectors.toList());
            iter = scheduler.tasks(idList, false);
        } else {
            if (status == null) {
                iter = scheduler.tasks(null, limit, page, false);
            } else {
                iter = scheduler.tasks(parseStatus(status), limit, page,
                                       false);
            }
        }

        List<Object> tasks = new ArrayList<>();
        while (iter.hasNext()) {
            tasks.add(iter.next().asMap(false));
        }
        if (limit != NO_LIMIT && tasks.size() > limit) {
            tasks = tasks.subList(0, (int) limit);
        }

        if (page == null) {
            return Maps.of("tasks", tasks);
        } else {
            return Maps.of("tasks", tasks, "page", PageInfo.pageInfo(iter));
        }
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @Parameter(description = "The graphspace name")
                                   @PathParam("graphspace") String graphSpace,
                                   @Parameter(description = "The graph name")
                                   @PathParam("graph") String graph,
                                   @Parameter(description = "The task id")
                                   @PathParam("id") long id,
                                   @Parameter(description = "Whether to load task result")
                                   @DefaultValue("true")
                                   @QueryParam("with_result")
                                   boolean withResult) {
        LOG.debug("Graph [{}] get task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        return scheduler.task(IdGenerator.of(id), withResult)
                        .asMap(true, withResult);
    }

    @GET
    @Timed
    @Path("{id}/result")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response getResult(@Context GraphManager manager,
                              @Context HugeConfig config,
                              @Context Request request,
                              @Parameter(description = "The graphspace name")
                              @PathParam("graphspace") String graphSpace,
                              @Parameter(description = "The graph name")
                              @PathParam("graph") String graph,
                              @Parameter(description = "The task id")
                              @PathParam("id") long id,
                              @Parameter(description = "The result page size")
                              @QueryParam("page_size") Integer pageSize,
                              @Parameter(description = "The result page token")
                              @QueryParam("page") String page) {
        TaskResultStreamMetrics.Mode mode =
                pageSize != null || page != null ?
                TaskResultStreamMetrics.Mode.PAGE :
                TaskResultStreamMetrics.Mode.COMPLETE;
        TaskResultStreamMetrics.RequestTrace trace =
                TaskResultStreamMetrics.request(graphSpace, graph, id, mode);
        try {
            return this.buildResultResponse(manager, config, request,
                                            graphSpace, graph, id,
                                            pageSize, page, trace);
        } catch (RuntimeException | Error e) {
            trace.preCommitFailure(e);
            throw e;
        }
    }

    private Response buildResultResponse(
            GraphManager manager, HugeConfig config, Request request,
            String graphSpace, String graph, long id, Integer pageSize,
            String page, TaskResultStreamMetrics.RequestTrace trace) {
        E.checkArgument(pageSize == null || page == null,
                        "The parameters 'page_size' and 'page' can't be " +
                        "specified together");

        boolean pagination = pageSize != null || page != null;
        TaskResultPageTokenCodec codec = null;
        Token token = null;
        long offset = 0L;
        RootType expectedRoot = null;
        if (pageSize != null) {
            int maxPageSize = config.get(
                    ServerOptions.TASK_RESULT_PAGE_SIZE_MAX);
            E.checkArgument(pageSize > 0 && pageSize <= maxPageSize,
                            "The page size must be between 1 and %s",
                            maxPageSize);
        } else if (page != null) {
            codec = tokenCodec(config);
            try {
                E.checkArgument(!page.isEmpty(),
                                "The page token can't be empty");
                token = codec.decode(page, nowSeconds());
                E.checkArgument(graphSpace.equals(token.graphSpace()) &&
                                graph.equals(token.graph()) &&
                                id == token.taskId(),
                                "The page token doesn't match the " +
                                "requested task");
                pageSize = token.pageSize();
                offset = token.nextOffset();
                expectedRoot = token.rootType();
                E.checkArgument(pageSize <= config.get(
                                        ServerOptions.
                                                TASK_RESULT_PAGE_SIZE_MAX),
                                "The page token page size exceeds the " +
                                "current limit");
            } catch (IllegalArgumentException e) {
                throw new InvalidTaskResultPageTokenException(
                        e.getMessage(), e);
            }
        }
        trace.offset(offset);

        if (pagination) {
            checkPageRange(offset, pageSize, config.get(
                    ServerOptions.TASK_RESULT_PAGE_OFFSET_MAX));
        }

        Semaphore limiter = this.resultStreamLimiter(config.get(
                ServerOptions.TASK_RESULT_ACTIVE_STREAMS_MAX));
        if (!limiter.tryAcquire()) {
            throw new ServiceUnavailableException(
                    "Too many task results are being streamed");
        }

        boolean releaseHere = true;
        try {
            HugeGraph hugeGraph = graph(manager, graphSpace, graph);
            HugeConfig graphConfig = (HugeConfig) hugeGraph.configuration();
            trace.backend(graphConfig.get(CoreOptions.BACKEND));
            TaskScheduler scheduler = hugeGraph.taskScheduler();
            TaskResultSnapshot snapshot = scheduler.taskResultSnapshot(
                    IdGenerator.of(id));
            ensureReadable(snapshot);
            Connection<?> connection = connection(request);
            BooleanSupplier responseCommitted = responseCommitted(request);

            StreamingOutput output;
            if (!pagination) {
                output = streamComplete(snapshot, limiter, config,
                                        connection, responseCommitted,
                                        trace);
            } else {
                if (token != null &&
                    !token.fingerprint().equals(snapshot.fingerprint())) {
                    throw new TaskResultChangedException(
                            "The task result changed after the page token " +
                            "was issued");
                }
                RootType rootType = preflight(snapshot, offset, pageSize,
                                               expectedRoot, config);

                if (codec == null) {
                    codec = tokenCodec(config);
                }
                long issuedAt = nowSeconds();
                long expiresAt = issuedAt + config.get(
                        ServerOptions.TASK_RESULT_PAGE_TOKEN_TTL);
                TaskResultPageTokenCodec pageCodec = codec;
                int resultPageSize = pageSize;
                long resultOffset = offset;
                Token probe = pageToken(graphSpace, graph, id, rootType,
                                        resultOffset + resultPageSize,
                                        resultPageSize,
                                        snapshot.fingerprint(), issuedAt,
                                        expiresAt);
                pageCodec.encode(probe);
                output = streamPage(snapshot, limiter, config, connection,
                                    responseCommitted, trace, rootType,
                                    resultOffset,
                                    resultPageSize, pageCodec, graphSpace,
                                    graph, id, issuedAt, expiresAt);
            }

            Response response = Response.ok(output,
                                            APPLICATION_JSON_WITH_CHARSET)
                                        .header("Cache-Control", "no-store")
                                        .build();
            releaseHere = false;
            return response;
        } finally {
            if (releaseHere) {
                limiter.release();
            }
        }
    }

    @DELETE
    @Timed
    @Path("{id}")
    @RedirectFilter.RedirectMasterRole
    public void delete(@Context GraphManager manager,
                       @Parameter(description = "The graphspace name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The graph name")
                       @PathParam("graph") String graph,
                       @Parameter(description = "The task id")
                       @PathParam("id") long id,
                       @Parameter(description = "Force delete the task even if it's running")
                       @DefaultValue("false") @QueryParam("force") boolean force) {
        LOG.debug("Graph [{}] delete task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.delete(IdGenerator.of(id), force);
        E.checkArgument(task != null, "There is no task with id '%s'", id);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RedirectFilter.RedirectMasterRole
    public Map<String, Object> update(@Context GraphManager manager,
                                      @Parameter(description = "The graphspace name")
                                      @PathParam("graphspace")
                                      String graphSpace,
                                      @Parameter(description = "The graph name")
                                      @PathParam("graph") String graph,
                                      @Parameter(description = "The task id")
                                      @PathParam("id") long id,
                                      @Parameter(description = "The action to perform on the task")
                                      @QueryParam("action") String action) {
        LOG.debug("Graph [{}] cancel task: {}", graph, id);

        if (!ACTION_CANCEL.equals(action)) {
            throw new NotSupportedException(String.format(
                    "Not support action '%s'", action));
        }

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        if (!task.completed() && !task.cancelling()) {
            scheduler.cancel(task);
            if (task.cancelling() || task.cancelled()) {
                return task.asMap();
            }
        }

        assert task.completed() || task.cancelling();
        throw new BadRequestException(String.format(
                "Can't cancel task '%s' which is completed or cancelling",
                id));
    }

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Status value must be in %s, but got '%s'",
                    Arrays.asList(TaskStatus.values()), status));
        }
    }

    private static void ensureReadable(TaskResultSnapshot snapshot) {
        TaskStatus status = snapshot.status();
        if (status != TaskStatus.SUCCESS) {
            String message;
            if (status == TaskStatus.FAILED ||
                status == TaskStatus.CANCELLED) {
                message = String.format(
                        "Task '%s' has status '%s'; read its error from the " +
                        "task details endpoint", snapshot.taskId(),
                        status.string());
            } else {
                message = String.format(
                        "Task '%s' result is not ready in status '%s'",
                        snapshot.taskId(), status.string());
            }
            if (status == TaskStatus.FAILED ||
                status == TaskStatus.CANCELLED) {
                throw new TaskResultNotReadableException(message);
            }
            throw new TaskResultNotReadyException(message);
        }
        if (!snapshot.hasResult()) {
            throw new TaskResultUnavailableException(String.format(
                    "Task '%s' has no persisted result", snapshot.taskId()));
        }
    }

    private static RootType preflight(TaskResultSnapshot snapshot,
                                      long offset, int pageSize,
                                      RootType expectedRoot,
                                      HugeConfig config) {
        try {
            RootType rootType = TaskResultStreamer.preflight(
                    snapshot, offset, pageSize,
                    config.get(ServerOptions.TASK_RESULT_SCAN_BYTES_MAX),
                    deadline(config.get(
                            ServerOptions.TASK_RESULT_SCAN_TIME_MAX)));
            if (expectedRoot != null && expectedRoot != rootType) {
                throw new TaskResultChangedException(
                        String.format("The task result root changed from " +
                                      "'%s' to '%s'", expectedRoot.text(),
                                      rootType.text()));
            }
            return rootType;
        } catch (TaskResultStreamException e) {
            if (e.reason() == Reason.SCAN_LIMIT_EXCEEDED ||
                e.reason() == Reason.TIMEOUT) {
                throwStatus(Response.Status.REQUEST_ENTITY_TOO_LARGE
                                    .getStatusCode(), e.getMessage());
            }
            if (e.reason() == Reason.INVALID_JSON) {
                throwStatus(Response.Status.CONFLICT.getStatusCode(),
                            e.getMessage());
            }
            if (e.reason() == Reason.NOT_PAGEABLE) {
                throw new TaskResultNotPageableException(e.getMessage(), e);
            }
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static StreamingOutput streamComplete(TaskResultSnapshot snapshot,
                                                   Semaphore limiter,
                                                   HugeConfig config,
                                                   Connection<?> connection,
                                                   BooleanSupplier
                                                   responseCommitted,
                                                   TaskResultStreamMetrics.RequestTrace trace) {
        int timeout = config.get(
                ServerOptions.TASK_RESULT_STREAM_TIME_MAX);
        return new TaskResultStreamingOutput(
                connection, responseCommitted, timeout, limiter, trace,
                snapshot.compressedSize(),
                (output, deadlineNanos) -> TaskResultStreamer.stream(
                        snapshot, output, deadlineNanos));
    }

    private static StreamingOutput streamPage(
            TaskResultSnapshot snapshot, Semaphore limiter, HugeConfig config,
            Connection<?> connection,
            BooleanSupplier responseCommitted,
            TaskResultStreamMetrics.RequestTrace trace, RootType rootType,
            long offset, int pageSize,
            TaskResultPageTokenCodec codec, String graphSpace, String graph,
            long taskId, long issuedAt, long expiresAt) {
        int timeout = config.get(
                ServerOptions.TASK_RESULT_STREAM_TIME_MAX);
        return new TaskResultStreamingOutput(
                connection, responseCommitted, timeout, limiter, trace,
                snapshot.compressedSize(), (output, deadlineNanos) ->
                TaskResultStreamer.streamPage(
                        snapshot, rootType, offset, pageSize,
                        config.get(ServerOptions.TASK_RESULT_SCAN_BYTES_MAX),
                        deadlineNanos,
                        output, cursor -> codec.encode(pageToken(
                                graphSpace, graph, taskId, cursor,
                                issuedAt, expiresAt))));
    }

    private static Connection<?> connection(Request request) {
        if (request == null || request.getContext() == null) {
            return null;
        }
        return request.getContext().getConnection();
    }

    private static BooleanSupplier responseCommitted(Request request) {
        if (request == null || request.getResponse() == null) {
            return null;
        }
        org.glassfish.grizzly.http.server.Response response =
                request.getResponse();
        return response::isCommitted;
    }

    private synchronized Semaphore resultStreamLimiter(int permits) {
        if (this.resultStreams == null) {
            this.resultStreams = new Semaphore(permits);
            this.resultStreamLimit = permits;
        } else {
            E.checkState(this.resultStreamLimit == permits,
                         "The task result stream limit changed from '%s' " +
                         "to '%s' after startup",
                         this.resultStreamLimit, permits);
        }
        return this.resultStreams;
    }

    private static TaskResultPageTokenCodec tokenCodec(HugeConfig config) {
        return TaskResultPageTokenCodec.fromConfig(config);
    }

    private static Token pageToken(String graphSpace, String graph,
                                   long taskId,
                                   TaskResultPageCursor cursor,
                                   long issuedAt, long expiresAt) {
        return pageToken(graphSpace, graph, taskId, cursor.rootType(),
                         cursor.nextOffset(), cursor.pageSize(),
                         cursor.fingerprint(), issuedAt, expiresAt);
    }

    private static Token pageToken(String graphSpace, String graph,
                                   long taskId, RootType rootType,
                                   long nextOffset, int pageSize,
                                   String fingerprint, long issuedAt,
                                   long expiresAt) {
        return new Token(graphSpace, graph, taskId, rootType, nextOffset,
                         pageSize, fingerprint, issuedAt, expiresAt);
    }

    private static void checkPageRange(long offset, int pageSize,
                                       long maxOffset) {
        if (offset > maxOffset || pageSize > maxOffset - offset) {
            throwStatus(Response.Status.REQUEST_ENTITY_TOO_LARGE
                                .getStatusCode(),
                        String.format("The task result page exceeds the " +
                                      "maximum offset '%s'", maxOffset));
        }
    }

    private static long deadline(int seconds) {
        long now = System.nanoTime();
        long duration = TimeUnit.SECONDS.toNanos(seconds);
        if (Long.MAX_VALUE - now < duration) {
            return Long.MAX_VALUE;
        }
        return now + duration;
    }

    private static long nowSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }

    private static void throwStatus(int status, String message) {
        throw new WebApplicationException(message, status);
    }
}
