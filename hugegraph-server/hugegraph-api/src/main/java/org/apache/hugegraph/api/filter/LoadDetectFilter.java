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

package org.apache.hugegraph.api.filter;

import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.define.WorkLoad;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import jakarta.inject.Singleton;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.ext.Provider;

@Provider
@Singleton
@PreMatching
public class LoadDetectFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(LoadDetectFilter.class);

    private static final Set<String> WHITE_API_LIST = ImmutableSet.of(
            "",
            "apis",
            "metrics",
            "versions"
    );

    // Call gc every 30+ seconds if memory is low and request frequently
    private static final RateLimiter GC_RATE_LIMITER =
            RateLimiter.create(1.0 / 30);

    // Log at most 1 request per second to avoid too many logs when server is under heavy load
    private static final RateLimiter BUSY_REJECT_LOG_RATE_LIMITER =
            RateLimiter.create(1.0);
    private static final RateLimiter MEMORY_REJECT_LOG_RATE_LIMITER =
            RateLimiter.create(1.0);

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;
    @Context
    private jakarta.inject.Provider<WorkLoad> loadProvider;
    private BooleanSupplier gcTrigger = LoadDetectFilter::triggerGcIfNeeded;
    private BooleanSupplier busyRejectLogPermit =
            BUSY_REJECT_LOG_RATE_LIMITER::tryAcquire;
    private BooleanSupplier memoryRejectLogPermit =
            MEMORY_REJECT_LOG_RATE_LIMITER::tryAcquire;
    private LongSupplier freeMemorySupplier = LoadDetectFilter::currentFreeMemoryInMB;

    public static boolean isWhiteAPI(ContainerRequestContext context) {
        List<PathSegment> segments = context.getUriInfo().getPathSegments();
        E.checkArgument(!segments.isEmpty(), "Invalid request uri '%s'",
                        context.getUriInfo().getPath());
        String rootPath = segments.get(0).getPath();
        return WHITE_API_LIST.contains(rootPath);
    }

    private static boolean triggerGcIfNeeded() {
        if (GC_RATE_LIMITER.tryAcquire(1)) {
            System.gc();
            return true;
        }
        return false;
    }

    private static long currentFreeMemoryInMB() {
        long allocatedMem = Runtime.getRuntime().totalMemory() -
                            Runtime.getRuntime().freeMemory();
        return (Runtime.getRuntime().maxMemory() - allocatedMem) / Bytes.MB;
    }

    @Override
    public void filter(ContainerRequestContext context) {
        if (LoadDetectFilter.isWhiteAPI(context)) {
            return;
        }

        HugeConfig config = this.configProvider.get();

        int maxWorkerThreads = config.get(ServerOptions.MAX_WORKER_THREADS);
        WorkLoad load = this.loadProvider.get();
        // There will be a thread doesn't work, dedicated to statistics
        int currentLoad = load.incrementAndGet();
        if (currentLoad >= maxWorkerThreads) {
            if (this.busyRejectLogPermit.getAsBoolean()) {
                LOG.warn("Rejected request due to high worker load, method={}, path={}, " +
                         "currentLoad={}, maxWorkerThreads={}",
                         context.getMethod(), context.getUriInfo().getPath(),
                         currentLoad, maxWorkerThreads);
            }
            throw new ServiceUnavailableException(String.format(
                    "The server is too busy to process the request, " +
                    "you can config %s to adjust it or try again later",
                    ServerOptions.MAX_WORKER_THREADS.name()));
        }

        long minFreeMemory = config.get(ServerOptions.MIN_FREE_MEMORY);
        long presumableFreeMem = this.freeMemorySupplier.getAsLong();
        if (presumableFreeMem < minFreeMemory) {
            boolean gcTriggered = this.gcTrigger.getAsBoolean();
            if (gcTriggered) {
                long recheckedFreeMem = this.freeMemorySupplier.getAsLong();
                if (recheckedFreeMem >= minFreeMemory) {
                    if (this.memoryRejectLogPermit.getAsBoolean()) {
                        LOG.warn("Low free memory recovered after GC, method={}, path={}, " +
                                 "presumableFreeMemMB={}, recheckedFreeMemMB={}, " +
                                 "minFreeMemoryMB={}",
                                 context.getMethod(), context.getUriInfo().getPath(),
                                 presumableFreeMem, recheckedFreeMem, minFreeMemory);
                    }
                    return;
                }
                if (this.memoryRejectLogPermit.getAsBoolean()) {
                    LOG.warn("Rejected request due to low free memory after GC, " +
                             "method={}, path={}, presumableFreeMemMB={}, " +
                             "recheckedFreeMemMB={}, minFreeMemoryMB={}",
                             context.getMethod(), context.getUriInfo().getPath(),
                             presumableFreeMem, recheckedFreeMem, minFreeMemory);
                }
                presumableFreeMem = recheckedFreeMem;
            } else if (this.memoryRejectLogPermit.getAsBoolean()) {
                LOG.warn("Rejected request due to low free memory, method={}, path={}, " +
                         "presumableFreeMemMB={}, minFreeMemoryMB={}",
                         context.getMethod(), context.getUriInfo().getPath(),
                         presumableFreeMem, minFreeMemory);
            }
            throw new ServiceUnavailableException(String.format(
                    "The server available memory %s(MB) is below than " +
                    "threshold %s(MB) and can't process the request, " +
                    "you can config %s to adjust it or try again later",
                    presumableFreeMem, minFreeMemory,
                    ServerOptions.MIN_FREE_MEMORY.name()));
        }
    }
}
