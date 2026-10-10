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

package org.apache.hugegraph.pd.boot;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.pulse.PDPulseSubject;
import org.apache.hugegraph.pd.service.KvServiceGrpcImpl;
import org.apache.hugegraph.pd.service.MetadataService;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationListener;
import org.lognet.springboot.grpc.context.GRpcServerInitializedEvent;

import io.grpc.Server;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Spring stops its transports before destroying singleton beans. The PDService
 * dependency keeps its metadata owners available until this cleanup completes.
 */
@Slf4j
@Component
public class ShutdownHook implements DisposableBean, ApplicationListener<GRpcServerInitializedEvent> {

    private final PDService service;
    private final KvServiceGrpcImpl kvService;
    private Server grpcServer;

    public ShutdownHook(PDService service, KvServiceGrpcImpl kvService) {
        this.service = service;
        this.kvService = kvService;
    }

    @Override
    public void onApplicationEvent(GRpcServerInitializedEvent event) {
        this.grpcServer = event.getServer();
    }

    @Override
    public void destroy() {
        // The starter's default stop initiates shutdown without waiting.
        awaitGrpcTermination();
        this.kvService.stopScheduling();
        closeOwners(this.service);
    }

    public static void closeOwners(PDService service) {
        log.info("Closing PD background work and native owners");
        TaskScheduleService tasks = service.getTaskService();
        if (tasks != null) {
            tasks.shutDown();
        }
        PDPulseSubject.stopScheduling();
        RaftEngine raft = RaftEngine.getInstance();
        raft.stopLeaderCallbacks();
        // Raft may still submit snapshot saves while its shutdown is joining.
        raft.shutDown();
        awaitUninterruptibleJobs();
        // A failed join must not release the database out from under Raft.
        MetadataFactory.shutdownStore();
        log.info("PD native owners closed");
    }

    private void awaitGrpcTermination() {
        if (this.grpcServer == null) {
            return;
        }
        // End long-lived watch/pulse streams as well as accepting no new calls.
        this.grpcServer.shutdownNow();
        boolean interrupted = Thread.interrupted();
        try {
            while (!this.grpcServer.isTerminated()) {
                try {
                    if (!this.grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Waiting for PD gRPC transport to terminate");
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void awaitUninterruptibleJobs() {
        ThreadPoolExecutor jobs = MetadataService.getUninterruptibleJobs();
        if (jobs == null) {
            return;
        }
        jobs.shutdown();
        boolean interrupted = Thread.interrupted();
        try {
            while (!jobs.isTerminated()) {
                try {
                    if (!jobs.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Waiting for PD metadata jobs, active: {}, queued: {}",
                                 jobs.getActiveCount(), jobs.getQueue().size());
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
