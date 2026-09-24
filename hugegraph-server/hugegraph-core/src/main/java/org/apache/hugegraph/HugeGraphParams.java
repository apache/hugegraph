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

package org.apache.hugegraph;

import org.apache.hugegraph.analyzer.Analyzer;
import org.apache.hugegraph.backend.LocalCounter;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.ram.RamTable;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.task.ServerInfoManager;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;

import com.google.common.util.concurrent.RateLimiter;

/**
 * Graph inner Params interface
 */
public interface HugeGraphParams {

    HugeGraph graph();

    String name();

    String spaceGraphName();

    GraphMode mode();

    GraphReadMode readMode();

    ISchemaTransaction schemaTransaction();

    GraphTransaction systemTransaction();

    GraphTransaction graphTransaction();

    GraphTransaction openTransaction();

    void closeTx();

    boolean started();

    boolean closed();

    boolean initialized();

    BackendFeatures backendStoreFeatures();

    BackendStore loadSchemaStore();

    BackendStore loadGraphStore();

    BackendStore loadSystemStore();

    EventHub schemaEventHub();

    EventHub graphEventHub();

    EventHub indexEventHub();

    HugeConfig configuration();

    ServerInfoManager serverManager();

    LocalCounter counter();

    AbstractSerializer serializer();

    Analyzer analyzer();

    RateLimiter writeRateLimiter();

    RateLimiter readRateLimiter();

    RamTable ramtable();

    <T> void submitEphemeralJob(EphemeralJob<T> job);

    String schedulerType();
}
