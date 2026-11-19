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

package org.apache.hugegraph.backend.store;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;

import com.alipay.remoting.rpc.RpcServer;

public interface BackendStoreProvider {

    String SCHEMA_STORE = "m";
    String GRAPH_STORE = "g";
    String SYSTEM_STORE = "s";

    // Backend store type
    String type();

    // Backend store version
    String storedVersion();

    // Current backend store driver version
    String driverVersion();

    /**
     * The name of the serializer this backend reads and writes (see
     * SerializerFactory): "binary" for the binary-only backends. A graph
     * takes it unless its config names a serializer explicitly.
     */
    default String serializerName() {
        return "binary";
    }

    /**
     * Whether a graph on this backend may use a serializer other than
     * {@link #serializerName()}. False for backends whose tables parse
     * stored keys in one fixed format (hstore, rocksdb): rows written with
     * another serializer are stored but fail to read.
     */
    default boolean acceptsOtherSerializer() {
        return false;
    }

    // Graph name (that's database name)
    String graph();

    BackendStore loadSystemStore(HugeConfig config);

    BackendStore loadSchemaStore(HugeConfig config);

    BackendStore loadGraphStore(HugeConfig config);

    void open(String name);

    void waitReady(RpcServer rpcServer);

    void close();

    void init();

    void clear();

    boolean initialized();

    void truncate();

    void createSnapshot();

    void resumeSnapshot();

    void listen(EventListener listener);

    void unlisten(EventListener listener);

    EventHub storeEventHub();

    void onCloneConfig(HugeConfig config, String newGraph);

    void onDeleteConfig(HugeConfig config);

    default boolean isHstore() {
        return "hstore".equals(type());
    }
}
