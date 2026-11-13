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

package org.apache.hugegraph.api.gremlin;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;

import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

public final class ForwardGremlinRestExecutor implements GremlinRestExecutor {

    private final HugeConfig config;
    private GremlinClient client;
    private boolean closed;

    public ForwardGremlinRestExecutor(HugeConfig config) {
        this.config = config;
    }

    private synchronized GremlinClient client() {
        if (this.closed) {
            throw new ServiceUnavailableException("Gremlin REST is stopping");
        }
        if (this.client == null) {
            int routes = this.config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE);
            this.client = new GremlinClient(
                    this.config.get(ServerOptions.GREMLIN_SERVER_URL),
                    this.config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT) * 1000,
                    routes, routes);
        }
        return this.client;
    }

    @Override
    public Response get(String authorization, MultivaluedMap<String, String> parameters,
                        String remoteAddress) {
        return this.client().doGetRequest(authorization, parameters);
    }

    @Override
    public Response post(String authorization, String body, String remoteAddress) {
        return this.client().doPostRequest(authorization, body);
    }

    @Override
    public synchronized void close() {
        if (!this.closed) {
            this.closed = true;
            if (this.client != null) {
                this.client.close();
            }
        }
    }
}
