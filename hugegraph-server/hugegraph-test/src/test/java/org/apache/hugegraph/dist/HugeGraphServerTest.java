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

package org.apache.hugegraph.dist;

import java.util.concurrent.CompletableFuture;

import org.apache.hugegraph.server.RestServer;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.junit.Test;
import org.mockito.Mockito;

public class HugeGraphServerTest {

    @Test
    public void testRollbackStartupStopsAllStartedResources() {
        MemoryMonitor monitor = Mockito.mock(MemoryMonitor.class);
        GremlinServer gremlinServer = Mockito.mock(GremlinServer.class);
        RestServer restServer = Mockito.mock(RestServer.class);
        Mockito.when(gremlinServer.stop()).thenReturn(
                CompletableFuture.completedFuture(null));
        Mockito.when(restServer.shutdown()).thenReturn(
                CompletableFuture.completedFuture(null));

        HugeGraphServer.rollbackStartup(monitor, gremlinServer, restServer);

        Mockito.verify(monitor).stop();
        Mockito.verify(gremlinServer).stop();
        Mockito.verify(restServer).shutdown();
    }

    @Test
    public void testRollbackStartupAfterMonitorConstructionFailure() {
        GremlinServer gremlinServer = Mockito.mock(GremlinServer.class);
        RestServer restServer = Mockito.mock(RestServer.class);
        Mockito.when(gremlinServer.stop()).thenReturn(
                CompletableFuture.completedFuture(null));
        Mockito.when(restServer.shutdown()).thenReturn(
                CompletableFuture.completedFuture(null));

        HugeGraphServer.rollbackStartup(null, gremlinServer, restServer);

        Mockito.verify(gremlinServer).stop();
        Mockito.verify(restServer).shutdown();
    }

    @Test
    public void testRollbackStartupContinuesAfterStopFailure() {
        MemoryMonitor monitor = Mockito.mock(MemoryMonitor.class);
        GremlinServer gremlinServer = Mockito.mock(GremlinServer.class);
        RestServer restServer = Mockito.mock(RestServer.class);
        Mockito.doThrow(new RuntimeException("monitor stop failed"))
               .when(monitor).stop();
        Mockito.when(gremlinServer.stop())
               .thenThrow(new RuntimeException("gremlin stop failed"));
        Mockito.when(restServer.shutdown()).thenReturn(
                CompletableFuture.completedFuture(null));

        HugeGraphServer.rollbackStartup(monitor, gremlinServer, restServer);

        Mockito.verify(monitor).stop();
        Mockito.verify(gremlinServer).stop();
        Mockito.verify(restServer).shutdown();
    }
}
