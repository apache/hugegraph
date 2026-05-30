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

package org.apache.hugegraph.core;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.constant.ServiceConstant;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.util.Log;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        PropertyKeyCoreTest.class,
        VertexLabelCoreTest.class,
        EdgeLabelCoreTest.class,
        IndexLabelCoreTest.class,
        VertexCoreTest.class,
        EdgeCoreTest.class,
        CountStrategyCoreTest.class,
        ParentAndSubEdgeCoreTest.class,
        PropertyCoreTest.VertexPropertyCoreTest.class,
        PropertyCoreTest.EdgePropertyCoreTest.class,
        RestoreCoreTest.class,
        TaskCoreTest.class,
        AuthTest.class,
        MultiGraphsTest.class,
        RamTableTest.class
})
public class CoreTestSuite {

    private static boolean registered = false;
    private static volatile HugeGraph graph = null;

    public static HugeGraph graph() {
        if (graph == null) {
            synchronized (CoreTestSuite.class) {
                if (graph == null) {
                    try {
                        initEnv();
                        init();
                    } catch (Throwable e) {
                        LOG.error("Failed to initialize HugeGraph instance", e);
                        graph = null;
                        throw new RuntimeException("Failed to initialize HugeGraph instance", e);
                    }
                    if (graph == null) {
                        String msg = "HugeGraph instance is null after initialization. " +
                                     "Please check Utils.open() configuration.";
                        LOG.error(msg);
                        throw new IllegalStateException(msg);
                    }
                }
            }
        }
        return graph;
    }

    protected static final Logger LOG = Log.logger(CoreTestSuite.class);

    @BeforeClass
    public static void initEnv() {
        if (registered) {
            return;
        }
        RegisterUtil.registerBackends();
        registered = true;
    }

    @BeforeClass
    public static void init() {
        PdMetaDriver.PDAuthConfig.setAuthority(ServiceConstant.SERVICE_NAME,
                                               ServiceConstant.AUTHORITY);
        graph = Utils.open();
        graph.clearBackend();
        graph.initBackend();
        graph.serverStarted(GlobalMasterInfo.master("server-test"));

        // Initialize DEFAULT graphspace for V2 tests
        try {
            MetaManager metaManager =
                    MetaManager.instance();
            if (metaManager.isReady()) {
                metaManager.initDefaultGraphSpace();
            }
        } catch (Exception e) {
            // MetaManager may not be initialized for non-hstore backends
            LOG.debug(
                    "Failed to initialize default graphspace (expected for non-hstore backends): " +
                    "{}",
                    e.getMessage());
        }
    }

    @AfterClass
    public static void clear() {
        if (graph == null) {
            return;
        }

        try {
            graph.clearBackend();
        } finally {
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.error("Error when close()", e);
            }
            graph = null;
        }
    }
}
