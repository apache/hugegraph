/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hugegraph.tinkerpop;

import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Utils;
import org.junit.Assume;
import org.junit.Test;

public class HugeGraphProviderLifecycleTest {

    @Test
    public void testProviderContextLifecycleWithMemoryBackend()
            throws Exception {
        Assume.assumeTrue("memory".equals(
                Utils.getConf().getString("backend")));
        RegisterUtil.registerBackends();
        HugeGraphProviderContext context = new HugeGraphProviderContext();
        ProcessTestGraphProvider provider = context.provider();
        TestGraph graph = null;
        try {
            Assert.assertSame(provider, context.provider());

            Map<String, Object> config = provider.getBaseConfiguration(
                    "provider_context", this.getClass(),
                    "testProviderContextLifecycleWithMemoryBackend", null);
            Configuration configuration = new MapConfiguration(config);
            graph = (TestGraph) provider.openTestGraph(configuration);

            Assert.assertEquals("memory", graph.hugegraph().backend());
            Assert.assertFalse(graph.closed());

            provider.clear(graph, configuration);
            Assert.assertFalse(graph.closed());

            context.clear();
            Assert.assertTrue(graph.closed());

            context.clear();
            Assert.assertNotSame(provider, context.provider());
        } finally {
            context.clear();
        }
    }
}
