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

package org.apache.hugegraph.pd.service;

import java.util.List;

import org.apache.hugegraph.pd.LogService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Any;

public class LogServiceTest {

    private PDConfig mockPdConfig = BaseServerTest.getConfig();

    private LogService logServiceUnderTest;

    @Before
    public void setUp() {
        logServiceUnderTest = new LogService(mockPdConfig);
    }

    @Test
    public void testGetLog() throws Exception {
        logServiceUnderTest.insertLog("action", "message",
                                      Any.newBuilder().build());

        // Run the test
        final List<Metapb.LogRecord> result = logServiceUnderTest.getLog(
                "action", 0L, System.currentTimeMillis());

        // Verify the results
        Assert.assertEquals(result.size(), 1);
    }
}
