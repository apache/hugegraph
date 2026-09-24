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

package org.apache.hugegraph.pd.rest;

import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.service.PDRestService;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pins the body of the two routes to the same leader balance, {@code GET /v1/task/balanceLeaders}
 * and {@code GET /v1/balanceLeaders}, when the balance is refused, for example inside the window
 * after {@code balancePartitions} sets the balance-shard key. Both must answer the status and
 * reason like their sibling endpoints do, not let the {@link PDException} escape as a bare
 * HTTP 500.
 */
public class BalanceLeadersAPITest {

    private static final String REASON = "balance shard is processing, please try later!";

    private static final LeaderBalance REFUSED = () -> {
        throw new PDException(1001, REASON);
    };

    private static final LeaderBalance BALANCED = () -> Map.of(1, 2L);

    @Test
    public void testTaskRouteRefusedBalanceReturnsErrorBody() throws Exception {
        TaskAPI api = new TaskAPI();
        api.pdRestService = service(REFUSED);

        assertErrorBody(api.balanceLeaders());
    }

    @Test
    public void testTaskRouteSuccessfulBalanceKeepsBody() {
        TaskAPI api = new TaskAPI();
        api.pdRestService = service(BALANCED);

        Assert.assertEquals("{\"1\":2}", api.balanceLeaders());
    }

    @Test
    public void testStoreRouteRefusedBalanceReturnsErrorBody() throws Exception {
        StoreAPI api = new StoreAPI();
        api.pdRestService = service(REFUSED);

        assertErrorBody(api.balanceLeaders());
    }

    @Test
    public void testStoreRouteSuccessfulBalanceKeepsBody() {
        StoreAPI api = new StoreAPI();
        api.pdRestService = service(BALANCED);

        Assert.assertEquals("{\"1\":2}", api.balanceLeaders());
    }

    private static PDRestService service(LeaderBalance balance) {
        return new PDRestService() {
            @Override
            public Map<Integer, Long> balancePartitionLeader() throws PDException {
                return balance.run();
            }
        };
    }

    private static void assertErrorBody(String json) throws Exception {
        Map<String, Object> body = new ObjectMapper().readValue(
                json, new TypeReference<Map<String, Object>>() {
                });

        Assert.assertEquals(1001, body.get("status"));
        Assert.assertEquals(REASON, body.get("error"));
    }

    private interface LeaderBalance {

        Map<Integer, Long> run() throws PDException;
    }
}
