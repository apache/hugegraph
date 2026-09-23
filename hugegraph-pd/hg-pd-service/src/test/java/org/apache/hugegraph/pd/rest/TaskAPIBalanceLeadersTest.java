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
 * Pins the body of {@code GET /v1/task/balanceLeaders} when the leader balance is refused,
 * for example inside the window after {@code balancePartitions} sets the balance-shard key.
 * The endpoint must answer the status and reason like its sibling task endpoints do, not
 * let the {@link PDException} escape as a bare HTTP 500.
 */
public class TaskAPIBalanceLeadersTest {

    private static final String REASON = "balance shard is processing, please try later!";

    @Test
    public void testRefusedBalanceReturnsErrorBody() throws Exception {
        TaskAPI api = apiReturning(() -> {
            throw new PDException(1001, REASON);
        });

        Map<String, Object> body = parse(api.balanceLeaders());

        Assert.assertEquals(1001, body.get("status"));
        Assert.assertEquals(REASON, body.get("error"));
    }

    @Test
    public void testSuccessfulBalanceKeepsBody() {
        TaskAPI api = apiReturning(() -> Map.of(1, 2L));

        Assert.assertEquals("{\"1\":2}", api.balanceLeaders());
    }

    private static TaskAPI apiReturning(LeaderBalance balance) {
        TaskAPI api = new TaskAPI();
        api.pdRestService = new PDRestService() {
            @Override
            public Map<Integer, Long> balancePartitionLeader() throws PDException {
                return balance.run();
            }
        };
        return api;
    }

    private static Map<String, Object> parse(String json) throws Exception {
        return new ObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {
        });
    }

    private interface LeaderBalance {

        Map<Integer, Long> run() throws PDException;
    }
}
