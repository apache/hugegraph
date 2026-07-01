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

package org.apache.hugegraph.SimpleClusterTest;

import java.io.IOException;
import java.util.List;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.junit.Assert;
import org.junit.Test;

public class SimpleClusterDeployTest extends BaseSimpleTest {

    @Test
    public void testPDNodesDeployment() {
        try {
            List<String> addrs = env.getPDGrpcAddrs();
            for (String addr : addrs) {
                PDConfig pdConfig = PDConfig.of(addr, Long.MAX_VALUE);
                pdClient = PDClient.create(pdConfig);
                pdClient.dbCompaction();
            }
            assert true;
        } catch (PDException pdException) {
            assert false;
        }
    }

    @Test
    public void testStoreNodesDeployment() throws IOException {
        List<String> addrs = env.getStoreRestAddrs();
        for (String addr : addrs) {
            String[] cmds = {"curl", addr};
            String responseMsg = execCmd(cmds);
            Assert.assertTrue(responseMsg.startsWith("{"));
        }
    }

    @Test
    public void testServerNode() {
        String path = URL_PREFIX + SCHEMA_PKS;
        createAndAssert(path, "{\n"
                              + "\"name\": \"name\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"age\",\n"
                              + "\"data_type\": \"INT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"city\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"lang\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"date\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"price\",\n"
                              + "\"data_type\": \"INT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"weight\",\n"
                              + "\"data_type\": \"DOUBLE\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"rank\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
    }
}
