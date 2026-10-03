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

package org.apache.hugegraph.pd.raft;

import org.apache.hugegraph.pd.config.PDConfig;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

import com.alipay.sofa.jraft.option.NodeOptions;

/**
 * A candidate opens a connection to every peer while it holds the raft node lock, so the
 * connect timeout bounds how long one unanswering peer stalls an election. It used to be
 * {@code raft.rpc-timeout} (10 s), shared with the install-snapshot timeout; each now has
 * its own option, and the install-snapshot timeout still follows rpc-timeout unless set.
 */
public class RaftEngineRpcTimeoutTest {

    @Test
    public void testEachRpcTimeoutComesFromItsOwnOption() {
        PDConfig.Raft raft = new PDConfig().new Raft();
        raft.setRpcTimeout(7000);
        raft.setRpcConnectTimeout(900);
        raft.setRpcInstallSnapshotTimeout(120000);
        NodeOptions options = new NodeOptions();

        RaftEngine.setRpcTimeouts(options, raft);

        Assert.assertEquals(900, options.getRpcConnectTimeoutMs());
        Assert.assertEquals(7000, options.getRpcDefaultTimeout());
        Assert.assertEquals(120000, options.getRpcInstallSnapshotTimeout());
    }

    @Test
    public void testDefaults() throws NoSuchFieldException {
        // The connect timeout takes jraft's default of 1 s
        Assert.assertEquals("${raft.rpc-connect-timeout:1000}",
                            valueOf("rpcConnectTimeout"));
        Assert.assertEquals("${raft.rpc-install-snapshot-timeout:0}",
                            valueOf("rpcInstallSnapshotTimeout"));
        Assert.assertEquals("${raft.rpc-timeout:10000}", valueOf("rpcTimeout"));

        PDConfig.Raft raft = new PDConfig().new Raft();
        Assert.assertEquals(1000, raft.getRpcConnectTimeout());
        Assert.assertEquals(new NodeOptions().getRpcConnectTimeoutMs(),
                            raft.getRpcConnectTimeout());
    }

    @Test
    public void testInstallSnapshotTimeoutFollowsRpcTimeoutWhenUnset() {
        PDConfig.Raft raft = new PDConfig().new Raft();
        raft.setRpcTimeout(10000);
        NodeOptions options = new NodeOptions();

        RaftEngine.setRpcTimeouts(options, raft);

        Assert.assertEquals(10000, options.getRpcInstallSnapshotTimeout());
    }

    private static String valueOf(String field) throws NoSuchFieldException {
        return PDConfig.Raft.class.getDeclaredField(field).getAnnotation(Value.class).value();
    }
}
