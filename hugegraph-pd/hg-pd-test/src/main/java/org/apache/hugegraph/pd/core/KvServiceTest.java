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

package org.apache.hugegraph.pd.core;

import java.util.Map;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.kv.TxnCompare;
import org.apache.hugegraph.pd.grpc.kv.TxnOp;
import org.apache.hugegraph.pd.grpc.kv.TxnRecord;
import org.apache.hugegraph.pd.grpc.kv.TxnRequest;
import org.apache.hugegraph.pd.grpc.kv.TxnResponse;
import org.junit.Assert;
import org.junit.Test;

public class KvServiceTest extends PDCoreTestBase {

    @Test
    public void testKv() {
        try {
            PDConfig pdConfig = getPdConfig();
            KvService service = new KvService(pdConfig);
            String key = "kvTest";
            String kvTest = service.get(key);
            Assert.assertEquals(kvTest, "");
            service.put(key, "kvTestValue");
            kvTest = service.get(key);
            Assert.assertEquals(kvTest, "kvTestValue");
            service.scanWithPrefix(key);
            service.delete(key);
            service.put(key, "kvTestValue");
            service.deleteWithPrefix(key);
            service.put(key, "kvTestValue", 1000L);
            service.keepAlive(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * The suite runs PD without raft, so this covers the local TXN path and its encoding
     */
    @Test
    public void testTxn() throws PDException {
        KvService service = new KvService(getPdConfig());
        String record = "HUGEGRAPH/hg/SCHEMA_SYNC/DEFAULT/txn-test";
        TxnRequest create = TxnRequest.newBuilder()
                                      .addCompares(TxnCompare.newBuilder().setKey("txn-test/a")
                                                             .setKind(TxnCompare.Kind.ABSENT))
                                      .addOps(TxnOp.newBuilder().setKey("txn-test/a")
                                                   .setType(TxnOp.Type.PUT).setValue("1"))
                                      .addOps(TxnOp.newBuilder().setKey("txn-test/b")
                                                   .setType(TxnOp.Type.PUT).setValue("2"))
                                      .setRecord(TxnRecord.newBuilder().setKey(record)
                                                          .setOp(TxnRecord.Op.BUMP))
                                      .build();
        TxnResponse first = service.txn(create);
        Assert.assertTrue(first.getSucceeded());
        Assert.assertEquals(1, first.getIncarnation());
        Map<String, String> kvs = service.scanWithPrefix("txn-test/");
        Assert.assertEquals(2, kvs.size());
        Assert.assertEquals("1", kvs.get("txn-test/a"));
        String value = "{\"rev\":" + first.getRevision() + ",\"inc\":1,\"state\":\"LIVE\"}";
        Assert.assertEquals(value, service.get(record));

        // a exists now, so the compare fails and nothing is written
        TxnResponse rejected = service.txn(create.toBuilder().addOps(
                TxnOp.newBuilder().setKey("txn-test/c").setType(TxnOp.Type.PUT)
                     .setValue("3")).build());
        Assert.assertFalse(rejected.getSucceeded());
        Assert.assertEquals(TxnResponse.Failure.COMPARE_FAILED, rejected.getFailure());
        Assert.assertEquals("", service.get("txn-test/c"));

        // An empty DELETE_PREFIX key is every KV key: rejected, and nothing is deleted
        TxnRequest all = TxnRequest.newBuilder().addOps(
                TxnOp.newBuilder().setType(TxnOp.Type.DELETE_PREFIX)).build();
        Assert.assertThrows(PDException.class, () -> service.txn(all));
        Assert.assertEquals("1", service.get("txn-test/a"));

        TxnRequest clear = TxnRequest.newBuilder()
                                     .addOps(TxnOp.newBuilder().setKey("txn-test/")
                                                  .setType(TxnOp.Type.DELETE_PREFIX))
                                     .setRecord(TxnRecord.newBuilder().setKey(record)
                                                         .setOp(TxnRecord.Op.BUMP)
                                                         .setExpectedIncarnation(1))
                                     .build();
        TxnResponse second = service.txn(clear);
        Assert.assertTrue(second.getSucceeded());
        Assert.assertTrue(second.getRevision() > first.getRevision());
        Assert.assertTrue(service.scanWithPrefix("txn-test/").isEmpty());
    }

    @Test
    public void testMember() {
        try {
            PDConfig pdConfig = getPdConfig();
            KvService service = new KvService(pdConfig);
            service.setPdConfig(pdConfig);
            PDConfig config = service.getPdConfig();
            // TODO
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
