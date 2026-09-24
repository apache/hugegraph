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

package org.apache.hugegraph.store.core;

import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toIntBytes;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toOwnerKey;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.UnitTestBase;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.response.BatchPutResponse;
import org.apache.hugegraph.store.cmd.response.CleanDataResponse;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.pd.DefaultPdProvider;
import org.apache.hugegraph.store.pd.PdProvider;
import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.util.Endpoint;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgCmdClientTest {

    private static AtomicLong id;
    private final String pdAddress = "127.0.0.1:8686";
    private final String graphName = "hugegraph";
    private final String tableName = UnitTestBase.DEFAULT_TEST_TABLE;
    private PdProvider pdProvider;
    private HgCmdClient hgCmdClient;
    private HgStoreClient storeClient;
    private PDClient pdClient;

    public static String getMd5(String txt) {
        String rs = "";
        String[] hexDigits =
                {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] b = messageDigest.digest(txt.getBytes());
            StringBuffer resultSb = new StringBuffer();
            for (int i = 0; i < b.length; i++) {
                int n = b[i];
                if (n < 0) {
                    n = 256 + n;
                }
                int d1 = n / 16;
                int d2 = n % 16;
                resultSb.append(hexDigits[d1] + hexDigits[d2]);
            }
            rs = resultSb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static Long getId() {
        // If needed longer or more redundant space, just use time * 10^n
        // Currently guaranteed to generate 10000 non-duplicate in 1 millisecond
        Long time = Long.valueOf(new SimpleDateFormat("HHmmssSSS").format(new Date())) * 10000 +
                    (long) (Math.random() * 100);
//        Long time = Long.valueOf(new SimpleDateFormat("MMddhhmmssSSS").format(new Date())
//        .toString());
//        System.out.println(time);
        if (id == null) {
            id = new AtomicLong(time);
            return id.get();
        }
        if (time <= id.get()) {
            id.addAndGet(1);
        } else {
            id = new AtomicLong(time);
        }
        return id.get();
    }

    @Test
    public void testGetStoreInfo() {

        hgCmdClient = new HgCmdClient();
        pdProvider = new DefaultPdProvider(pdAddress);
        hgCmdClient.init(new RpcOptions(), null);

        Store response = hgCmdClient.getStoreInfo(pdAddress);

    }

    //    @Test
    public void testBatchPut() {

        hgCmdClient = new HgCmdClient();
        pdProvider = new DefaultPdProvider(pdAddress);
        hgCmdClient.init(new RpcOptions(), new HgCmdClient.PartitionAgent() {
            @Override
            public Endpoint getPartitionLeader(String graphName, int partitionId) {
                Metapb.Shard shard = pdProvider.getPartitionLeader(graphName, partitionId);
                return JRaftUtils.getEndPoint(
                        pdProvider.getStoreByID(shard.getStoreId()).getRaftAddress());
            }
        });

        storeClient = HgStoreClient.create(PDConfig.of(pdAddress)
                                                   .setEnableCache(true));
        HgStoreSession session = storeClient.openSession(graphName);
        pdClient = storeClient.getPdClient();
        session.createTable(tableName);
        String createGraph = "create_graph";
        HgOwnerKey hgOwnerKey = toOwnerKey(createGraph);
        // Need to write data, then the graph will be created.
        session.put(tableName,
                    hgOwnerKey, createGraph.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(createGraph, toStr(session.get(tableName, hgOwnerKey)));

        Integer partId = 0;
        String key = "key-1";
        List<BatchPutRequest.KV> kvs = new LinkedList<>();
        int x = 0;
        for (int i = 1; i <= 3; i++) {
            key = "key-" + i;
            BatchPutRequest.KV kv = BatchPutRequest.KV.of(tableName, 1,
                                                          key.getBytes(StandardCharsets.UTF_8),
                                                          key.getBytes(StandardCharsets.UTF_8));
            kvs.add(kv);

            BatchPutRequest request = new BatchPutRequest();
            request.setGraphName(graphName);
            request.setPartitionId(partId);
            request.setEntries(kvs);

            try {
                BatchPutResponse response = hgCmdClient.batchPut(request);
                if (response == null) {
                    log.error("response is null ");
                } else if (response.getStatus() == null) {
                    log.error("response status is null");
                }

                log.info("response status:{} {}", response.getStatus(), i);

                Assert.assertTrue(response.getStatus().isOK());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        HgKvIterator<HgKvEntry> hgKvIterator = session.scanIterator(tableName);
        Assert.assertTrue(hgKvIterator.hasNext());
        boolean findKey = false;
        while (hgKvIterator.hasNext()) {
            HgKvEntry entry = hgKvIterator.next();
            if (toStr(entry.key()).equals(key) && toStr(entry.value()).equals(key)) {
                log.info("key={} value={}", toStr(entry.key()), toStr(entry.value()));
                findKey = true;
            }
        }
        Assert.assertTrue(findKey);
    }

    //     @Test
    public void testCleanData() {

        hgCmdClient = new HgCmdClient();
        pdProvider = new DefaultPdProvider(pdAddress);
        hgCmdClient.init(new RpcOptions(), new HgCmdClient.PartitionAgent() {
            @Override
            public Endpoint getPartitionLeader(String graphName, int partitionId) {
                Metapb.Shard shard = pdProvider.getPartitionLeader(graphName, partitionId);
                return JRaftUtils.getEndPoint(
                        pdProvider.getStoreByID(shard.getStoreId()).getRaftAddress());
            }
        });

        storeClient = HgStoreClient.create(PDConfig.of(pdAddress)
                                                   .setEnableCache(true));
        HgStoreSession session = storeClient.openSession(graphName);
        pdClient = storeClient.getPdClient();
        session.createTable(tableName);
        String createGraph = "create_graph";
        HgOwnerKey hgOwnerKey = toOwnerKey(createGraph);
        // Need to write data, then the graph will be created.
        session.put(tableName,
                    hgOwnerKey, createGraph.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(createGraph, toStr(session.get(tableName, hgOwnerKey)));

        Integer partId = 0;

        Metapb.Partition pt = Metapb.Partition.newBuilder().build();
        CleanDataRequest request = new CleanDataRequest();
        request.setGraphName(graphName);
        request.setPartitionId(partId);

        try {
            CleanDataResponse response = hgCmdClient.cleanData(request);
            if (response == null) {
                log.error("response is null ");
            } else if (response.getStatus() == null) {
                log.error("response status is null");
            }

            log.info("response status:{} ", response.getStatus());

            Assert.assertTrue(response.getStatus().isOK());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // @Test
    // TODO: partition not local
    public void testUpdatePartitionLeader() {
        hgCmdClient = new HgCmdClient();
        pdProvider = new DefaultPdProvider(pdAddress);
        hgCmdClient.init(new RpcOptions(), new HgCmdClient.PartitionAgent() {
            @Override
            public Endpoint getPartitionLeader(String graphName, int partitionId) {
                Metapb.Shard shard = pdProvider.getPartitionLeader(graphName, partitionId);
                return JRaftUtils.getEndPoint(
                        pdProvider.getStoreByID(shard.getStoreId()).getRaftAddress());
            }
        });

        storeClient = HgStoreClient.create(PDConfig.of(pdAddress)
                                                   .setEnableCache(true));
        HgStoreSession session = storeClient.openSession(graphName);
        pdClient = storeClient.getPdClient();
        session.createTable(tableName);
        String createGraph = "create_graph";
        HgOwnerKey hgOwnerKey = toOwnerKey(createGraph);
        // Need to write data, then the graph will be created.
        session.put(tableName,
                    hgOwnerKey, createGraph.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(createGraph, toStr(session.get(tableName, hgOwnerKey)));

    }

    // @Test
    // TODO: partition not local
    public void testData() {
        hgCmdClient = new HgCmdClient();
        pdProvider = new DefaultPdProvider(pdAddress);
        hgCmdClient.init(new RpcOptions(), new HgCmdClient.PartitionAgent() {
            @Override
            public Endpoint getPartitionLeader(String graphName, int partitionId) {
                Metapb.Shard shard = pdProvider.getPartitionLeader(graphName, partitionId);
                return JRaftUtils.getEndPoint(
                        pdProvider.getStoreByID(shard.getStoreId()).getRaftAddress());
            }
        });

        storeClient = HgStoreClient.create(PDConfig.of(pdAddress)
                                                   .setEnableCache(true));
        HgStoreSession session = storeClient.openSession("hugegraphtest");
        pdClient = storeClient.getPdClient();
        session.truncate();

        int loop = 3;

        for (int i = 0; i < loop; i++) {
            HgOwnerKey hgOwnerKey = toOwnerKey(i + "owner:" + i, i + "k:" + i);
            session.put(tableName, hgOwnerKey, toIntBytes(i));
        }

        try {
            HgKvIterator<HgKvEntry> iterable = session.scanIterator(tableName);
            int x = 0;
            while (iterable.hasNext()) {
                HgKvEntry entry = iterable.next();
                log.info("data:{}-{}", toStr(entry.key()), entry.value());
                x++;
            }
            log.info("x={}", x);
            Assert.assertEquals(x, loop);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // @Test
    // TODO: partition not local
    public void testCompressionData() {

        hgCmdClient = new HgCmdClient();
        pdProvider = new DefaultPdProvider(pdAddress);
        hgCmdClient.init(new RpcOptions(), new HgCmdClient.PartitionAgent() {
            @Override
            public Endpoint getPartitionLeader(String graphName, int partitionId) {
                Metapb.Shard shard = pdProvider.getPartitionLeader(graphName, partitionId);
                return JRaftUtils.getEndPoint(
                        pdProvider.getStoreByID(shard.getStoreId()).getRaftAddress());
            }
        });

        storeClient = HgStoreClient.create(PDConfig.of(pdAddress)
                                                   .setEnableCache(true));
        HgStoreSession session = storeClient.openSession("hugegraphtest");
        pdClient = storeClient.getPdClient();
        session.truncate();

        int loop = 10;

        for (int i = 0; i < loop; i++) {
            String key = "d41d8cd98f00b204e9800998ecf8427e" + getMd5("a" + i) + getId();
            String value = "10000" + getId() + getId();
            HgOwnerKey hgOwnerKey = toOwnerKey("d41d8cd98f00b204e9800998ecf8427e", key);
            session.put(tableName, hgOwnerKey, value.getBytes());
        }

        try {
            HgKvIterator<HgKvEntry> iterable = session.scanIterator(tableName);
            int x = 0;
            while (iterable.hasNext()) {
                HgKvEntry entry = iterable.next();
                x++;
            }
            log.info("x={}", x);
            Assert.assertEquals(x, loop);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
