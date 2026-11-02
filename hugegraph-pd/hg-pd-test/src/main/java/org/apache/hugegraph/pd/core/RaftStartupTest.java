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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.RaftStartup;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class RaftStartupTest {

    @Test
    public void testRaftDoesNotStartBeforeMetadataOpens() throws Exception {
        String dataPath = "tmp/test-raft-startup";
        PDConfig config = newConfig(dataPath, "127.0.0.1:0");
        // A regular file where the metadata RocksDB directory belongs makes its open fail
        FileUtils.forceMkdir(new File(dataPath));
        FileUtils.touch(new File(dataPath, "rocksdb"));
        withFactoryStore(null, () -> {
            IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
                                                          () -> RaftStartup.init(config));
            Assert.assertTrue(e.getMessage(),
                              e.getMessage().startsWith("Failed to open PD metadata"));
            Assert.assertFalse(new File(dataPath, "raft").exists());
            Assert.assertNull(RaftEngine.getInstance().getRaftNode());
        });
    }

    @Test
    public void testRaftFailureReleasesMetadataItOpened() throws Exception {
        String dataPath = "tmp/test-raft-startup-owned";
        // An address jraft cannot parse makes Raft fail after metadata has opened
        PDConfig config = newConfig(dataPath, "not-a-peer");
        withFactoryStore(null, () -> {
            Assert.assertThrows(IllegalArgumentException.class, () -> RaftStartup.init(config));
            Assert.assertNull(factoryStore());
            Assert.assertNull(RaftEngine.getInstance().getRaftNode());
            Assert.assertNull(Whitebox.getInternalState(RaftEngine.getInstance(), "raftRpcClient"));
            // The metadata LOCK is free again
            try (Options options = new Options();
                 RocksDB ignored = RocksDB.open(options, dataPath + "/rocksdb")) {
                Assert.assertNotNull(ignored);
            }
        });
    }

    @Test
    public void testRaftFailureKeepsMetadataOpenedByAnotherOwner() throws Exception {
        PDConfig config = newConfig("tmp/test-raft-startup-shared", "not-a-peer");
        HgKVStore shared = mock(HgKVStore.class);
        withFactoryStore(shared, () -> {
            Assert.assertThrows(IllegalArgumentException.class, () -> RaftStartup.init(config));
            Assert.assertSame(shared, factoryStore());
            verify(shared, never()).close();
            // Raft built its rpc client before failing on the address; it is stopped even
            // though the metadata store belongs to someone else
            Assert.assertNull(Whitebox.getInternalState(RaftEngine.getInstance(), "raftRpcClient"));
        });
    }

    @Test
    public void testRaftFailureUnregistersMetadataHandler() throws Exception {
        PDConfig config = newConfig("tmp/test-raft-startup-handler", "not-a-peer");
        // With Raft enabled the metadata store registers itself as a raft task handler
        config.getRaft().setEnable(true);
        config.getRaft().setPeersList("not-a-peer");
        Object stateMachine = Whitebox.getInternalState(RaftEngine.getInstance(), "stateMachine");
        List<?> handlers = Whitebox.getInternalState(stateMachine, "taskHandlers");
        int before = handlers.size();
        withFactoryStore(null, () -> {
            Assert.assertThrows(IllegalArgumentException.class, () -> RaftStartup.init(config));
            Assert.assertNull(factoryStore());
            Assert.assertEquals(before, handlers.size());
        });
    }

    private static PDConfig newConfig(String dataPath, String raftAddress) throws Exception {
        FileUtils.deleteQuietly(new File(dataPath));
        PDConfig config = new PDConfig();
        config.setDataPath(dataPath);
        PDConfig.Raft raft = config.new Raft();
        raft.setDataPath(dataPath + "/raft");
        raft.setAddress(raftAddress);
        config.setRaft(raft);
        return config;
    }

    private static Object factoryStore() throws Exception {
        return factoryField("store").get(null);
    }

    private static Field factoryField(String name) throws Exception {
        Field field = MetadataFactory.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static void withFactoryStore(HgKVStore initial, Body body) throws Exception {
        Assert.assertNull(RaftEngine.getInstance().getRaftNode());
        Field storeField = factoryField("store");
        Field shutdownField = factoryField("shutdown");
        synchronized (MetadataFactory.class) {
            Object previous = storeField.get(null);
            boolean shutdown = shutdownField.getBoolean(null);
            try {
                storeField.set(null, initial);
                shutdownField.setBoolean(null, false);
                body.run();
            } finally {
                Object left = storeField.get(null);
                if (left != null && left != initial) {
                    ((HgKVStore) left).close();
                }
                storeField.set(null, previous);
                shutdownField.setBoolean(null, shutdown);
                FileUtils.deleteQuietly(new File("tmp/test-raft-startup"));
                FileUtils.deleteQuietly(new File("tmp/test-raft-startup-owned"));
                FileUtils.deleteQuietly(new File("tmp/test-raft-startup-shared"));
                FileUtils.deleteQuietly(new File("tmp/test-raft-startup-handler"));
            }
        }
    }

    private interface Body {

        void run() throws Exception;
    }
}
