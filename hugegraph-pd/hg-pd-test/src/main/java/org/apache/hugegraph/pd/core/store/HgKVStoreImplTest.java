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

package org.apache.hugegraph.pd.core.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.HgKVStoreImpl;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class HgKVStoreImplTest {

    private static final String testPath = "tmp/test";
    private static PDConfig pdConfig;

    @BeforeClass
    public static void init() throws IOException {
        File testFile = new File(testPath);
        if (testFile.exists()) {
            FileUtils.deleteDirectory(testFile);
        }
        FileUtils.forceMkdir(testFile);
        pdConfig = new PDConfig() {{
            setDataPath(testPath);
        }};
    }

    @Test
    public void Test() throws PDException {
        HgKVStore kvStore = new HgKVStoreImpl();
        kvStore.init(pdConfig);

        {
            byte[] key = "hello".getBytes();
            byte[] value = "pd".getBytes();
            kvStore.put(key, value);
        }
        for (int i = 0; i < 100; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }

        Assert.assertEquals(100, kvStore.scanPrefix("k".getBytes()).size());

        kvStore.removeByPrefix("k".getBytes());
        Assert.assertEquals(0, kvStore.scanPrefix("k".getBytes()).size());

        kvStore.close();
    }

    @Test
    public void testScanLogRangeExcludesEnd() throws PDException {
        long timestamp = 1700000000000L;
        Metapb.LogRecord record = Metapb.LogRecord.newBuilder()
                                                 .setAction("range-boundary")
                                                 .setTimestamp(timestamp)
                                                 .build();
        byte[] key = MetadataKeyHelper.getLogKey(record);
        byte[] start = MetadataKeyHelper.getLogKeyPrefix(record.getAction(), 0L);
        byte[] end = MetadataKeyHelper.getLogKeyPrefix(record.getAction(), timestamp + 1L);
        HgKVStore kvStore = new HgKVStoreImpl();
        kvStore.init(pdConfig);
        try {
            kvStore.put(key, record.toByteArray());

            Assert.assertEquals(0, kvStore.scanRange(start, key).size());
            Assert.assertEquals(1, kvStore.scanRange(key, end).size());
            Assert.assertEquals(1, kvStore.scanRange(start, end).size());
        } finally {
            kvStore.close();
        }
    }

    @Test
    public void TestSnapshot() throws PDException {
        HgKVStore kvStore = new HgKVStoreImpl();
        kvStore.init(pdConfig);

        // put 100 data
        for (int i = 0; i < 100; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }
        Assert.assertEquals(100, kvStore.scanPrefix("k".getBytes()).size());

        // save snapshot
        String snapshotPath = Paths.get(testPath, "snapshot").toString();
        kvStore.saveSnapshot(snapshotPath);

        // put another 100 data
        for (int i = 100; i < 200; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }
        Assert.assertEquals(200, kvStore.scanPrefix("k".getBytes()).size());

        // load snapshot
        kvStore.loadSnapshot(snapshotPath);
        Assert.assertEquals(100, kvStore.scanPrefix("k".getBytes()).size());

        // put another 100 data
        for (int i = 100; i < 200; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }
        Assert.assertEquals(200, kvStore.scanPrefix("k".getBytes()).size());

        kvStore.close();
    }

    @Test
    public void testInitWaitsForHeldLock() throws Exception {
        PDConfig config = newConfig("tmp/test-lock-released");
        String dbPath = config.getDataPath() + "/rocksdb";
        FileUtils.forceMkdir(new File(dbPath));
        HgKVStore kvStore = new HgKVStoreImpl();
        try (Options options = new Options().setCreateIfMissing(true)) {
            RocksDB holder = RocksDB.open(options, dbPath);
            // Release the LOCK the way a previous PD JVM does while it is still stopping
            Thread releaser = new Thread(() -> {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                holder.close();
            }, "test-lock-releaser");
            releaser.setDaemon(true);
            releaser.start();
            try {
                long start = System.nanoTime();
                kvStore.init(config);
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                Assert.assertTrue("opened after " + elapsed + " ms", elapsed >= 500L);
                byte[] key = "lock".getBytes(StandardCharsets.UTF_8);
                kvStore.put(key, key);
                Assert.assertArrayEquals(key, kvStore.get(key));
            } finally {
                releaser.join();
                kvStore.close();
                FileUtils.deleteQuietly(new File(config.getDataPath()));
            }
        }
    }

    @Test
    public void testInitFailsWhenLockIsNeverReleased() throws Exception {
        PDConfig config = newConfig("tmp/test-lock-held");
        String dbPath = config.getDataPath() + "/rocksdb";
        FileUtils.forceMkdir(new File(dbPath));
        HgKVStoreImpl kvStore = new HgKVStoreImpl();
        Whitebox.setInternalState(kvStore, "lockWaitTimeoutMs", 1000L);
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB ignored = RocksDB.open(options, dbPath)) {
            long start = System.nanoTime();
            IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
                                                          () -> kvStore.init(config));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue("gave up after " + elapsed + " ms", elapsed >= 1000L);
            Assert.assertTrue(e.getCause() instanceof PDException);
            Assert.assertTrue(e.getCause().getCause() instanceof RocksDBException);
        } finally {
            kvStore.close();
            FileUtils.deleteQuietly(new File(config.getDataPath()));
        }
    }

    @Test
    public void testInitDoesNotRetryOtherOpenFailures() throws Exception {
        PDConfig config = newConfig("tmp/test-lock-not-a-dir");
        // A regular file where the RocksDB directory belongs is not a lock conflict
        File dbFile = new File(config.getDataPath() + "/rocksdb");
        FileUtils.forceMkdir(dbFile.getParentFile());
        FileUtils.touch(dbFile);
        HgKVStore kvStore = new HgKVStoreImpl();
        long start = System.nanoTime();
        try {
            IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
                                                          () -> kvStore.init(config));
            String message = e.getCause().getCause().getMessage();
            Assert.assertTrue(message, message.contains("is not a directory"));
        } finally {
            kvStore.close();
            FileUtils.deleteQuietly(new File(config.getDataPath()));
        }
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("retried for " + elapsed + " ms", elapsed < 5000L);
    }

    @Test(timeout = 60_000L)
    public void testInitWaitsForLockHeldByAnotherProcess() throws Exception {
        PDConfig config = newConfig("tmp/test-process-lock-released");
        String dbPath = config.getDataPath() + "/rocksdb";
        FileUtils.forceMkdir(new File(dbPath));
        HgKVStore kvStore = new HgKVStoreImpl();
        Process holder = startLockHolder(dbPath);
        try {
            CompletableFuture<Void> init = CompletableFuture.runAsync(() -> kvStore.init(config));
            // The helper keeps the LOCK until told otherwise, so init() can only be retrying
            // here; this wait spans the first 500 ms retry interval
            Assert.assertThrows(TimeoutException.class,
                                () -> init.get(1500L, TimeUnit.MILLISECONDS));
            holder.getOutputStream().close();
            init.get(30L, TimeUnit.SECONDS);
            byte[] key = "lock".getBytes(StandardCharsets.UTF_8);
            kvStore.put(key, key);
            Assert.assertArrayEquals(key, kvStore.get(key));
            Assert.assertEquals(0, holder.waitFor());
        } finally {
            holder.destroyForcibly().waitFor();
            kvStore.close();
            FileUtils.deleteQuietly(new File(config.getDataPath()));
        }
    }

    @Test(timeout = 60_000L)
    public void testInitFailsWhenAnotherProcessKeepsLock() throws Exception {
        PDConfig config = newConfig("tmp/test-process-lock-held");
        String dbPath = config.getDataPath() + "/rocksdb";
        FileUtils.forceMkdir(new File(dbPath));
        HgKVStoreImpl kvStore = new HgKVStoreImpl();
        Whitebox.setInternalState(kvStore, "lockWaitTimeoutMs", 1000L);
        Process holder = startLockHolder(dbPath);
        try {
            long start = System.nanoTime();
            IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
                                                          () -> kvStore.init(config));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue("gave up after " + elapsed + " ms", elapsed >= 1000L);
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue(cause instanceof RocksDBException);
            Assert.assertTrue(cause.getMessage(), cause.getMessage().contains("While lock file"));
        } finally {
            holder.destroyForcibly().waitFor();
            kvStore.close();
            FileUtils.deleteQuietly(new File(config.getDataPath()));
        }
    }

    private static Process startLockHolder(String dbPath) throws Exception {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                                             RocksDBLockHolder.class.getName(), dbPath)
                .redirectErrorStream(true)
                .start();
        BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        String line;
        do {
            line = output.readLine();
        } while (line != null && !RocksDBLockHolder.LOCKED.equals(line));
        if (line == null) {
            Assert.fail("Lock holder exited with " + process.waitFor());
        }
        return process;
    }

    private static PDConfig newConfig(String path) throws IOException {
        File dir = new File(path);
        if (dir.exists()) {
            FileUtils.deleteDirectory(dir);
        }
        return new PDConfig() {{
            setDataPath(path);
        }};
    }
}
