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

package org.apache.hugegraph.store.util;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.MetricX;

public class HgStoreTestUtil {
    public static final String GRAPH_NAME = "default/hugegraph/g";
    public static final String GRAPH_NAME2 = "default/hugegraph2/g";
    public static final String TABLE_NAME = "unit-table";
    public static final String TABLE_NAME2 = "unit-table-2";

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String keyPrefix) {
        return batchPut(session, keyPrefix, 100);
    }

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String keyPrefix,
                                                   int loop) {
        return batchPut(session, TABLE_NAME, keyPrefix, loop);
    }

    public static Map<HgOwnerKey, byte[]> batchPut(
            HgStoreSession session, String tableName, String keyPrefix, int loop) {
        return batchPut(session, tableName, keyPrefix, loop, 1, key -> toOwnerKey(key));
    }

    public static Map<HgOwnerKey, byte[]> batchPut(
            HgStoreSession session, String tableName, byte[] keyPrefix, int loop) {
        return batchPut(session, tableName, keyPrefix, loop, 1,
                        (prefix, key) -> toOwnerKey(prefix, key));
    }

    public static Map<HgOwnerKey, byte[]> batchPut(
            HgStoreSession session, String tableName, String keyPrefix, int loop, int start) {
        return batchPut(session, tableName, keyPrefix, loop, start, key -> toOwnerKey(key));
    }

    public static Map<HgOwnerKey, byte[]> batchPut(
            HgStoreSession session, String tableName, String keyPrefix, int loop,
            Function<String, HgOwnerKey> f) {
        return batchPut(session, tableName, keyPrefix, loop, 1, f);
    }

    public static Map<HgOwnerKey, byte[]> batchPut(
            HgStoreSession session,
            String tableName,
            String keyPrefix,
            int loop,
            int start,
            Function<String, HgOwnerKey> f) {

        Map<HgOwnerKey, byte[]> res = new LinkedHashMap<>();

        int length = String.valueOf(loop).length();

        session.beginTx();
        for (int i = start; i <= loop; i++) {

            HgOwnerKey key = f.apply(keyPrefix + "-" + padLeftZeros(String.valueOf(i), length));

            byte[] value = toBytes(keyPrefix + "-V-" + i);
            res.put(key, value);
            session.put(tableName, key, value);

            if ((i + 1) % 10000 == 0) {
                println("commit: " + (i + 1));
                session.commit();
                session.beginTx();
            }
        }
        if (session.isTx()) {
            session.commit();
        }

        return res;
    }

    public static Map<HgOwnerKey, byte[]> batchPut(
            HgStoreSession session,
            String tableName,
            byte[] keyPrefix,
            int loop,
            int start,
            BiFunction<byte[], String, HgOwnerKey> f) {

        Map<HgOwnerKey, byte[]> res = new LinkedHashMap<>();

        int length = String.valueOf(loop).length();

        session.beginTx();
        for (int i = start; i <= loop; i++) {

            HgOwnerKey key = f.apply(keyPrefix, padLeftZeros(String.valueOf(i), length));

            byte[] value = toBytes(keyPrefix + "-V-" + i);
            res.put(key, value);
            session.put(tableName, key, value);

            if ((i + 1) % 10000 == 0) {
                println("commit: " + (i + 1));
                session.commit();
                session.beginTx();
            }
        }
        if (session.isTx()) {
            session.commit();
        }

        return res;
    }

    /*---- common -----*/
    public static void printOwner(List<HgOwnerKey> list) {
        if (list == null) {
            return;
        }

        for (HgOwnerKey entry : list) {
            println(entry);
        }
    }

    public static void printNum(List<HgKvEntry> list, String title) {
        if (list == null) {
            return;
        }

        println(title + " size: " + list.size());
    }

    public static int println(Iterable<HgKvIterator<HgKvEntry>> iterators) {
        AtomicInteger counter = new AtomicInteger();
        iterators.forEach(e -> counter.addAndGet(HgStoreTestUtil.println(e)));
        return counter.get();
    }

    public static int println(HgKvIterator<HgKvEntry> iterator) {
        if (iterator == null) {
            return 0;
        }

        AtomicInteger counter = new AtomicInteger();

        while (iterator.hasNext()) {
            counter.incrementAndGet();
            print(iterator.next());
            println(" " + Arrays.toString(iterator.position()));
        }

        iterator.close();

        return counter.get();
    }

    public static void println(HgKvIterator<HgKvEntry> iterator, Function<Integer, Boolean> mod) {
        if (iterator == null) {
            return;
        }
        int count = 0;

        while (iterator.hasNext()) {
            count++;
            if (mod.apply(count)) {
                print(iterator.next());
                println(" " + Arrays.toString(iterator.position()));
            }
        }

        iterator.close();
    }

    public static void println(List<HgKvEntry> list) {
        if (list == null) {
            return;
        }

        for (HgKvEntry entry : list) {
            println(entry);
        }
    }

    public static void println(List<HgKvEntry> list, int mod) {
        if (list == null) {
            return;
        }

        for (int i = 0; i < list.size(); i++) {
            if (i % mod == 0) {
                println(list.get(i));
            }
        }
    }

    public static void println(HgKvEntry kv) {
        if (kv == null) {
            System.out.println("null");
            return;
        }
        println("[ " + kv.code() + " " + toStr(kv.key()) + " : " + toStr(kv.value()) + " ]");
    }

    public static void println(HgOwnerKey key) {
        if (key == null) {
            System.out.println("null");
            return;
        }
        println("[ " + toInt(key.getOwner()) + " : " + toStr(key.getKey()) + " ]");
    }

    public static void println(String str) {
        System.out.println(str);
    }

    public static void println(Number num) {
        System.out.println(num);
    }

    public static void print(String str) {
        System.out.print(str);
    }

    public static void print(HgKvEntry kv) {
        if (kv == null) {
            System.out.println("null");
            return;
        }
        print("[ " + kv.code() + " " + toStr(kv.key()) + " : " + toStr(kv.value()) + " ]");
    }

    private static byte[] getOwner(String key) {
        return getOwner(toBytes(key));
    }

    private static byte[] getOwner(byte[] key) {
        return toBytes(Arrays.hashCode(key));
    }

    public static HgOwnerKey toAllPartitionKey(String key) {
        return HgOwnerKey.of(HgStoreClientConst.ALL_PARTITION_OWNER, toBytes(key));
    }

    public static HgOwnerKey toAllPartitionKey(byte[] prefix, String key) {
        return HgOwnerKey.of(HgStoreClientConst.ALL_PARTITION_OWNER, toBytes(prefix, key));
    }

    public static HgOwnerKey toAllPartitionKey(byte[] prefix) {
        return HgOwnerKey.of(HgStoreClientConst.ALL_PARTITION_OWNER, prefix);
    }

    public static HgOwnerKey toOwnerKey(byte[] prefix, String key) {
        byte[] newKey = toBytes(prefix, key);
        return new HgOwnerKey(getOwner(newKey), newKey);
    }

    public static HgOwnerKey toOwnerKey(String key) {
        return new HgOwnerKey(getOwner(key), toBytes(key));
    }

    public static HgOwnerKey toOwnerKey(byte[] key) {
        return new HgOwnerKey(getOwner(key), key);
    }

    public static HgOwnerKey toOwnerKey(String owner, String key) {
        return HgOwnerKey.of(toBytes(owner), toBytes(key));
    }

    public static HgOwnerKey toOwnerKey(int code, String key) {
        return HgOwnerKey.of(code, toBytes(key));
    }

    public static String toStr(byte[] b) {
        if (b == null) {
            return "";
        }
        if (b.length == 0) {
            return "";
        }
        return new String(b, StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(byte[] prefix, String str) {
        if (str == null) {
            return null;
        }
        byte[] buf = str.getBytes(StandardCharsets.UTF_8);
        byte[] res = new byte[buf.length + prefix.length];
        System.arraycopy(prefix, 0, res, 0, prefix.length);
        System.arraycopy(buf, 0, res, prefix.length, buf.length);
        return res;
    }

    public static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(l);
        return buffer.array();
    }

    private static byte[] toBytes(final int i) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(i);
        return buffer.array();
    }

    public static long toLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    public static int toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getInt();
    }

    public static String padLeftZeros(String str, int n) {
        return String.format("%1$" + n + "s", str).replace(' ', '0');
    }

    public static String toSuffix(int num, int length) {
        return "-" + padLeftZeros(String.valueOf(num), length);
    }

    public static int amountOf(List list) {
        if (list == null) {
            return 0;
        }
        return list.size();
    }

    public static int amountOf(Iterator iterator) {
        if (iterator == null) {
            return 0;
        }
        int count = 0;
        while (iterator.hasNext()) {
            Object ignore = iterator.next();
            ++count;
        }
        return count;
    }

    public static int oOMAmountOf(Iterator iterator) {
        if (iterator == null) {
            return 0;
        }
        int count = 0;
        while (iterator.hasNext()) {
            Object obj = iterator.next();
            ++count;
            if (count % 10000 == 0) {
                println(count);
                sleeping(10);
            }
            if (count % 100000 == 0) {
                break;
            }
        }
        if (iterator instanceof Closeable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    public static int amountIn(List<? extends Iterator> iterators) {
        return iterators.stream().map(e -> HgStoreTestUtil.amountOf(e)).reduce(0, Integer::sum);
    }

    public static void sleeping(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void parallelTest(int threads, Runnable runner,
                                    Consumer<Throwable> throwableConsumer) {
        int threadsAmount = threads;
        CountDownLatch countDownLatch = new CountDownLatch(threadsAmount);
        ExecutorService pool = new ThreadPoolExecutor(threadsAmount, threadsAmount + 20,
                                                      200, TimeUnit.SECONDS,
                                                      new ArrayBlockingQueue<>(1000));
        for (int i = 0; i < threadsAmount; i++) {
            pool.submit(
                    () -> {
                        try {
                            runner.run();
                        } catch (Throwable t) {
                            throwableConsumer.accept(t);
                        }
                        countDownLatch.countDown();
                    });
        }

        try {
            countDownLatch.await();
            pool.shutdown();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void repeatedlyTest(int times, Runnable runner) {
        MetricX metrics = MetricX.ofStart();

        for (int j = 0; j < times; j++) {
            try {
                runner.run();
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            } catch (Throwable t) {
                metrics.countFail();
                t.printStackTrace();
            }
        }
        metrics.end();
        System.out.println("*************************************************");
        System.out.println("*************  Repeatedly Test Completed  **************");
        System.out.println("Total: " + metrics.past() / 1000 + " sec.");
        System.out.println("Repeated: " + times + " times.");
        System.out.println("Fail: " + metrics.getFailureCount() + " times.");
        System.out.println("*************************************************");
    }

    public static void runWaiting() {
        while (true) {
            sleeping(1000);
        }
    }
}
