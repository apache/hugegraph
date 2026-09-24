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

package org.apache.hugegraph.store.node.grpc;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.buffer.KVByteBuffer;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.node.util.HgAssert;
import org.apache.hugegraph.store.node.util.PropertyUtil;
import org.apache.hugegraph.store.term.Bits;

import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

/**
 * Support parallel read batch query iterator
 */
@Slf4j
public class ParallelScanIterator implements ScanIterator {

    private static final int waitDataMaxTryTimes = 600;
    protected static int maxBodySize =
            PropertyUtil.getInt("app.scan.stream.body.size", 1024 * 1024);
    private final int batchSize = PropertyUtil.getInt("app.scan.stream.entries.size", 20000);
    private final Supplier<KVPair<QueryCondition, ScanIterator>> batchSupplier;
    private final Supplier<Long> limitSupplier;
    private final BlockingQueue<List<KV>> queue;
    private final ReentrantLock queueLock = new ReentrantLock();
    final private ThreadPoolExecutor executor;
    private final ScanQueryRequest query;
    private final Queue<KVScanner> scanners = new LinkedList<>();
    private final Queue<KVScanner> pauseScanners = new LinkedList<>();
    final private List<KV> NO_DATA = new ArrayList<>();
    private final boolean orderVertex;
    private final boolean orderEdge;
    private int maxWorkThreads = Utils.cpus() / 8;
    private int maxInQueue = maxWorkThreads * 2;
    private volatile boolean finished;
    private List<KV> current = null;

    private ParallelScanIterator(Supplier<KVPair<QueryCondition, ScanIterator>> iteratorSupplier,
                                 Supplier<Long> limitSupplier,
                                 ScanQueryRequest query,
                                 ThreadPoolExecutor executor) {
        this.executor = executor;
        this.batchSupplier = iteratorSupplier;
        this.limitSupplier = limitSupplier;
        this.finished = false;
        this.query = query;
        orderVertex = query.getOrderType() == ScanOrderType.ORDER_STRICT;
        orderEdge = query.getOrderType() == ScanOrderType.ORDER_WITHIN_VERTEX;
        if (orderVertex) {
            this.maxWorkThreads = 1;
        } else {
            this.maxWorkThreads =
                    Math.max(1, Math.min(query.getConditionCount() / 16, maxWorkThreads));
        }
        this.maxInQueue = maxWorkThreads * 2;
        // Edge sorted requires a larger queue
        queue = new LinkedBlockingQueue<>(maxInQueue * 2);
        createScanner();
    }

    public static ParallelScanIterator of(
            Supplier<KVPair<QueryCondition, ScanIterator>> iteratorSupplier,
            Supplier<Long> limitSupplier,
            ScanQueryRequest query,
            ThreadPoolExecutor executor) {
        HgAssert.isArgumentNotNull(iteratorSupplier, "iteratorSupplier");
        HgAssert.isArgumentNotNull(limitSupplier, "limitSupplier");
        return new ParallelScanIterator(iteratorSupplier, limitSupplier, query, executor);
    }

    @Override
    public boolean hasNext() {
        int tryTimes = 0;
        while (current == null && tryTimes < waitDataMaxTryTimes) {
            try {
                if (queue.size() != 0 || !finished) {
                    current = queue.poll(100,
                                         TimeUnit.MILLISECONDS);  // Regularly check if the
                    // client has been closed.
                    if (current == null && !finished) {
                        wakeUpScanner();
                    }
                } else {
                    break;
                }
            } catch (InterruptedException e) {
                log.error("hasNext interrupted {}", e);
                break;
            }
            tryTimes++;
        }
        if (current == null && tryTimes >= waitDataMaxTryTimes) {
            log.error("Wait data timeout!!!, scanner is {}/{}", scanners.size(),
                      pauseScanners.size());
        }
        return current != null && current != NO_DATA;
    }

    @Override
    public boolean isValid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KV> next() {
        List<KV> t = current;
        current = null;
        if (queue.size() < maxWorkThreads) {
            wakeUpScanner();
        }
        return t;
    }

    @Override
    public void close() {
        finished = true;
        synchronized (scanners) {
            scanners.forEach(scanner -> {
                scanner.close();
            });
        }
        synchronized (pauseScanners) {
            pauseScanners.forEach(s -> {
                s.close();
            });
        }
        queue.clear();
    }

    /**
     * Create Scanner
     */
    private void createScanner() {
        synchronized (scanners) {
            for (int i = 0; i < maxWorkThreads; i++) {
                scanners.add(new KVScanner());
            }
            scanners.forEach(scanner -> {
                executor.execute(() -> scanner.scanKV());
            });
        }
    }

    /**
     * Wake up scanner
     */
    private void wakeUpScanner() {
        synchronized (pauseScanners) {
            if (!pauseScanners.isEmpty()) {
                KVScanner scanner = pauseScanners.poll();
                if (scanner != null) {
                    executor.execute(() -> scanner.scanKV());
                }
            }
        }
    }

    /**
     * Sleep Scanner
     *
     * @param scanner
     */
    private void suspendScanner(KVScanner scanner) {
        synchronized (pauseScanners) {
            pauseScanners.add(scanner);
        }
    }

    private void quitScanner(KVScanner scanner) {
        synchronized (scanners) {
            scanner.close();
            scanners.remove(scanner);
            if (scanners.size() == 0) {
                putData(NO_DATA);
                this.finished = true;
            }
        }
    }

    /**
     * Add to queue, return whether the queue is full
     *
     * @param data
     * @return false: Queue is full
     */
    private boolean putData(List<KV> data) {
        try {
            this.queue.put(data);
        } catch (InterruptedException e) {
            log.error("exception ", e);
            this.finished = true;
            return false;
        }
        return this.queue.size() < maxInQueue;
    }

    private boolean putData(List<KV> data, boolean hasNext) {
        try {
            queueLock.lock();
            this.queue.put(data);
        } catch (InterruptedException e) {
            log.error("exception ", e);
            this.finished = true;
            return false;
        } finally {
            if (!hasNext) {
                queueLock.unlock();
            }
        }
        // Data not ended, thread continues to execute
        return hasNext || this.queue.size() < maxInQueue;
    }

    private synchronized KVPair<QueryCondition, ScanIterator> getIterator() {
        return this.batchSupplier.get();
    }

    private long getLimit() {
        Long limit = this.limitSupplier.get();
        if (limit == null || limit <= 0) {
            limit = Long.valueOf(Integer.MAX_VALUE);
        }
        return limit;
    }

    static class KV {

        public int sn;
        public byte[] key;
        public byte[] value;

        public boolean hasSN = false;

        public static KV of(RocksDBSession.BackendColumn col) {
            KV kv = new KV();
            kv.key = col.name;
            kv.value = col.value;
            return kv;
        }

        public static KV ofSeparator(int value) {
            KV kv = new KV();
            kv.key = new byte[4];
            Bits.putInt(kv.key, 0, value);
            return kv;
        }

        public KV setNo(int sn) {
            this.sn = sn;
            hasSN = true;
            return this;
        }

        public void write(KVByteBuffer buffer) {
            if (hasSN) {
                buffer.putInt(sn);
            }
            buffer.put(key);
            buffer.put(value);
        }

        public int size() {
            return this.key.length + this.value.length + 1;
        }
    }

    class KVScanner {

        private final ReentrantLock iteratorLock = new ReentrantLock();
        private ScanIterator iterator = null;
        private QueryCondition query = null;
        private long limit;
        private long counter;
        private volatile boolean closed = false;

        private ScanIterator getIterator() {
            // Iterator has no data, or the point has reached the limit, switch to a new iterator.
            if (iterator == null || !iterator.hasNext() || counter >= limit) {
                if (iterator != null) {
                    iterator.close();
                }
                KVPair<QueryCondition, ScanIterator> pair = ParallelScanIterator.this.getIterator();
                query = pair.getKey();
                iterator = pair.getValue();
                limit = getLimit();
                counter = 0;
            }
            return iterator;
        }

        public void scanKV() {
            boolean canNext = true;
            ArrayList<KV> dataList = new ArrayList<>(batchSize);
            dataList.ensureCapacity(batchSize);
            iteratorLock.lock();
            try {
                long entriesSize = 0, bodySize = 0;
                while (canNext && !closed) {
                    iterator = this.getIterator();
                    if (iterator == null) {
                        break;
                    }
                    while (iterator.hasNext() && entriesSize < batchSize &&
                           bodySize < maxBodySize &&
                           counter < limit && !closed) {
                        KV kv = KV.of(iterator.next());
                        dataList.add(orderVertex ? kv.setNo(query.getSerialNo()) : kv);
                        bodySize += kv.size();
                        entriesSize++;
                        counter++;
                    }
                    if ((entriesSize >= batchSize || bodySize >= maxBodySize) ||
                        (orderEdge && bodySize >= maxBodySize / 2)) {
                        if (orderEdge) {
                            // Sort the edges, ensure all edges of one point are consecutive,
                            // prevent other points from inserting.
                            canNext = putData(dataList, iterator != null && iterator.hasNext());
                        } else {
                            canNext = putData(dataList);
                        }
                        dataList = new ArrayList<>(batchSize);
                        dataList.ensureCapacity(batchSize);
                        entriesSize = bodySize = 0;
                    }
                }
                if (!dataList.isEmpty()) {
                    if (orderEdge) {
                        putData(dataList, false);
                    } else {
                        putData(dataList);
                    }
                }
            } catch (Exception e) {
                log.error("exception {}", e);
            } finally {
                iteratorLock.unlock();
                if (iterator != null && counter < limit && !closed) {
                    suspendScanner(this);
                } else {
                    quitScanner(this);
                }
            }
        }

        public void close() {
            closed = true;
            iteratorLock.lock();
            try {
                if (iterator != null) {
                    iterator.close();
                }
            } finally {
                iteratorLock.unlock();
            }
        }
    }
}
