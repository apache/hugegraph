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

package org.apache.hugegraph.store.client.grpc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvOrderedIterator;
import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.client.util.PropertyUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * Batch query result merging, blocking queue working mode
 * Splitting of request tasks, creating multiple request queues
 */
@Slf4j
public class KvBatchScannerMerger implements KvCloseableIterator<HgKvIterator<HgKvEntry>>,
                                             HgPageSize {

    static int maxWaitCount = PropertyUtil.getInt("net.kv.scanner.wait.timeout", 60);
    protected final BlockingQueue<Supplier<HgKvIterator<HgKvEntry>>> queue =
            new LinkedBlockingQueue<>();
    private final KvBatchScanner.TaskSplitter taskSplitter;
    private final List<KvBatchScanner> scanners = new CopyOnWriteArrayList<>();
    private Supplier<HgKvIterator<HgKvEntry>> current = null;

    public KvBatchScannerMerger(KvBatchScanner.TaskSplitter splitter) {
        this.taskSplitter = splitter;
        splitter.setNotifier(this);
    }

    public void startTask() {
        taskSplitter.splitTask();
    }

    public void dataArrived(KvBatchScanner scanner, Supplier<HgKvIterator<HgKvEntry>> supplier)
            throws InterruptedException {
        queue.put(supplier);
    }

    @Override
    public boolean hasNext() {
        int waitTime = 0;
        while (current == null) {
            try {
                // Queue has data, and there are active queryers, tasks not yet allocated.
                if (queue.size() != 0 || scanners.size() > 0 || !taskSplitter.isFinished()) {
                    current = queue.poll(1,
                                         TimeUnit.SECONDS);  // Regularly check if the client has
                    // been closed.
                } else {
                    break;
                }
                if (current == null) {
                    // Timeout retry
                    sendTimeout();
                    if (++waitTime > maxWaitCount) {
                        log.error(
                                "KvBatchScanner wait data timeout {}, closeables is {}, task is {}",
                                waitTime, scanners.size(), taskSplitter.isFinished());
                        break;
                    }
                }
            } catch (InterruptedException e) {
                log.error("hasNext interrupted {}", e);
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return current != null && current != KvBatchScanner.NO_DATA;
    }

    @Override
    public HgKvIterator<HgKvEntry> next() {
        HgKvIterator<HgKvEntry> iterator = null;
        if (current != null) {
            iterator = current.get();
        }
        current = null;
        return iterator;
    }

    @Override
    public void close() {
        taskSplitter.close();
        scanners.forEach(c -> c.close());
    }

    private void sendTimeout() {
        scanners.forEach(v -> {
            v.sendResponse();
        });
    }

    @Override
    public long getPageSize() {
        return 0;
    }

    public void registerScanner(KvBatchScanner closeable) {
        this.scanners.add(closeable);
    }

    /**
     * Return value < 0 indicates the task is finished.
     *
     * @param closeable
     * @return
     */
    public int unregisterScanner(KvBatchScanner closeable) {
        this.scanners.remove(closeable);
        try {
            taskSplitter.splitTask();
        } catch (Exception e) {
            log.error("exception ", e);
        }
        return taskSplitter.isFinished() && this.scanners.size() == 0 ?
               -1 : this.scanners.size();
    }

    public int getScannerCount() {
        return this.scanners.size();
    }

    /**
     * Assemble multiple ordered iterators of a Scanner into one iterator
     */
    static class ScannerDataQueue {

        private BlockingQueue<Supplier<HgKvIterator<HgKvEntry>>> queue;
        private HgKvOrderedIterator<HgKvEntry> iterator = null;
        private int currentSN = 0;
        private HgKvEntry entry;

        public ScannerDataQueue() {
            queue = new LinkedBlockingQueue<>();
        }

        public int sn() {
            return currentSN;
        }

        public void add(Supplier<HgKvIterator<HgKvEntry>> supplier) {
            if (queue != null) {
                queue.add(supplier);
            }
        }

        /**
         * Whether the iterator is valid, if there is no data, wait for the data to arrive.
         *
         * @return
         */
        public boolean hasNext() {
            while (entry == null && queue != null) {
                try {
                    int waitTime = 0;
                    Supplier<HgKvIterator<HgKvEntry>> current;
                    current = queue.poll(1,
                                         TimeUnit.SECONDS);  // Regularly check if the client has
                    // been closed.
                    if (current == null) {
                        if (++waitTime > maxWaitCount) {
                            break;
                        }
                    } else if (current == KvBatchScanner.NO_DATA) {
                        queue = null;
                        break;
                    } else {
                        iterator = (HgKvOrderedIterator<HgKvEntry>) current.get();
                        if (iterator != null && iterator.hasNext()) {
                            moveNext();
                        } else {
                            iterator = null;
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("hasNext interrupted {}", e);
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
            return entry != null;
        }

        public HgKvEntry next() {
            HgKvEntry current = entry;
            moveNext();
            return current;
        }

        private void moveNext() {
            if (iterator.hasNext()) {
                entry = iterator.next();
                currentSN = (int) iterator.getSequence();
            } else {
                entry = null;
                iterator = null;
            }
        }
    }

    /**
     * Merge sort for multiple Scanner return results
     */
    static class SortedScannerMerger extends KvBatchScannerMerger {

        // Each stream corresponds to a receive queue
        private final Map<KvBatchScanner, ScannerDataQueue> scannerQueues =
                new ConcurrentHashMap<>();

        public SortedScannerMerger(KvBatchScanner.TaskSplitter splitter) {
            super(splitter);
            queue.add(() -> {
                // Perform merge sort on the store's return result
                return new HgKvIterator<HgKvEntry>() {
                    private ScannerDataQueue iterator;
                    private int currentSN = 0;
                    private HgKvEntry entry;

                    @Override
                    public byte[] key() {
                        return entry.key();
                    }

                    @Override
                    public byte[] value() {
                        return entry.value();
                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public byte[] position() {
                        return new byte[0];
                    }

                    @Override
                    public void seek(byte[] position) {
                        throw new RuntimeException("not implemented");
                    }

                    @Override
                    public boolean hasNext() {
                        //
                        if (iterator == null || !iterator.hasNext() || currentSN != iterator.sn()) {
                            iterator = selectIterator();
                        }

                        if (iterator != null) {
                            currentSN = iterator.sn();
                        }
                        return iterator != null;
                    }

                    @Override
                    public HgKvEntry next() {
                        entry = iterator.next();
                        return entry;
                    }
                };
            });
        }

        /**
         * Pick an iterator with the smallest sn from multiple Scanners
         * If Scanner has no data, wait for the data to arrive.
         *
         * @return
         */
        private ScannerDataQueue selectIterator() {
            int sn = Integer.MAX_VALUE;
            ScannerDataQueue current = null;
            while (current == null && !scannerQueues.isEmpty()) {
                Iterator<KvBatchScanner> itr = scannerQueues.keySet().iterator();
                while (itr.hasNext()) {
                    KvBatchScanner key = itr.next();
                    ScannerDataQueue kvItr = scannerQueues.get(key);
                    if (!kvItr.hasNext()) {
                        scannerQueues.remove(key);
                        continue;
                    }
                    if (kvItr.sn() <= sn) {
                        sn = kvItr.sn();
                        current = kvItr;
                    }
                }
            }
            return current;
        }

        @Override
        public void registerScanner(KvBatchScanner scanner) {
            super.registerScanner(scanner);
            scannerQueues.putIfAbsent(scanner, new ScannerDataQueue());
        }

        @Override
        public int unregisterScanner(KvBatchScanner scanner) {
            dataArrived(scanner, KvBatchScanner.NO_DATA);
            return super.unregisterScanner(scanner);
        }

        @Override
        public void dataArrived(KvBatchScanner scanner,
                                Supplier<HgKvIterator<HgKvEntry>> supplier) {
            scannerQueues.putIfAbsent(scanner, new ScannerDataQueue());
            scannerQueues.get(scanner).add(supplier);
        }
    }
}
