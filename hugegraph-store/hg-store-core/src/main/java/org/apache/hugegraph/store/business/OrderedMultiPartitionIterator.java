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

package org.apache.hugegraph.store.business;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Function;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;

public final class OrderedMultiPartitionIterator implements ScanIterator {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final List<Integer> partitionIds;
    private final Function<Integer, ScanIterator> supplier;
    private final List<SourceEntry> sources;
    private final PriorityQueue<SourceEntry> queue;

    private boolean initialized;
    private boolean closed;
    private Integer currentPartitionId;

    private OrderedMultiPartitionIterator(List<Integer> partitionIds,
                                          Function<Integer, ScanIterator> supplier) {
        this.partitionIds = new ArrayList<>(Objects.requireNonNull(partitionIds));
        Collections.sort(this.partitionIds);
        this.supplier = Objects.requireNonNull(supplier);
        this.sources = new ArrayList<>(this.partitionIds.size());
        this.queue = new PriorityQueue<>((left, right) -> {
            int result = Arrays.compareUnsigned(left.entry.name,
                                                right.entry.name);
            if (result != 0) {
                return result;
            }
            return Integer.compare(left.partitionId, right.partitionId);
        });
        this.initialized = false;
        this.closed = false;
        this.currentPartitionId = null;
    }

    public static OrderedMultiPartitionIterator of(
            List<Integer> partitionIds,
            Function<Integer, ScanIterator> supplier) {
        return new OrderedMultiPartitionIterator(partitionIds, supplier);
    }

    @Override
    public boolean hasNext() {
        if (this.closed) {
            return false;
        }
        this.initialize();
        if (this.queue.isEmpty()) {
            this.close();
            return false;
        }
        return true;
    }

    @Override
    public boolean isValid() {
        return this.hasNext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        SourceEntry source = this.queue.poll();
        BackendColumn current = source.entry;
        this.currentPartitionId = source.partitionId;
        try {
            if (source.iterator.hasNext()) {
                source.entry = source.iterator.next();
                this.queue.add(source);
            } else {
                this.closeSource(source);
            }
        } catch (RuntimeException | Error e) {
            this.closeAfterFailure(e);
            throw e;
        }
        return (T) current;
    }

    @Override
    public long count() {
        long count = 0L;
        while (this.hasNext()) {
            this.next();
            count++;
        }
        return count;
    }

    @Override
    public byte[] position() {
        if (this.currentPartitionId == null) {
            return EMPTY_BYTES;
        }
        return ByteBuffer.allocate(Integer.BYTES)
                         .putInt(this.currentPartitionId)
                         .array();
    }

    @Override
    public void seek(byte[] position) {
        if (position == null || position.length == 0) {
            return;
        }
        throw new UnsupportedOperationException(
                "Ordered scans resume from their physical start key");
    }

    @Override
    public void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        Throwable failure = null;
        for (SourceEntry source : this.sources) {
            try {
                this.closeSource(source);
            } catch (RuntimeException | Error e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        this.queue.clear();
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    private void initialize() {
        if (this.initialized) {
            return;
        }
        this.initialized = true;
        try {
            for (int partitionId : this.partitionIds) {
                ScanIterator iterator = this.supplier.apply(partitionId);
                if (iterator == null) {
                    continue;
                }
                SourceEntry source = new SourceEntry(partitionId, iterator);
                this.sources.add(source);
                if (iterator.hasNext()) {
                    source.entry = iterator.next();
                    this.queue.add(source);
                } else {
                    this.closeSource(source);
                }
            }
        } catch (RuntimeException | Error e) {
            this.closeAfterFailure(e);
            throw e;
        }
    }

    private void closeSource(SourceEntry source) {
        if (source.closed) {
            return;
        }
        source.closed = true;
        source.iterator.close();
    }

    private void closeAfterFailure(Throwable failure) {
        try {
            this.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static final class SourceEntry {

        private final int partitionId;
        private final ScanIterator iterator;
        private BackendColumn entry;
        private boolean closed;

        private SourceEntry(int partitionId, ScanIterator iterator) {
            this.partitionId = partitionId;
            this.iterator = iterator;
            this.entry = null;
            this.closed = false;
        }
    }
}
