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

package org.apache.hugegraph.store.client;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;

final class OrderedKvIterator implements HgKvIterator<HgKvEntry> {

    private final List<? extends HgKvIterator<? extends HgKvEntry>> iterators;
    private final PriorityQueue<SourceEntry> queue;
    private final boolean[] sourceClosed;
    private final long limit;

    private boolean initialized;
    private boolean closed;
    private long count;
    private HgKvEntry current;
    private byte[] position;

    OrderedKvIterator(List<? extends HgKvIterator<? extends HgKvEntry>> iterators,
                      long limit) {
        this.iterators = iterators;
        this.queue = new PriorityQueue<>((left, right) -> {
            int result = Arrays.compareUnsigned(left.entry.key(),
                                                right.entry.key());
            if (result != 0) {
                return result;
            }
            return Integer.compare(left.source, right.source);
        });
        this.sourceClosed = new boolean[iterators.size()];
        this.limit = limit <= HgStoreClientConst.NO_LIMIT ? Long.MAX_VALUE :
                     limit;
        this.initialized = false;
        this.closed = false;
        this.count = 0L;
        this.current = null;
        this.position = HgStoreClientConst.EMPTY_BYTES;
    }

    @Override
    public boolean hasNext() {
        if (this.closed) {
            return false;
        }
        this.initialize();
        boolean hasNext = this.count < this.limit && !this.queue.isEmpty();
        if (!hasNext) {
            this.close();
        }
        return hasNext;
    }

    @Override
    public HgKvEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        SourceEntry sourceEntry = this.queue.poll();
        this.current = sourceEntry.entry;
        this.position = this.current.key();
        this.count++;

        try {
            if (this.count < this.limit) {
                this.addNext(sourceEntry.source);
            } else {
                this.close();
            }
        } catch (RuntimeException | Error e) {
            this.closeAfterFailure(e);
            throw e;
        }
        return this.current;
    }

    @Override
    public byte[] key() {
        return this.current == null ? null : this.current.key();
    }

    @Override
    public byte[] value() {
        return this.current == null ? null : this.current.value();
    }

    @Override
    public byte[] position() {
        return this.position;
    }

    @Override
    public void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        Throwable failure = null;
        for (int i = 0; i < this.iterators.size(); i++) {
            try {
                this.closeSource(i);
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
            for (int i = 0; i < this.iterators.size(); i++) {
                this.addNext(i);
            }
        } catch (RuntimeException | Error e) {
            this.closeAfterFailure(e);
            throw e;
        }
    }

    private void addNext(int source) {
        HgKvIterator<? extends HgKvEntry> iterator =
                this.iterators.get(source);
        if (iterator.hasNext()) {
            this.queue.add(new SourceEntry(source, iterator.next()));
        } else {
            this.closeSource(source);
        }
    }

    private void closeSource(int source) {
        if (this.sourceClosed[source]) {
            return;
        }
        this.sourceClosed[source] = true;
        this.iterators.get(source).close();
    }

    private void closeAfterFailure(Throwable failure) {
        try {
            this.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static final class SourceEntry {

        private final int source;
        private final HgKvEntry entry;

        private SourceEntry(int source, HgKvEntry entry) {
            this.source = source;
            this.entry = entry;
        }
    }
}
