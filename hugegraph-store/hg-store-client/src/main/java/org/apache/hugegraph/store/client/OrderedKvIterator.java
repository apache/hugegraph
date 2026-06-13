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
    private final long limit;

    private boolean initialized;
    private long count;
    private HgKvEntry current;
    private byte[] position;
    private byte[] seekPosition;

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
        this.limit = limit <= HgStoreClientConst.NO_LIMIT ? Long.MAX_VALUE :
                     limit;
        this.initialized = false;
        this.count = 0L;
        this.current = null;
        this.position = HgStoreClientConst.EMPTY_BYTES;
        this.seekPosition = HgStoreClientConst.EMPTY_BYTES;
    }

    @Override
    public boolean hasNext() {
        this.initialize();
        return this.count < this.limit && !this.queue.isEmpty();
    }

    @Override
    public HgKvEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        SourceEntry entry = this.queue.poll();
        this.current = entry.entry;
        this.position = entry.entry.key();
        this.count++;

        HgKvIterator<? extends HgKvEntry> iterator =
                this.iterators.get(entry.source);
        this.addNext(entry.source, iterator);
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
    public void seek(byte[] position) {
        if (this.initialized) {
            throw new IllegalStateException("Can't seek after reading");
        }
        this.seekPosition = position == null ? HgStoreClientConst.EMPTY_BYTES :
                            position;
    }

    @Override
    public void close() {
        for (HgKvIterator<? extends HgKvEntry> iterator : this.iterators) {
            iterator.close();
        }
        this.queue.clear();
    }

    private void initialize() {
        if (this.initialized) {
            return;
        }
        for (int i = 0; i < this.iterators.size(); i++) {
            HgKvIterator<? extends HgKvEntry> iterator =
                    this.iterators.get(i);
            this.addNext(i, iterator);
        }
        this.initialized = true;
    }

    private void addNext(int source,
                         HgKvIterator<? extends HgKvEntry> iterator) {
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            if (this.seekPosition.length == 0 ||
                Arrays.compareUnsigned(entry.key(), this.seekPosition) > 0) {
                this.queue.add(new SourceEntry(source, entry));
                break;
            }
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
