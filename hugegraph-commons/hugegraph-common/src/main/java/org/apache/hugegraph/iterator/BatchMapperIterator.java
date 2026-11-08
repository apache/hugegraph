/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.iterator;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;

import com.google.common.collect.ImmutableList;

public class BatchMapperIterator<T, R> extends WrappedIterator<R> {

    private final int batch;
    private final Iterator<T> originIterator;
    private final Function<List<T>, Iterator<R>> mapperCallback;

    private Iterator<R> batchIterator;

    public BatchMapperIterator(int batch, Iterator<T> origin,
                               Function<List<T>, Iterator<R>> mapper) {
        E.checkArgument(batch > 0, "Expect batch > 0, but got %s", batch);
        this.batch = batch;
        this.originIterator = origin;
        this.mapperCallback = mapper;
        this.batchIterator = null;
    }

    @Override
    public void close() throws Exception {
        try (AutoCloseable origin = super::close) {
            this.resetBatchIterator();
        }
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        if (this.batchIterator != null && this.fetchFromBatch()) {
            return true;
        }

        List<T> batch = this.nextBatch();
        assert this.batchIterator == null;
        while (!batch.isEmpty()) {
            // Do fetch
            this.batchIterator = this.mapperCallback.apply(batch);
            if (this.batchIterator != null && this.fetchFromBatch()) {
                return true;
            }
            // Try next batch
            batch = this.nextBatch();
        }
        return false;
    }

    protected final List<T> nextBatch() {
        if (!this.originIterator.hasNext()) {
            return ImmutableList.of();
        }
        List<T> list = InsertionOrderUtil.newList();
        for (int i = 0; i < this.batch && this.originIterator.hasNext(); i++) {
            T next = this.originIterator.next();
            list.add(next);
        }
        return list;
    }

    protected final boolean fetchFromBatch() {
        E.checkNotNull(this.batchIterator, "mapper results");
        while (this.batchIterator.hasNext()) {
            R result = this.batchIterator.next();
            if (result != null) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        this.resetBatchIterator();
        return false;
    }

    protected final void resetBatchIterator() {
        if (this.batchIterator == null) {
            return;
        }
        close(this.batchIterator);
        this.batchIterator = null;
    }
}
