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

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.node.util.HgAssert;
import org.apache.hugegraph.store.node.util.HgStoreConst;

/**
 * 2022/2/28
 *
 * @version 0.3.0 added limit support
 */
@NotThreadSafe
public final class BatchScanIterator implements ScanIterator {

    private final Supplier<KVPair<QueryCondition, ScanIterator>> batchSupplier;
    private final Supplier<Long> limitSupplier;
    private final AtomicBoolean closed = new AtomicBoolean();
    private ScanIterator iterator;
    private boolean hasNext = false;
    private Throwable cleanupFailure;
    private long curCount;
    private long curLimit;

    private BatchScanIterator(Supplier<KVPair<QueryCondition, ScanIterator>> iteratorSupplier,
                              Supplier<Long> limitSupplier) {
        this.batchSupplier = iteratorSupplier;
        this.limitSupplier = limitSupplier;
    }

    public static BatchScanIterator of(
            Supplier<KVPair<QueryCondition, ScanIterator>> iteratorSupplier,
            Supplier<Long> limitSupplier) {
        HgAssert.isArgumentNotNull(iteratorSupplier, "iteratorSupplier");
        HgAssert.isArgumentNotNull(limitSupplier, "limitSupplier");
        return new BatchScanIterator(iteratorSupplier, limitSupplier);
    }

    private ScanIterator getIterator() {
        ScanIterator buf;
        int count = 0;
        this.curCount = 0L;

        do {
            buf = this.batchSupplier.get().getValue();
            this.iterator = buf;

            if (buf == null) {
                break;
            }

            if (!buf.hasNext()) {
                closeIterator();
                buf = null;
            }

            if (++count == Integer.MAX_VALUE) {
                throw new RuntimeException("Do loop times more than Integer.MAX_VALUE");
            }

        } while (buf == null);

        if (buf != null) {
            Long limit = this.limitSupplier.get();

            if (limit == null || limit <= 0) {
                this.curLimit = Integer.MAX_VALUE;
            } else {
                this.curLimit = limit;
            }

        }

        return buf;

    }

    @Override
    public boolean hasNext() {

        rethrowCleanupFailure();
        if (this.closed.get()) {
            return false;
        }
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        } else if (!this.iterator.hasNext()) {
            closeIterator();
            this.iterator = this.getIterator();
        } else if (this.curCount == this.curLimit) {
            closeIterator();
            this.iterator = this.getIterator();
        }

        if (this.iterator == null) {
            return false;
        } else {
            this.hasNext = true;
            return true;
        }
    }

    @Override
    public <T> T next() {
        if (this.hasNext) {
            this.hasNext = false;
        } else {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
        }

        this.curCount++;

        return this.iterator.next();
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            closeIterator();
        }
        rethrowCleanupFailure();
    }

    private void closeIterator() {
        ScanIterator current = this.iterator;
        this.iterator = null;
        if (current != null) {
            try {
                current.close();
            } catch (RuntimeException | Error failure) {
                this.cleanupFailure = failure;
            }
        }
        rethrowCleanupFailure();
    }

    private void rethrowCleanupFailure() {
        if (this.cleanupFailure instanceof RuntimeException) {
            throw (RuntimeException) this.cleanupFailure;
        }
        if (this.cleanupFailure != null) {
            throw (Error) this.cleanupFailure;
        }
    }

    @Override
    public boolean isValid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] position() {
        if (this.iterator != null) {
            return this.iterator.position();
        }
        return HgStoreConst.EMPTY_BYTES;
    }
}
