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
import java.util.function.Supplier;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

/**
 * This is a wrapper of the ScanIterator that provides a mechanism
 * to set a threshold value in order to abort the iterating operation.
 */
final class FusingScanIterator implements ScanIterator {

    public static final byte[] EMPTY_BYTES = new byte[0];
    private long max;
    private long accumulator;
    private Supplier<ScanIterator> supplier;
    private ScanIterator iterator;
    private Throwable cleanupFailure;
    private boolean closed;
    private byte[] position = EMPTY_BYTES;

    private FusingScanIterator() {
    }

    public static FusingScanIterator maxOf(long maxThreshold,
                                           Supplier<ScanIterator> iteratorSupplier) {
        FusingScanIterator res = new FusingScanIterator();
        res.max = maxThreshold;
        res.supplier = iteratorSupplier;
        return res;
    }

    private ScanIterator getIterator() {
        while ((this.iterator = this.supplier.get()) != null) {
            if (this.iterator.hasNext()) {
                return this.iterator;
            }
            closeCurrent();
        }
        return null;
    }

    private void init() {
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        }
    }

    @Override
    public boolean hasNext() {
        rethrowCleanupFailure();
        if (this.closed || this.isThresholdExceeded()) {
            return false;
        }
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        }
        return this.iterator != null;
    }

    @Override
    public boolean isValid() {
        return hasNext();
    }

    @Override
    public byte[] position() {
        return this.position;
    }

    /**
     * @return true, when the threshold is exceeded.
     */
    private boolean isThresholdExceeded() {
        return this.accumulator >= this.max;
    }

    @Override
    public <T> T next() {
        if (this.isThresholdExceeded()) {
            throw new NoSuchElementException();
        }
        this.init();
        if (this.iterator == null) {
            throw new NoSuchElementException();
        }
        T t = this.iterator.next();
        position = this.iterator.position();
        this.accumulator++;
        if (!this.iterator.hasNext() || this.isThresholdExceeded()) {
            closeCurrent();
        }
        return t;
    }

    @Override
    public void close() {
        this.closed = true;
        closeCurrent();
    }

    private void closeCurrent() {
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
}
