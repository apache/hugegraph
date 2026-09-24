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

package org.apache.hugegraph.store.client.query;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.client.type.HgStoreClientException;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The iterator encapsulating batched stream responses supports type conversion via a T -> E mapper
 * Server results are enqueued via invoker calls, which the iterator then consumes
 * iterator <-------read --------- queue
 * |                             ↑
 * invoke                   Observer.onNext
 * |                             |
 * ↓                             |
 * req ---- send req ----------server
 */
@Slf4j
public class StreamKvIterator<E> implements HgKvIterator<E> {

    /**
     * Operation that closing iterator, clear server cache
     */
    private final Consumer<Boolean> closeOp;

    @Setter
    @Getter
    private String address;

    /**
     * iterator returned from observer
     */
    private Iterator<E> iterator = null;

    private final Supplier<Iterator<E>> iteratorSupplier;

    /**
     * With close function, needed to pass
     */
    public StreamKvIterator(Consumer<Boolean> closeOp, Supplier<Iterator<E>> supplier) {
        this.closeOp = closeOp;
        this.iteratorSupplier = supplier;
    }

    @Override
    public byte[] key() {
        throw new HgStoreClientException("position function not supported");
    }

    @Override
    public byte[] value() {
        throw new HgStoreClientException("position function not supported");
    }

    @Override
    public void close() {
        this.closeOp.accept(true);
    }

    @Override
    public byte[] position() {
        throw new HgStoreClientException("position function not supported");
    }

    @Override
    public void seek(byte[] position) {
        throw new HgStoreClientException("seek function not supported");
    }

    @Override
    public boolean hasNext() {
        if (this.iterator == null || !this.iterator.hasNext()) {
            this.iterator = this.iteratorSupplier.get();
        }
        return this.iterator != null && this.iterator.hasNext();
    }

    @Override
    public E next() {
        return this.iterator.next();
    }
}
