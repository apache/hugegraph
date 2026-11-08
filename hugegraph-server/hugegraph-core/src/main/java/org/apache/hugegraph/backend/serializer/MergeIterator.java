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

package org.apache.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.util.E;

public class MergeIterator<T, R> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final BiFunction<T, R, Boolean> merger;
    private final List<Iterator<R>> iterators = new ArrayList<>();
    private final List<R> headElements;

    public MergeIterator(Iterator<T> originIterator,
                         List<Iterator<R>> iterators,
                         BiFunction<T, R, Boolean> merger) {
        E.checkArgumentNotNull(originIterator, "The origin iterator of " +
                                               "MergeIterator can't be null");
        E.checkArgument(iterators != null && !iterators.isEmpty(),
                        "The iterators of MergeIterator can't be " +
                        "null or empty");
        E.checkArgumentNotNull(merger, "The merger function of " +
                                       "MergeIterator can't be null");
        this.originIterator = originIterator;
        this.headElements = new ArrayList<>();
        this.iterators.addAll(iterators);
        this.merger = merger;

        try {
            for (Iterator<R> iterator : this.iterators) {
                if (iterator.hasNext()) {
                    this.headElements.add(iterator.next());
                } else {
                    this.headElements.add(none());
                }
            }
        } catch (Throwable failure) {
            try {
                this.closeInputs();
            } catch (Throwable cleanupFailure) {
                if (failure != cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    @Override
    public void close() throws Exception {
        this.closeInputs();
    }

    private void closeInputs() throws Exception {
        Throwable failure = null;
        for (Iterator<R> iter : this.iterators) {
            if (iter instanceof AutoCloseable autoCloseable) {
                try {
                    autoCloseable.close();
                } catch (Throwable e) {
                    failure = mergeFailure(failure, e);
                }
            }
        }
        try {
            super.close();
        } catch (Throwable e) {
            failure = mergeFailure(failure, e);
        }
        if (failure instanceof Exception exception) {
            throw exception;
        } else if (failure instanceof Error error) {
            throw error;
        } else if (failure != null) {
            throw new AssertionError(failure);
        }
    }

    private static Throwable mergeFailure(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        if (!this.originIterator.hasNext()) {
            return false;
        }

        T next = this.originIterator.next();

        for (int i = 0; i < this.iterators.size(); i++) {
            R element = this.headElements.get(i);
            if (element == none()) {
                continue;
            }

            if (this.merger.apply(next, element)) {
                Iterator<R> iter = this.iterators.get(i);
                if (iter.hasNext()) {
                    this.headElements.set(i, iter.next());
                } else {
                    this.headElements.set(i, none());
                    close(iter);
                }
            }
        }

        assert this.current == none();
        this.current = next;
        return true;
    }
}
