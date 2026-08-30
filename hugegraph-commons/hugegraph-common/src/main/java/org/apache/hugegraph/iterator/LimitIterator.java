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
import java.util.function.Function;

public class LimitIterator<T> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final Function<T, Boolean> filterCallback;
    private boolean originClosed;

    public LimitIterator(Iterator<T> origin, Function<T, Boolean> filter) {
        this.originIterator = origin;
        this.filterCallback = filter;
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    public void close() throws Exception {
        if (this.originClosed || this.originIterator == null) {
            return;
        }
        this.originClosed = true;
        super.close();
    }

    @Override
    protected final boolean fetch() {
        while (this.originIterator.hasNext()) {
            T next = this.originIterator.next();
            if (next == null) {
                continue;
            }
            // Do filter
            boolean reachLimit = this.filterCallback.apply(next);
            if (reachLimit) {
                this.closeOriginIterator();
                return false;
            }
            assert this.current == none();
            this.current = next;
            return true;
        }
        return false;
    }

    protected final void closeOriginIterator() {
        if (this.originClosed || this.originIterator == null) {
            return;
        }
        this.originClosed = true;
        close(this.originIterator);
    }
}
