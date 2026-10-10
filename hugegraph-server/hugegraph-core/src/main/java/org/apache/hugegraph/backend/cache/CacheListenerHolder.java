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

package org.apache.hugegraph.backend.cache;

import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.util.Events;

/*
 * Caches and listeners belong to the graph, including between request leases.
 * Transaction close releases a lease; graph close disposes the holder.
 */
final class CacheListenerHolder {

    final EventListener listener;
    final EventHub hub;
    final Runnable cleanup;
    // Must only be read or written inside ConcurrentMap.compute() for the
    // enclosing registry; ConcurrentHashMap.compute() serialises per-key access.
    int refCount;

    CacheListenerHolder(EventListener listener, EventHub hub) {
        this(listener, hub, () -> { });
    }

    CacheListenerHolder(EventListener listener, EventHub hub, Runnable cleanup) {
        this.listener = listener;
        this.hub = hub;
        this.cleanup = cleanup;
        this.refCount = 1;
    }

    void close() {
        try {
            this.hub.unlisten(Events.CACHE, this.listener);
        } finally {
            this.cleanup.run();
        }
    }
}
