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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.concurrent.KeyLock;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.util.E;

public class RamCache extends AbstractCache<Id, Object> {

    // Implement LRU cache
    private final ConcurrentMap<Id, LinkNode<Id, Object>> map;
    private final LinkedQueueNonBigLock<Id, Object> queue;

    private final KeyLock keyLock;
    private final long halfCapacity;

    public RamCache() {
        this(DEFAULT_SIZE);
    }

    public RamCache(long capacity) {
        super(capacity);

        if (capacity < 0L) {
            capacity = 0L;
        }
        this.keyLock = new KeyLock();
        this.halfCapacity = capacity >> 1;

        long initialCapacity = capacity >= MB ? capacity >> 10 : 256;
        if (initialCapacity > MAX_INIT_CAP) {
            initialCapacity = MAX_INIT_CAP;
        }

        this.map = new ConcurrentHashMap<>((int) initialCapacity);
        this.queue = new LinkedQueueNonBigLock<>();
    }

    @Override
    @Watched(prefix = "ramcache")
    protected final Object access(Id id) {
        assert id != null;

        if (this.map.size() <= this.halfCapacity) {
            LinkNode<Id, Object> node = this.map.get(id);
            if (node == null) {
                return null;
            }
            assert id.equals(node.key());
            return node.value();
        }

        // Avoid to catch lock if cache missed
        if (!this.containsKey(id)) {
            return null;
        }

        final Lock lock = this.keyLock.lock(id);
        try {
            // Maybe the id removed by other threads and returned null value
            LinkNode<Id, Object> node = this.map.get(id);
            if (node == null) {
                return null;
            }

            // NOTE: update the queue only if the size > capacity/2
            if (this.map.size() > this.halfCapacity) {
                // Move the node from mid to tail
                if (this.queue.remove(node) == null) {
                    // The node may be removed by others through dequeue()
                    return null;
                }
                this.queue.enqueue(node);
            }

            assert id.equals(node.key());
            return node.value();
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Watched(prefix = "ramcache")
    protected final boolean write(Id id, Object value, long timeOffset) {
        assert id != null;
        long capacity = this.capacity();
        assert capacity > 0;

        final Lock lock = this.keyLock.lock(id);
        try {
            // The cache is full
            this.removeOldestIfCacheFull(id, capacity);

            // Remove the old node if exists
            LinkNode<Id, Object> node = this.map.get(id);
            if (node != null) {
                this.queue.remove(node);
            }

            // Add the new item to tail of the queue, then map it
            this.map.put(id, this.queue.enqueue(id, value, timeOffset));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Watched(prefix = "ramcache")
    protected final void remove(Id id) {
        if (id == null) {
            return;
        }

        final Lock lock = this.keyLock.lock(id);
        try {
            /*
             * Remove the id from map and queue
             * NOTE: it maybe return null if other threads have removed the id
             */
            LinkNode<Id, Object> node = this.map.remove(id);
            if (node != null) {
                this.queue.remove(node);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected Iterator<CacheNode<Id, Object>> nodes() {
        Iterator<LinkNode<Id, Object>> iter = this.map.values().iterator();
        @SuppressWarnings({"unchecked", "rawtypes"})
        Iterator<CacheNode<Id, Object>> iterSuper = (Iterator) iter;
        return iterSuper;
    }

    @Override
    public boolean containsKey(Id id) {
        return this.map.containsKey(id);
    }

    @Watched(prefix = "ramcache")
    @Override
    public void traverse(Consumer<Object> consumer) {
        E.checkNotNull(consumer, "consumer");
        // NOTE: forEach is 20% faster than for-in with ConcurrentHashMap
        this.map.values().forEach(node -> consumer.accept(node.value()));
    }

    @Watched(prefix = "ramcache")
    @Override
    public void clear() {
        // TODO: synchronized
        if (this.capacity() <= 0 || this.map.isEmpty()) {
            return;
        }
        this.map.clear();
        this.queue.clear();
    }

    @Override
    public long size() {
        return this.map.size();
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    private void removeOldestIfCacheFull(Id id, long capacity) {
        while (this.map.size() >= capacity) {
            /*
             * Remove the oldest from the queue
             * NOTE: it maybe return null if someone else (that's other
             * threads) are doing dequeue() and the queue may be empty.
             */
            LinkNode<Id, Object> removed = this.queue.dequeue();
            if (removed == null) {
                /*
                 * If at this time someone add some new items, these will
                 * be cleared in the map, but still stay in the queue, so
                 * the queue will have some more nodes than the map.
                 */
                this.map.clear();
                break;
            }
            /*
             * Remove the oldest from the map
             * NOTE: it maybe return null if other threads are doing remove
             */
            this.map.remove(removed.key());
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache replaced '{}' with '{}' (capacity={})",
                          removed.key(), id, capacity);
            }
            /*
             * Release the object
             * NOTE: we can't reuse the removed node due to someone else
             * may access the node (will do remove() -> enqueue())
             */
            removed = null;
        }
    }

    private static final class LinkNode<K, V> extends CacheNode<K, V> {

        private LinkNode<K, V> prev;
        private LinkNode<K, V> next;

        public LinkNode(K key, V value) {
            this(key, value, 0L);
        }

        public LinkNode(K key, V value, long timeOffset) {
            super(key, value, timeOffset);
            this.prev = this.next = null;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LinkNode)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            LinkNode<K, V> other = (LinkNode<K, V>) obj;
            return this.key().equals(other.key());
        }

        public int hashCode() {
            return this.key().hashCode();
        }
    }

    private static final class LinkedQueueNonBigLock<K, V> {

        private final KeyLock keyLock;
        private final LinkNode<K, V> empty;
        private final LinkNode<K, V> head;
        private final LinkNode<K, V> rear;
        // private volatile long size;

        @SuppressWarnings("unchecked")
        public LinkedQueueNonBigLock() {
            this.keyLock = new KeyLock();
            this.empty = new LinkNode<>((K) "<empty>", null);
            this.head = new LinkNode<>((K) "<head>", null);
            this.rear = new LinkNode<>((K) "<rear>", null);

            this.reset();
        }

        /**
         * Reset the head node and rear node
         * NOTE:
         * only called by LinkedQueueNonBigLock() without lock
         * or called by clear() with lock(head, rear)
         */
        private void reset() {
            this.head.prev = this.empty;
            this.head.next = this.rear;

            this.rear.prev = this.head;
            this.rear.next = this.empty;

            assert this.head.next == this.rear;
            assert this.rear.prev == this.head;
        }

        /**
         * Dump keys of all nodes in this queue (just for debug)
         */
        private List<K> dumpKeys() {
            List<K> keys = new LinkedList<>();
            LinkNode<K, V> node = this.head.next;
            while (node != this.rear && node != this.empty) {
                assert node != null;
                keys.add(node.key());
                node = node.next;
            }
            return keys;
        }

        /**
         * Check whether a key not in this queue (just for debug)
         */
        @SuppressWarnings("unused")
        private boolean checkNotInQueue(K key) {
            List<K> keys = this.dumpKeys();
            if (keys.contains(key)) {
                throw new RuntimeException(String.format(
                        "Expect %s should be not in %s", key, keys));
            }
            return true;
        }

        /**
         * Check whether there is circular reference (just for debug)
         * NOTE: but it is important to note that this is only key check
         * rather than pointer check.
         */
        @SuppressWarnings("unused")
        private boolean checkPrevNotInNext(LinkNode<K, V> self) {
            LinkNode<K, V> prev = self.prev;
            if (prev.key() == null) {
                assert prev == this.head || prev == this.empty : prev;
                return true;
            }
            List<K> keys = this.dumpKeys();
            int prevPos = keys.indexOf(prev.key());
            int selfPos = keys.indexOf(self.key());
            if (prevPos > selfPos && selfPos != -1) {
                throw new RuntimeException(String.format(
                        "Expect %s should be before %s, actual %s",
                        prev.key(), self.key(), keys));
            }
            return true;
        }

        private List<Lock> lock(Object... nodes) {
            return this.keyLock.lockAll(nodes);
        }

        private List<Lock> lock(Object node1, Object node2) {
            return this.keyLock.lockAll(node1, node2);
        }

        private void unlock(List<Lock> locks) {
            this.keyLock.unlockAll(locks);
        }

        /**
         * Clear the queue
         */
        public void clear() {
            assert this.rear.prev != null : this.head.next;

            while (true) {
                /*
                 * If someone is removing the last node by remove(),
                 * it will update the rear.prev, so we should lock it.
                 */
                LinkNode<K, V> last = this.rear.prev;

                List<Lock> locks = this.lock(this.head, last, this.rear);
                try {
                    if (last != this.rear.prev) {
                        // The rear.prev has changed, try to get lock again
                        continue;
                    }
                    this.reset();
                } finally {
                    this.unlock(locks);
                }
                return;
            }
        }

        /**
         * Add an item with key-value to the queue
         */
        public LinkNode<K, V> enqueue(K key, V value, long timeOffset) {
            return this.enqueue(new LinkNode<>(key, value, timeOffset));
        }

        /**
         * Add a node to tail of the queue
         */
        public LinkNode<K, V> enqueue(LinkNode<K, V> node) {
            assert node != null;
            assert node.prev == null || node.prev == this.empty;
            assert node.next == null || node.next == this.empty;

            while (true) {
                LinkNode<K, V> last = this.rear.prev;
                assert last != this.empty : last;

                // TODO: should we lock the new `node`?
                List<Lock> locks = this.lock(last, this.rear);
                try {
                    if (last != this.rear.prev) {
                        // The rear.prev has changed, try to get lock again
                        continue;
                    }

                    /*
                     * Link the node to the `rear` before to the `last` if we
                     * have not locked the `node` itself, because dumpKeys()
                     * may get the new node with next=null.
                     * TODO: it also depends on memory barrier.
                     */

                    // Build the link between `node` and the `rear`
                    node.next = this.rear;
                    assert this.rear.prev == last : this.rear.prev;
                    this.rear.prev = node;

                    // Build the link between `last` and `node`
                    node.prev = last;
                    last.next = node;

                    return node;
                } finally {
                    this.unlock(locks);
                }
            }
        }

        /**
         * Remove a node from head of the queue
         */
        public LinkNode<K, V> dequeue() {
            while (true) {
                LinkNode<K, V> first = this.head.next;
                if (first == this.rear) {
                    // Empty queue
                    return null;
                }

                List<Lock> locks = this.lock(this.head, first);
                try {
                    if (first != this.head.next) {
                        // The head.next has changed, try to get lock again
                        continue;
                    }

                    // Break the link between the `head` and `first`
                    assert first.next != null && first.next != this.empty;
                    this.head.next = first.next;
                    first.next.prev = this.head;

                    // Clear the links of the `first` node
                    first.prev = this.empty;
                    first.next = this.empty;

                    return first;
                } finally {
                    this.unlock(locks);
                }
            }
        }

        /**
         * Remove a specified node from the queue
         */
        public LinkNode<K, V> remove(LinkNode<K, V> node) {
            assert node != this.empty;
            assert node != this.head && node != this.rear;

            while (true) {
                LinkNode<K, V> prev = node.prev;
                if (prev == this.empty || node.next == this.empty) {
                    // Ignore the `node` if it has been removed
                    return null;
                }

                List<Lock> locks = this.lock(prev, node);
                try {
                    if (prev != node.prev) {
                        /*
                         * The previous node has changed (maybe it's lock
                         * released after it's removed, then we got the
                         * lock), so try again until it's not changed.
                         */
                        continue;
                    }
                    assert node.next != null : node;
                    assert node.next != this.empty : node.next;
                    assert node.next != node.prev : node.next;

                    // Break `node` & Build the link between node.prev~node.next
                    node.prev.next = node.next;
                    node.next.prev = node.prev;

                    assert prev == node.prev : prev.key() + "!=" + node.prev;

                    // Clear the links of `node`
                    node.prev = this.empty;
                    node.next = this.empty;

                    return node;
                } finally {
                    this.unlock(locks);
                }
            }
        }
    }
}
