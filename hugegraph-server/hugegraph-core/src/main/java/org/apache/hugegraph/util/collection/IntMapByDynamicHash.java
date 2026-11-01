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

package org.apache.hugegraph.util.collection;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import sun.misc.Unsafe;

/**
 * This class implements a concurrent hash map specifically designed for integer keys and values.
 * It uses low-level programming techniques such as direct memory access via `sun.misc.Unsafe` to
 * achieve high performance.
 * The class is part of the Apache HugeGraph project.
 */
public class IntMapByDynamicHash implements IntMap {

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final float LOAD_FACTOR = 0.75f;

    private static final int PARTITIONED_SIZE_THRESHOLD = 4096;

    private static final int NULL_VALUE = Integer.MIN_VALUE;

    private static final AtomicReferenceFieldUpdater<IntMapByDynamicHash, Entry[]>
            TABLE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(IntMapByDynamicHash.class, Entry[].class,
                                                   "table");

    private volatile Entry[] table;

    /**
     * Partition counting to improve the concurrency performance of addToSize()
     */
    private int[] partitionedSize;

    /**
     * updated via atomic field updater
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int size;

    private static final Entry RESIZING = new Entry(NULL_VALUE, NULL_VALUE, (byte) 1);
    private static final Entry RESIZED = new Entry(NULL_VALUE, NULL_VALUE, (byte) 2);

    private static final Entry RESIZE_SENTINEL = new Entry(NULL_VALUE, NULL_VALUE, (byte) 3);

    /**
     * must be (2^n) - 1
     */
    private static final int SIZE_BUCKETS = 7;

    /**
     * Constructor for the IntMapByDynamicHash class.
     *
     * @param initialCapacity the initial capacity of the map.
     */
    public IntMapByDynamicHash(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Initial Capacity: " + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        long size = (long) (1.0 + (long) initialCapacity / LOAD_FACTOR);
        int cap = (size >= (long) MAXIMUM_CAPACITY) ?
                  MAXIMUM_CAPACITY : tableSizeFor((int) size);
        if (cap >= PARTITIONED_SIZE_THRESHOLD) {
            // we want 7 extra slots, and 64 bytes for each slot int are 4 bytes,
            // so 64 bytes are 16 ints.
            this.partitionedSize =
                    new int[SIZE_BUCKETS * 16];
        }
        // The end index is for resizeContainer
        this.table = new Entry[cap + 1];
    }

    /**
     * Default constructor for the IntMapByDynamicHash class.
     * Initializes the map with the default initial capacity.
     */
    public IntMapByDynamicHash() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    private static void setTableAt(Object[] array, int index, Object newValue) {
        UNSAFE.putObjectVolatile(array, ((long) index << ENTRY_ARRAY_SHIFT) + ENTRY_ARRAY_BASE,
                                 newValue);
    }

    private static int tableSizeFor(int c) {
        int n = c - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /* ---------------- Table element access -------------- */

    private static long entryOffset(int index) {
        return ((long) index << ENTRY_ARRAY_SHIFT) + ENTRY_ARRAY_BASE;
    }

    private static Object tableAt(Object[] array, int index) {
        return UNSAFE.getObjectVolatile(array, entryOffset(index));
    }

    private static boolean casTableAt(Object[] array, int index, Object expected, Object newValue) {
        return UNSAFE.compareAndSwapObject(array, entryOffset(index), expected, newValue);
    }

    /**
     * Puts a key-value pair into the map. If the key already exists in the map, its value is
     * updated.
     *
     * @param key   the key to be put into the map.
     * @param value the value to be associated with the key.
     * @return true if the operation is successful.
     */
    @Override
    public boolean put(int key, int value) {
        Entry[] currentArray = this.table;
        int hash = this.hash(key, currentArray.length);
        Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, hash);
        if (o == null) {
            Entry newEntry = new Entry(key, value);
            this.addToSize(1);
            if (IntMapByDynamicHash.casTableAt(currentArray, hash, null, newEntry)) {
                return true;
            }
            this.addToSize(-1);
        }

        this.slowPut(key, value, currentArray);
        return true;
    }

    /**
     * This method is used when the normal put operation fails due to a hash collision.
     * It searches for the key in the chain and if found, replaces the entry.
     * If the key is not found, it adds a new entry.
     *
     * @param key          the key to be put into the map.
     * @param value        the value to be associated with the key.
     * @param currentTable the current table where the key-value pair is to be put.
     * @return the old value if the key is already present in the map, otherwise NULL_VALUE.
     */
    private int slowPut(int key, int value, Entry[] currentTable) {
        int length;
        int index;
        Entry o;

        while (true) {
            length = currentTable.length;
            index = this.hash(key, length);
            o = (Entry) IntMapByDynamicHash.tableAt(currentTable, index);

            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                Entry e = o;
                boolean found = false;

                // Search for the key in the chain
                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        found = true;
                        break;
                    }
                    e = e.getNext();
                }

                if (found) {
                    int oldVal = e.getValue();
                    // Key found, replace the entry
                    Entry newEntry =
                            new Entry(key, value, this.createReplacementChainForRemoval(o, e));
                    if (IntMapByDynamicHash.casTableAt(currentTable, index, o, newEntry)) {
                        return oldVal;
                    }
                } else {
                    // Key not found, add a new entry
                    Entry newEntry = new Entry(key, value, o);
                    if (IntMapByDynamicHash.casTableAt(currentTable, index, o, newEntry)) {
                        this.incrementSizeAndPossiblyResize(currentTable, length, o);
                        return NULL_VALUE;
                    }
                }
            }
        }
    }

    /**
     * Retrieves the value associated with the given key from the map.
     *
     * @param key the key whose associated value is to be returned.
     * @return the value associated with the given key, or NULL_VALUE if the key does not exist
     * in the map.
     */
    @Override
    public int get(int key) {
        Entry[] currentArray = this.table;
        int hash = this.hash(key, currentArray.length);
        Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, hash);
        if (o == RESIZED || o == RESIZING) {
            return this.slowGet(key, currentArray);
        }
        for (Entry e = o; e != null; e = e.getNext()) {
            int k;
            // TODO: check why key == k is always false
            if ((k = e.getKey()) == key || key == k) {
                return e.value;
            }
        }
        return NULL_VALUE;
    }

    /**
     * This method is used when the normal get operation fails due to a hash collision.
     * It searches for the key in the chain and returns the associated value if found.
     *
     * @param key          the key whose associated value is to be returned.
     * @param currentArray the current table where the key-value pair is located.
     * @return the value associated with the given key, or NULL_VALUE if the key does not exist
     * in the map.
     */
    private int slowGet(int key, Entry[] currentArray) {
        while (true) {
            int length = currentArray.length;
            int hash = this.hash(key, length);
            Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, hash);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, hash);
            } else {
                Entry e = o;
                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                return NULL_VALUE;
            }
        }
    }

    /**
     * Removes the key-value pair with the given key from the map.
     *
     * @param key the key whose associated key-value pair is to be removed.
     * @return true if the key-value pair was found and removed, false otherwise.
     */
    @Override
    public boolean remove(int key) {
        Entry[] currentTable = this.table;
        int hash = this.hash(key, currentTable.length);
        Entry o = (Entry) IntMapByDynamicHash.tableAt(currentTable, hash);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(key, currentTable) != null;
        }

        Entry e = o;
        while (e != null) {
            int candidate = e.getKey();
            if (candidate == key) {
                Entry replacement = this.createReplacementChainForRemoval(o, e);
                if (IntMapByDynamicHash.casTableAt(currentTable, hash, o, replacement)) {
                    this.addToSize(-1);
                    return true;
                }
                return this.slowRemove(key, currentTable) != null;
            }
            e = e.getNext();
        }
        return false;
    }

    /**
     * This method is used when the normal remove operation fails due to a hash collision.
     * It searches for the key in the chain and if found, removes the entry.
     *
     * @param key          the key whose associated key-value pair is to be removed.
     * @param currentTable the current table where the key-value pair is located.
     * @return the removed entry if the key is found, otherwise null.
     */
    private Entry slowRemove(int key, Entry[] currentTable) {
        int length;
        int index;
        Entry o;

        while (true) {
            length = currentTable.length;
            index = this.hash(key, length);
            o = (Entry) IntMapByDynamicHash.tableAt(currentTable, index);
            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                Entry e = o;

                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        Entry replacement = this.createReplacementChainForRemoval(o, e);
                        if (IntMapByDynamicHash.casTableAt(currentTable, index, o, replacement)) {
                            this.addToSize(-1);
                            return e;
                        }
                        // Key found, but CAS failed, restart the loop
                        break;
                    }
                    e = e.getNext();
                }

                if (e == null) {
                    return null;
                }
            }
        }
    }

    /**
     * Checks if the map contains a key-value pair with the given key.
     *
     * @param key the key to be checked.
     * @return true if the map contains a key-value pair with the given key, false otherwise.
     */
    @Override
    public boolean containsKey(int key) {
        return this.getEntry(key) != null;
    }

    @Override
    public IntIterator keys() {
        return new KeyIterator();
    }

    @Override
    public IntIterator values() {
        return new ValueIterator();
    }

    /**
     * Removes all the mappings from this map. The map will be empty after this call returns.
     */
    @Override
    public void clear() {
        Entry[] currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length - 1; i++) {
                Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer =
                            (ResizeContainer) IntMapByDynamicHash.tableAt(currentArray,
                                                                          currentArray.length - 1);
                } else if (o != null) {
                    Entry e = o;
                    if (IntMapByDynamicHash.casTableAt(currentArray, i, o, null)) {
                        int removedEntries = 0;
                        while (e != null) {
                            removedEntries++;
                            e = e.getNext();
                        }
                        this.addToSize(-removedEntries);
                    }
                }
            }
            if (resizeContainer != null) {
                if (resizeContainer.isNotDone()) {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        } while (resizeContainer != null);
    }

    @Override
    public int size() {
        int localSize = this.size;
        if (this.partitionedSize != null) {
            for (int i = 0; i < SIZE_BUCKETS; i++) {
                localSize += this.partitionedSize[i << 4];
            }
        }
        return localSize;
    }

    @Override
    public boolean concurrent() {
        return true;
    }

    private int hash(int key, int length) {
        return key & (length - 2);
    }

    private Entry getEntry(int key) {
        Entry[] currentArray = this.table;
        while (true) {
            int length = currentArray.length;
            int index = this.hash(key, length);
            Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry e = o;
                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        return e;
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    private void addToSize(int value) {
        if (this.partitionedSize != null) {
            if (this.incrementPartitionedSize(value)) {
                return;
            }
        }
        this.incrementLocalSize(value);
    }

    private boolean incrementPartitionedSize(int value) {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & SIZE_BUCKETS;
        if (h != 0) {
            h = (h - 1) << 4;
            long address = ((long) h << INT_ARRAY_SHIFT) + INT_ARRAY_BASE;
            while (true) {
                int localSize = UNSAFE.getIntVolatile(this.partitionedSize, address);
                if (UNSAFE.compareAndSwapInt(this.partitionedSize, address, localSize,
                                             localSize + value)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void incrementLocalSize(int value) {
        while (true) {
            int localSize = this.size;
            if (UNSAFE.compareAndSwapInt(this, SIZE_OFFSET, localSize, localSize + value)) {
                break;
            }
        }
    }

    private Entry createReplacementChainForRemoval(Entry original, Entry toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Entry replacement = null;
        Entry e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Entry(e.getKey(), e.getValue(), replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    private void incrementSizeAndPossiblyResize(Entry[] currentArray, int length, Entry prev) {
        this.addToSize(1);
        if (prev != null) {
            int localSize = this.size();
            int threshold = (int) (length * LOAD_FACTOR); // threshold = length * 0.75
            if (localSize + 1 > threshold) {
                this.resize(currentArray);
            }
        }
    }

    private Entry[] helpWithResizeWhileCurrentIndex(Entry[] currentArray, int index) {
        Entry[] newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (IntMapByDynamicHash.tableAt(currentArray, index) != RESIZED) {
            helpCount++;
            newArray = this.helpWithResize(currentArray);
            if ((helpCount & 7) == 0) {
                Thread.yield();
            }
        }
        return newArray;
    }

    private void resize(Entry[] oldTable) {
        this.resize(oldTable, (oldTable.length - 1 << 1) + 1);
    }

    /**
     * Resizes the map to a new capacity. This method is called when the map's size exceeds its
     * threshold. It creates a new array with the new capacity and transfers all entries from the
     * old array to the new one.
     * Note: newSize must be a power of 2 + 1
     *
     * @param oldTable The old table to resize.
     * @param newSize  The new size for the table.
     */
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void resize(Entry[] oldTable, int newSize) {
        int oldCapacity = oldTable.length;
        int end = oldCapacity - 1;
        Entry last = (Entry) IntMapByDynamicHash.tableAt(oldTable, end);
        if (this.size() < end && last == RESIZE_SENTINEL) {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY) {
            throw new RuntimeException("max capacity of map exceeded");
        }
        ResizeContainer resizeContainer = null;
        // This ownResize records whether current thread need to perform the expansion operation of
        // the map by itself
        boolean ownResize = false;
        if (last == null || last == RESIZE_SENTINEL) {
            // allocating a new array is too expensive to make this an atomic operation
            synchronized (oldTable) {
                if (IntMapByDynamicHash.tableAt(oldTable, end) == null) {
                    IntMapByDynamicHash.setTableAt(oldTable, end, RESIZE_SENTINEL);
                    if (this.partitionedSize == null && newSize >= PARTITIONED_SIZE_THRESHOLD) {
                        this.partitionedSize = new int[SIZE_BUCKETS * 16];
                    }
                    resizeContainer = new ResizeContainer(new Entry[newSize], oldTable.length - 1);
                    IntMapByDynamicHash.setTableAt(oldTable, end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize) {
            this.transfer(oldTable, resizeContainer);

            Entry[] src = this.table;
            while (!TABLE_UPDATER.compareAndSet(this, oldTable, resizeContainer.nextArray)) {
                /*
                we're in a double resize situation; we'll have to go help until it's our turn
                to set the table
                 */
                if (src != oldTable) {
                    this.helpWithResize(src);
                }
            }
        } else {
            this.helpWithResize(oldTable);
        }
    }

    /**
     * Transfers all entries from the source table to the destination table. This method is
     * called during the resize operation. It iterates over the source table and for each non-null
     * entry, it copies the entry to the destination table. If the entry in the source table is
     * marked as RESIZED or RESIZING, it helps with the resize operation.
     * After all entries are transferred, it notifies the ResizeContainer that the resize operation
     * is done.
     *
     * @param src             The source table from which entries are to be transferred.
     * @param resizeContainer The container that holds the state of the resize operation.
     */
    private void transfer(Entry[] src, ResizeContainer resizeContainer) {
        Entry[] dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length - 1; ) {
            Entry o = (Entry) IntMapByDynamicHash.tableAt(src, j);
            if (o == null) {
                if (IntMapByDynamicHash.casTableAt(src, j, null, RESIZED)) {
                    j++;
                }
            } else if (o == RESIZED || o == RESIZING) {
                /*
                 During the expansion process, other threads have already migrated the elements at
                 this location to the new array. This means that the elements in the current
                 position have already been processed and do not need to be migrated again.
                 */
                j = (j & ~(ResizeContainer.QUEUE_INCREMENT - 1)) + ResizeContainer.QUEUE_INCREMENT;
                /*
                 When there is only one thread for expansion, there is no concurrency issue
                 and there is no need to wait.
                 */
                if (resizeContainer.resizers.get() == 1) {
                    break;
                }
            } else {
                Entry e = o;
                if (IntMapByDynamicHash.casTableAt(src, j, o, RESIZING)) {
                    while (e != null) {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    IntMapByDynamicHash.setTableAt(src, j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    /**
     * Enable the current thread to participate in the expansion
     */
    private Entry[] helpWithResize(Entry[] currentArray) {
        ResizeContainer resizeContainer =
                (ResizeContainer) IntMapByDynamicHash.tableAt(currentArray,
                                                              currentArray.length - 1);
        Entry[] newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT) {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    /**
     * Transfers entries from the old table to the new table in reverse order. This method is used
     * to help the resize operation by spreading the work among multiple threads. Each thread
     * transfers a portion of the entries from the end of the old table to the beginning of the new
     * table.
     *
     * @param src             The old table to transfer entries from.
     * @param resizeContainer The container that holds the state of the resize operation.
     */
    private void reverseTransfer(Entry[] src, ResizeContainer resizeContainer) {
        Entry[] dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0) {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0) {
                if (start < 0) {
                    start = 0;
                }
                for (int j = end - 1; j >= start; ) {
                    Entry o = (Entry) IntMapByDynamicHash.tableAt(src, j);
                    if (o == null) {
                        if (IntMapByDynamicHash.casTableAt(src, j, null, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        Entry e = o;
                        if (IntMapByDynamicHash.casTableAt(src, j, o, RESIZING)) {
                            while (e != null) {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
                            IntMapByDynamicHash.setTableAt(src, j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    /**
     * Copies an entry from the old table to the new table. This method is called during the resize
     * operation. It does not check if the entry already exists in the new table, so it should only
     * be called with entries that are not in the new table yet.
     *
     * @param dest        The new table to copy the entry to.
     * @param toCopyEntry The entry to copy.
     */
    private void unconditionalCopy(Entry[] dest, Entry toCopyEntry) {
        Entry[] currentArray = dest;
        while (true) {
            int length = currentArray.length;
            int index = this.hash(toCopyEntry.getKey(), length);
            Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray =
                        ((ResizeContainer) IntMapByDynamicHash.tableAt(currentArray,
                                                                       length - 1)).nextArray;
            } else {
                Entry newEntry;
                if (o == null) {
                    if (toCopyEntry.getNext() == null) {
                        newEntry = toCopyEntry; // no need to duplicate
                    } else {
                        newEntry = new Entry(toCopyEntry.getKey(), toCopyEntry.getValue());
                    }
                } else {
                    newEntry = new Entry(toCopyEntry.getKey(), toCopyEntry.getValue(), o);
                }
                if (IntMapByDynamicHash.casTableAt(currentArray, index, o, newEntry)) {
                    return;
                }
            }
        }
    }

    /**
     * The ResizeContainer class is used to hold the state of the resize operation.
     * It contains the new array to which entries are transferred, the number of threads
     * participating in the resize operation, and the position in the old array from which
     * entries are transferred.
     */
    private static final class ResizeContainer extends Entry {

        private static final int QUEUE_INCREMENT =
                Math.min(1 << 10,
                         Integer.highestOneBit(IntSet.CPUS) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final Entry[] nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(Entry[] nextArray, int oldSize) {
            super(NULL_VALUE, NULL_VALUE, (byte) 4);
            this.nextArray = nextArray;
            this.queuePosition = new AtomicInteger(oldSize);
        }

        public void incrementResizer() {
            this.resizers.incrementAndGet();
        }

        public void decrementResizerAndNotify() {
            int remaining = this.resizers.decrementAndGet();
            if (remaining == 0) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }

        public int getQueuePosition() {
            return this.queuePosition.get();
        }

        public int subtractAndGetQueuePosition() {
            return this.queuePosition.addAndGet(-QUEUE_INCREMENT);
        }

        public void waitForAllResizers() {
            if (this.resizers.get() > 0) {
                for (int i = 0; i < 16; i++) {
                    if (this.resizers.get() == 0) {
                        break;
                    }
                }
                for (int i = 0; i < 16; i++) {
                    if (this.resizers.get() == 0) {
                        break;
                    }
                    Thread.yield();
                }
            }
            if (this.resizers.get() > 0) {
                synchronized (this) {
                    while (this.resizers.get() > 0) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            }
        }

        public boolean isNotDone() {
            return this.resizers.get() > 0;
        }

        public void zeroOutQueuePosition() {
            this.queuePosition.set(0);
        }
    }

    private static class Entry {

        final int key;
        volatile int value;
        volatile Entry next;

        /**
         * 0 NORMAL
         * 1 RESIZING
         * 2 RESIZED
         * 3 RESIZE_SENTINEL
         * 4 RESIZE_CONTAINER
         */
        final byte state;

        public Entry(int key, int value, byte state) {
            this.key = key;
            this.value = value;
            this.state = state;
        }

        public Entry(int key, int value) {
            this.key = key;
            this.value = value;
            this.next = null;
            this.state = 0;
        }

        public Entry(int key, int value, Entry next) {
            this.key = key;
            this.value = value;
            this.next = next;
            this.state = 0;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public Entry getNext() {
            return next;
        }

        @Override
        public String toString() {
            return this.key + "=" + this.value;
        }
    }

    /* ---------------- Iterator -------------- */

    private static final class IteratorState {

        private Entry[] currentTable;
        private int start;
        private int end;

        private IteratorState(Entry[] currentTable) {
            this.currentTable = currentTable;
            this.end = this.currentTable.length - 1;
        }

        private IteratorState(Entry[] currentTable, int start, int end) {
            this.currentTable = currentTable;
            this.start = start;
            this.end = end;
        }
    }

    /**
     * The HashIterator class is an abstract base class for iterators over the map.
     * It maintains the current state of the iteration, which includes the current table
     * being iterated over and the index of the next entry to be returned.
     * The findNext() method is used to advance the iterator to the next entry.
     */
    private abstract class HashIterator implements IntIterator {

        private List<IteratorState> todo;
        private IteratorState currentState;
        private Entry next;
        private int index;

        protected HashIterator() {
            this.currentState = new IteratorState(IntMapByDynamicHash.this.table);
            this.findNext();
        }

        /**
         * This method is used to advance the iterator to the next entry.
         * It iterates over the entries in the current table from the current index
         * until it finds a non-null entry. If it encounters a RESIZED or RESIZING entry,
         * it helps with the resize operation and continues the iteration in the new table.
         * If it reaches the end of the current table and there are still tables left to be
         * iterated over, it switches to the next table.
         */
        private void findNext() {
            while (this.index < this.currentState.end) {
                Entry o =
                        (Entry) IntMapByDynamicHash.tableAt(this.currentState.currentTable,
                                                            this.index);
                if (o == RESIZED || o == RESIZING) {
                    Entry[] nextArray =
                            IntMapByDynamicHash.this.helpWithResizeWhileCurrentIndex(
                                    this.currentState.currentTable, this.index);
                    int endResized = this.index + 1;
                    while (endResized < this.currentState.end) {
                        if (IntMapByDynamicHash.tableAt(this.currentState.currentTable,
                                                        endResized) != RESIZED) {
                            break;
                        }
                        endResized++;
                    }
                    if (this.todo == null) {
                        this.todo = new ArrayList<>(4);
                    }
                    if (endResized < this.currentState.end) {
                        this.todo.add(new IteratorState(
                                this.currentState.currentTable, endResized, this.currentState.end));
                    }
                    int powerTwoLength = this.currentState.currentTable.length - 1;
                    this.todo.add(new IteratorState(nextArray, this.index + powerTwoLength,
                                                    endResized + powerTwoLength));
                    this.currentState.currentTable = nextArray;
                    this.currentState.end = endResized;
                    this.currentState.start = this.index;
                } else if (o != null) {
                    this.next = o;
                    this.index++;
                    break;
                } else {
                    this.index++;
                }
            }
            if (this.next == null && this.index == this.currentState.end && this.todo != null &&
                !this.todo.isEmpty()) {
                this.currentState = this.todo.remove(this.todo.size() - 1);
                this.index = this.currentState.start;
                this.findNext();
            }
        }

        @Override
        public final boolean hasNext() {
            return this.next != null;
        }

        final Entry nextEntry() {
            Entry e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            return e;
        }
    }

    private final class ValueIterator extends HashIterator {

        @Override
        public int next() {
            return this.nextEntry().getValue();
        }
    }

    private final class KeyIterator extends HashIterator {

        @Override
        public int next() {
            return this.nextEntry().getKey();
        }
    }

    /* ---------------- Unsafe mechanics -------------- */
    private static final Unsafe UNSAFE = IntSet.UNSAFE;
    private static final long ENTRY_ARRAY_BASE;
    private static final int ENTRY_ARRAY_SHIFT;
    private static final long INT_ARRAY_BASE;
    private static final int INT_ARRAY_SHIFT;
    private static final long SIZE_OFFSET;

    static {
        try {
            Class<?> tableClass = Entry[].class;
            ENTRY_ARRAY_BASE = UNSAFE.arrayBaseOffset(tableClass);
            int objectArrayScale = UNSAFE.arrayIndexScale(tableClass);
            if ((objectArrayScale & (objectArrayScale - 1)) != 0) {
                throw new AssertionError("data type scale not a power of two");
            }
            ENTRY_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(objectArrayScale);

            Class<?> intArrayClass = int[].class;
            INT_ARRAY_BASE = UNSAFE.arrayBaseOffset(intArrayClass);
            int intArrayScale = UNSAFE.arrayIndexScale(intArrayClass);
            if ((intArrayScale & (intArrayScale - 1)) != 0) {
                throw new AssertionError("data type scale not a power of two");
            }
            INT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(intArrayScale);

            Class<?> mapClass = IntMapByDynamicHash.class;
            SIZE_OFFSET = UNSAFE.objectFieldOffset(mapClass.getDeclaredField("size"));
        } catch (NoSuchFieldException | SecurityException e) {
            throw new AssertionError(e);
        }
    }
}
