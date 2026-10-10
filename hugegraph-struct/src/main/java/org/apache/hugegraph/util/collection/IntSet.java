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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.UnsafeUtil;
import org.eclipse.collections.api.collection.primitive.MutableIntCollection;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

public interface IntSet {

    boolean add(int key);

    boolean remove(int key);

    boolean contains(int key);

    void clear();

    int size();

    boolean concurrent();

    /**
     * NOTE: IntSetBySegments(backend by IntSetByFixedAddr) is:
     * - slower 2.5x than IntSetByFixedAddr for single thread;
     * - slower 2.0x than IntSetByFixedAddr for 4 threads;
     */
    final class IntSetBySegments implements IntSet {

        private final IntSet[] sets;
        private final long capacity;
        private final long unsignedSize;
        private final int segmentSize;
        private final int segmentShift;
        private final int segmentMask;
        private final Function<Integer, IntSet> creator;

        private static final int DEFAULT_SEGMENTS = IntSet.CPUS * 100;
        private static final Function<Integer, IntSet> DEFAULT_CREATOR =
                IntSetByFixedAddr4Unsigned::new;

        @SuppressWarnings("static-access")
        private static final int BASE_OFFSET = UNSAFE.ARRAY_OBJECT_BASE_OFFSET;
        @SuppressWarnings("static-access")
        private static final int SHIFT = 31 - Integer.numberOfLeadingZeros(
                UNSAFE.ARRAY_OBJECT_INDEX_SCALE);

        public IntSetBySegments(int capacity) {
            this(capacity, DEFAULT_SEGMENTS, DEFAULT_CREATOR);
        }

        public IntSetBySegments(int capacity, int segments) {
            this(capacity, segments, DEFAULT_CREATOR);
        }

        public IntSetBySegments(int capacity, int segments,
                                Function<Integer, IntSet> creator) {
            E.checkArgument(segments >= 1,
                            "Invalid segments %s", segments);
            E.checkArgument(capacity >= segments,
                            "Invalid capacity %s, expect >= segments %s",
                            capacity, segments);

            this.sets = new IntSet[segments];
            // include signed and unsigned number
            this.unsignedSize = capacity;
            this.capacity = this.unsignedSize * 2L;
            this.segmentSize = IntSet.segmentSize(this.capacity, segments);
            this.segmentShift = Integer.numberOfTrailingZeros(this.segmentSize);
            /*
             * The mask is lower bits of each segment size, like
             * segmentSize=4096 (0x1000), segmentMask=4095 (0xfff),
             * NOTE: `-1 >>> 0` or `-1 >>> 32` is -1.
             */
            this.segmentMask = this.segmentShift == 0 ?
                               0 : -1 >>> (32 - this.segmentShift);
            this.creator = creator;
        }

        @Override
        public boolean add(int key) {
            int innerKey = (int) ((key + this.unsignedSize) & this.segmentMask);
            return segment(key).add(innerKey);
        }

        @Override
        public boolean remove(int key) {
            int innerKey = (int) ((key + this.unsignedSize) & this.segmentMask);
            return segment(key).remove(innerKey);
        }

        @Override
        public boolean contains(int key) {
            long ukey = key + this.unsignedSize;
            if (ukey >= this.capacity || ukey < 0L) {
                return false;
            }
            int innerKey = (int) (ukey & this.segmentMask);
            return segment(key).contains(innerKey);
        }

        @Override
        public void clear() {
            for (int i = 0; i < this.sets.length; i++) {
                IntSet set = this.segmentAt(i);
                if (set != null) {
                    set.clear();
                }
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (int i = 0; i < this.sets.length; i++) {
                IntSet set = this.segmentAt(i);
                if (set != null) {
                    size += set.size();
                }
                // TODO: can we assume all the remaining sets are null here
            }
            return size;
        }

        @Override
        public boolean concurrent() {
            return true;
        }

        private IntSet segment(int key) {
            long ukey = key + this.unsignedSize;
            if (ukey >= this.capacity || ukey < 0L) {
                E.checkArgument(false,
                                "The key %s is out of bound %s",
                                key, this.capacity);
            }

            long index = ukey >>> this.segmentShift;
            IntSet exist = this.sets[(int) index];
            if (exist != null) {
                return exist;
            }

            // volatile get this.sets[index]
            long offset = (index << SHIFT) + BASE_OFFSET;
            Object old = UNSAFE.getObjectVolatile(this.sets, offset);
            if (old != null) {
                return (IntSet) old;
            }

            // set this.sets[index] = new IntSet()
            IntSet set = this.creator.apply(this.segmentSize);
            while (true) {
                if (UNSAFE.compareAndSwapObject(this.sets, offset, null, set)) {
                    return set;
                }
                old = UNSAFE.getObjectVolatile(this.sets, offset);
                if (old != null) {
                    return (IntSet) old;
                }
            }
        }

        private IntSet segmentAt(int index) {
            // volatile get this.sets[index]
            long offset = ((long) index << SHIFT) + BASE_OFFSET;
            IntSet set = (IntSet) UNSAFE.getObjectVolatile(this.sets, offset);
            return set;
        }
    }

    /**
     * NOTE: IntSetByFixedAddr is:
     * - faster 3x than ec IntIntHashSet for single thread;
     * - faster 6x than ec IntIntHashSet for 4 threads, 4x operations
     * with 0.67x cost;
     * - faster 20x than ec IntIntHashSet-segment-lock for 4 threads;
     * - faster 60x than ec IntIntHashSet-global-lock for 4 threads;
     */
    final class IntSetByFixedAddr implements IntSet {

        private final long[] bits;
        private final long numBits;
        private final long numBitsUnsigned;
        private final AtomicInteger size;

        public IntSetByFixedAddr(int numBits) {
            this.numBitsUnsigned = numBits;
            this.numBits = numBits * 2L;
            this.bits = new long[IntSet.bits2words(this.numBits)];
            this.size = new AtomicInteger();
        }

        @Override
        public boolean add(int key) {
            long ukey = key + this.numBitsUnsigned;
            long offset = this.offset(key);
            long bitmask = IntSetByFixedAddr4Unsigned.bitmaskOfKey(ukey);

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV | bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] |= bitmask;
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.incrementAndGet();
                    return true;
                }
            }
        }

        @Override
        public boolean contains(int key) {
            long ukey = key + this.numBitsUnsigned;
            if (ukey >= this.numBits || ukey < 0L) {
                return false;
            }
            long offset = this.offset(key);
            long bitmask = IntSetByFixedAddr4Unsigned.bitmaskOfKey(ukey);

            long value = UNSAFE.getLongVolatile(this.bits, offset);
            return (value & bitmask) != 0L;
        }

        @Override
        public boolean remove(int key) {
            long ukey = key + this.numBitsUnsigned;
            long offset = this.offset(key);
            long bitmask = IntSetByFixedAddr4Unsigned.bitmaskOfKey(ukey);

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV & ~bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] &= ~bitmask
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.decrementAndGet();
                    return true;
                }
            }
        }

        @Override
        public void clear() {
            Arrays.fill(this.bits, 0);
            this.size.set(0);
        }

        @Override
        public int size() {
            return this.size.get();
        }

        @Override
        public boolean concurrent() {
            return true;
        }

        private long offset(long key) {
            long ukey = key + this.numBitsUnsigned;
            if (ukey >= this.numBits || ukey < 0L) {
                E.checkArgument(false, "The key %s is out of bound %s",
                                key, this.numBits);
            }
            return IntSetByFixedAddr4Unsigned.bitOffsetToByteOffset(ukey);
        }
    }

    final class IntSetByFixedAddr4Unsigned implements IntSet {

        private final long[] bits;
        private final int numBits;
        private final AtomicInteger size;

        @SuppressWarnings("static-access")
        private static final int BASE_OFFSET = UNSAFE.ARRAY_LONG_BASE_OFFSET;
        @SuppressWarnings("static-access")
        private static final int MUL8 = 31 - Integer.numberOfLeadingZeros(
                UNSAFE.ARRAY_LONG_INDEX_SCALE);

        public IntSetByFixedAddr4Unsigned(int numBits) {
            this.numBits = numBits;
            this.bits = new long[IntSet.bits2words(numBits)];
            this.size = new AtomicInteger();
        }

        @Override
        public boolean add(int key) {
            long offset = this.offset(key);
            long bitmask = bitmaskOfKey(key);

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV | bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] |= bitmask;
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.incrementAndGet();
                    return true;
                }
            }
        }

        @Override
        public boolean contains(int key) {
            if (key >= this.numBits || key < 0) {
                return false;
            }
            long offset = this.offset(key);
            long bitmask = bitmaskOfKey(key);

            long value = UNSAFE.getLongVolatile(this.bits, offset);
            return (value & bitmask) != 0L;
        }

        @Override
        public boolean remove(int key) {
            long offset = this.offset(key);
            long bitmask = bitmaskOfKey(key);

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV & ~bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] &= ~bitmask
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.decrementAndGet();
                    return true;
                }
            }
        }

        @Override
        public void clear() {
            Arrays.fill(this.bits, 0);
            this.size.set(0);
        }

        @Override
        public int size() {
            return this.size.get();
        }

        @Override
        public boolean concurrent() {
            return true;
        }

        public int nextKey(int key) {
            if (key < 0) {
                key = 0;
            }
            if (key >= this.numBits) {
                return key;
            }

            long offset = this.offset(key);
            int startBit = key & (int) MOD64;
            int bitsEachLong = 64;
            int bytesEachLong = 8;
            key -= startBit;

            // check the first long
            long value = UNSAFE.getLongVolatile(this.bits, offset);
            if (value != 0L) {
                for (int bit = startBit; bit < bitsEachLong; bit++) {
                    long bitmask = 1L << bit;
                    if ((value & bitmask) != 0L) {
                        return key + bit;
                    }
                }
            }
            offset += bytesEachLong;
            key += bitsEachLong;

            // check the remaining
            while (key < this.numBits) {
                value = UNSAFE.getLongVolatile(this.bits, offset);
                if (value != 0L) {
                    for (int bit = 0; bit < bitsEachLong; bit++) {
                        long bitmask = 1L << bit;
                        if ((value & bitmask) != 0L) {
                            return key + bit;
                        }
                    }
                }
                offset += bytesEachLong;
                key += bitsEachLong;
            }
            return key;
        }

        private long offset(int key) {
            if (key >= this.numBits || key < 0) {
                E.checkArgument(false, "The key %s is out of bound %s",
                                key, this.numBits);
            }
            return bitOffsetToByteOffset(key);
        }

        private static long bitOffsetToByteOffset(long key) {
            // bits to long offset
            long index = key >> DIV64;
            // long offset to byte offset
            long offset = index << MUL8;
            // add the array base offset
            offset += BASE_OFFSET;
            return offset;
        }

        private static long bitmaskOfKey(long key) {
            long bitIndex = key & MOD64;
            long bitmask = 1L << bitIndex;
            return bitmask;
        }
    }

    final class IntSetByEcSegment implements IntSet {

        private final MutableIntCollection[] sets;
        private final int segmentMask;

        public IntSetByEcSegment(int segments) {
            segments = IntSet.sizeToPowerOf2Size(segments);
            this.segmentMask = segments - 1;
            this.sets = new MutableIntCollection[segments];
            for (int i = 0; i < segments; i++) {
                /*
                 * NOTE: asSynchronized() is:
                 * - about slower 2x for single thread;
                 * - about slower 4x for 4 threads, 4x operations with 16x cost;
                 * - about faster 20x than global-lock for 4 threads;
                 */
                this.sets[i] = new IntHashSet().asSynchronized();
            }
        }

        private MutableIntCollection set(int key) {
            // NOTE '%' is slower 20% ~ 50% than '&': key % this.sets.length;
            int index = key & this.segmentMask;
            return this.sets[index];
        }

        @Override
        public boolean add(int key) {
            return set(key).add(key);
        }

        @Override
        public boolean contains(int key) {
            return set(key).contains(key);
        }

        @Override
        public boolean remove(int key) {
            return set(key).remove(key);
        }

        @Override
        public void clear() {
            for (MutableIntCollection set : this.sets) {
                set.clear();
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (MutableIntCollection set : this.sets) {
                size += set.size();
            }
            return size;
        }

        @Override
        public boolean concurrent() {
            return false;
        }
    }

    final class IntSetByFixedAddrByHppc implements IntSet {

        private final com.carrotsearch.hppc.BitSet bits;

        public IntSetByFixedAddrByHppc(int numBits) {
            this.bits = new com.carrotsearch.hppc.BitSet(numBits);
        }

        @Override
        public boolean add(int key) {
            this.bits.set(key);
            return true;
        }

        @Override
        public boolean remove(int key) {
            this.bits.clear(key);
            return true;
        }

        @Override
        public boolean contains(int key) {
            return this.bits.get(key);
        }

        @Override
        public void clear() {
            this.bits.clear();
        }

        @Override
        public int size() {
            return (int) this.bits.size();
        }

        @Override
        public boolean concurrent() {
            return false;
        }
    }

    int CPUS = Runtime.getRuntime().availableProcessors();
    sun.misc.Unsafe UNSAFE = UnsafeUtil.getUnsafe();

    long MOD64 = 0x3fL;
    int DIV64 = 6;

    static int segmentSize(long capacity, int segments) {
        long eachSize = capacity / segments;
        eachSize = IntSet.sizeToPowerOf2Size((int) eachSize);
        /*
         * Supply total size
         * like capacity=20 and segments=19, then eachSize=1
         * should increase eachSize to eachSize * 2.
         */
        while (eachSize * segments < capacity) {
            eachSize <<= 1;
        }
        return (int) eachSize;
    }

    static int sizeToPowerOf2Size(int size) {
        if (size < 1) {
            size = 1;
        }

        int n = size - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        size = n + 1;

        return size;
    }

    static int bits2words(long numBits) {
        return (int) ((numBits - 1) >>> DIV64) + 1;
    }
}
