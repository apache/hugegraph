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

package org.apache.hugegraph.store.business;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.BiFunction;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/11/2
 *
 * @version 1.1.0 implements position method to pass partition-id on 2022/03/10
 */
@Slf4j
public class MultiPartitionIterator implements ScanIterator {

    public final static byte[] EMPTY_BYTES = new byte[0];
    private final Queue<Integer> partitions;
    private final BiFunction<Integer, byte[], ScanIterator> supplier;
    private ScanIterator iterator;
    private Integer curPartitionId;
    private Integer positionPartitionId;
    private byte[] positionKey;
    private RuntimeException cleanupFailure;
    private boolean closed;

    private MultiPartitionIterator(List<Integer> partitionIds,
                                   BiFunction<Integer, byte[], ScanIterator> supplier) {
        /*****************************************************************************
         ** CAUTION: MAKE SURE IT SORTED IN A FIXED ORDER! TO DO THIS IS FOR PAGING. **
         *****************************************************************************/
        Collections.sort(partitionIds);
        this.partitions = new LinkedList<>(partitionIds);
        this.supplier = supplier;
    }

    public static MultiPartitionIterator of(List<Integer> partitionIdList,
                                            BiFunction<Integer, byte[], ScanIterator> supplier) {
        return new MultiPartitionIterator(partitionIdList, supplier);
    }

    private static byte[] toBytes(final int i) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(i);
        return buffer.array();
    }

    public static int toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getInt();
    }

    private ScanIterator getIterator() {
        while (!this.partitions.isEmpty()) {
            this.curPartitionId = this.partitions.poll();
            if (!this.inPosition(this.curPartitionId)) {
                continue;
            }
            ScanIterator child = this.supplier.apply(this.curPartitionId,
                                                     getPositionKey(this.curPartitionId));
            if (child == null) {
                continue;
            }
            try {
                if (child.hasNext()) {
                    return child;
                }
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(child, failure);
                throw failure;
            }
            closeCreatedIterator(child);
        }
        return null;
    }

    private void init() {
        if (!this.closed && this.iterator == null) {
            this.iterator = this.getIterator();
        }
    }

    @Override
    public boolean hasNext() {
        this.init();
        return this.iterator != null;
    }

    @Override
    public boolean isValid() {
        this.init();
        return this.iterator != null;
    }

    @Override
    public <T> T next() {
        this.init();
        if (this.iterator == null) {
            throw new NoSuchElementException();
        }
        T t = this.iterator.next();
        if (!this.iterator.hasNext()) {
            closeCurrentIterator();
        }
        return t;
    }

    @Override
    public long count() {
        long count = 0;
        while (this.hasNext()) {
            try {
                count += this.iterator.count();
            } catch (RuntimeException | Error failure) {
                ScanIterator child = this.iterator;
                this.iterator = null;
                closeAfterFailure(child, failure);
                throw failure;
            }
            closeCurrentIterator();
        }
        return count;
    }

    /**
     * @return the current partition-id in bytes form.
     */
    @Override
    public byte[] position() {
        if (this.curPartitionId == null) {
            return EMPTY_BYTES;
        }
        return toBytes(this.curPartitionId.shortValue());
    }

    @Override
    public void seek(byte[] position) {
        if (position == null || position.length < Integer.BYTES) {
            return;
        }
        byte[] buf = new byte[Integer.BYTES];
        System.arraycopy(position, 0, buf, 0, Integer.BYTES);
        this.positionPartitionId = toInt(buf);
        this.positionKey = new byte[position.length - Integer.BYTES];
        System.arraycopy(position, Integer.BYTES, this.positionKey, 0, this.positionKey.length);

    }

    @Override
    public void close() {
        this.closed = true;
        try {
            closeCurrentIterator();
        } catch (RuntimeException | Error failure) {
            // closeCreatedIterator retains the failure even after ownership is released here.
            throw this.cleanupFailure;
        }
        if (this.cleanupFailure != null) {
            throw this.cleanupFailure;
        }
    }

    private void closeCurrentIterator() {
        ScanIterator child = this.iterator;
        this.iterator = null;
        if (child != null) {
            closeCreatedIterator(child);
        }
    }

    private void closeAfterFailure(ScanIterator child, Throwable failure) {
        try {
            closeCreatedIterator(child);
        } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private boolean inPosition(int partitionId) {
        if (this.positionPartitionId == null) {
            return true;
        }
        return partitionId >= this.positionPartitionId;
    }

    private byte[] getPositionKey(int partitionId) {
        if (this.positionKey == null || this.positionKey.length == 0) {
            return null;
        }
        if (this.positionPartitionId == null) {
            return null;
        }
        if (this.positionPartitionId.intValue() == partitionId) {
            return this.positionKey;
        } else {
            return null;
        }

    }

    /**
     * obtain iteration list of all partitions
     *
     * @return iteration list
     */
    public List<ScanIterator> getIterators() {
        List<ScanIterator> opened = new ArrayList<>();
        if (this.closed) {
            return opened;
        }
        try {
            for (int id : this.partitions) {
                ScanIterator child = this.supplier.apply(id, getPositionKey(id));
                if (child == null) {
                    continue;
                }
                opened.add(child);
                if (!child.hasNext()) {
                    opened.remove(opened.size() - 1);
                    closeCreatedIterator(child);
                }
            }
            // Ownership transfers to the caller only when all partitions were opened.
            return opened;
        } catch (RuntimeException | Error failure) {
            for (ScanIterator child : opened) {
                try {
                    closeCreatedIterator(child);
                } catch (RuntimeException | Error closeFailure) {
                    if (closeFailure != failure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            throw failure;
        }
    }

    private void closeCreatedIterator(ScanIterator child) {
        try {
            child.close();
        } catch (RuntimeException | Error failure) {
            if (this.cleanupFailure == null) {
                this.cleanupFailure = new IllegalStateException(
                        "partition iterator cleanup failed", failure);
            } else {
                this.cleanupFailure.addSuppressed(failure);
            }
            throw failure;
        }
    }

}
