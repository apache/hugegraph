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

package org.apache.hugegraph.pd.common;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class GraphCache {

    private Graph graph;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean writing = new AtomicBoolean(false);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Integer, AtomicBoolean> state = new ConcurrentHashMap<>();
    private Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private volatile RangeMap<Long, Integer> range = TreeRangeMap.create();

    public GraphCache(Graph graph) {
        this.graph = graph;
    }

    public void init(List<Partition> ps) {
        Map<Integer, Partition> gps = new ConcurrentHashMap<>(ps.size(), 1);
        if (!CollectionUtils.isEmpty(ps)) {
            WriteLock lock = getLock().writeLock();
            try {
                lock.lock();
                for (Partition p : ps) {
                    gps.put(p.getId(), p);
                    range.put(Range.closedOpen(p.getStartKey(), p.getEndKey()), p.getId());
                }
            } catch (Exception e) {
                log.warn("init graph with error:", e);
            } finally {
                lock.unlock();
            }
        }
        setPartitions(gps);

    }

    public Partition getPartition(Integer id) {
        return partitions.get(id);
    }

    public Partition addPartition(Integer id, Partition p) {
        return partitions.put(id, p);
    }

    public Partition removePartition(Integer id) {
        Partition p = partitions.get(id);
        if (p != null) {
            RangeMap<Long, Integer> range = getRange();
            if (Objects.equals(p.getId(), range.get(p.getStartKey())) &&
                Objects.equals(p.getId(), range.get(p.getEndKey() - 1))) {
                WriteLock lock = getLock().writeLock();
                lock.lock();
                try {
                    range.remove(range.getEntry(p.getStartKey()).getKey());
                } catch (Exception e) {
                    log.warn("remove partition with error:", e);
                } finally {
                    lock.unlock();
                }
            }
        }
        return partitions.remove(id);
    }

    public void removePartitions() {
        getState().clear();
        RangeMap<Long, Integer> range = getRange();
        WriteLock lock = getLock().writeLock();
        try {
            lock.lock();
            if (range != null) {
                range.clear();
            }
        } catch (Exception e) {
            log.warn("remove partition with error:", e);
        } finally {
            lock.unlock();
        }
        getPartitions().clear();
        getInitialized().set(false);
    }

    /*
     * Requires external write lock
     * */
    public void reset() {
        partitions.clear();
        try {
            range.clear();
        } catch (Exception e) {

        }
    }

    public boolean updatePartition(Partition partition) {
        int partId = partition.getId();
        Partition p = getPartition(partId);
        if (p != null && p.equals(partition)) {
            return false;
        }
        WriteLock lock = getLock().writeLock();
        try {
            lock.lock();
            RangeMap<Long, Integer> range = getRange();
            addPartition(partId, partition);
            try {
                if (p != null) {
                    // The old [1-3) is overwritten by [2-3). When [1-3) becomes [1-2), the
                    // original [1-3) should not be deleted.
                    // Only when it is confirmed that the old start and end are both your own can
                    // the old be deleted (i.e., before it is overwritten).
                    if (Objects.equals(partId, range.get(partition.getStartKey())) &&
                        Objects.equals(partId, range.get(partition.getEndKey() - 1))) {
                        range.remove(range.getEntry(partition.getStartKey()).getKey());
                    }
                }
                range.put(Range.closedOpen(partition.getStartKey(), partition.getEndKey()), partId);
            } catch (Exception e) {
                log.warn("update partition with error:", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
        return true;
    }

}
