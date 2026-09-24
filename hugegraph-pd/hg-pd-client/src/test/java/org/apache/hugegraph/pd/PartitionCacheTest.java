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

package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PartitionCache;
import org.apache.hugegraph.pd.grpc.Metapb;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
// import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionCacheTest {

    // @Test
    public void test() {
        PartitionCache cache = new PartitionCache();
        for (int i = 0; i < 10; i++) {
            KVPair<Metapb.Partition, Metapb.Shard> partShards =
                    new KVPair<>(Metapb.Partition.newBuilder()
                                                 .setStartKey(i * 10)
                                                 .setEndKey((i + 1) * 10)
                                                 .build(), null);
            cache.updatePartition("aa", i, partShards.getKey());
        }

        for (int i = 0; i < 100; i++) {
            KVPair<Metapb.Partition, Metapb.Shard> partShards = cache.getPartitionByCode("aa", i);
            System.out.println(" " + i + " " + partShards.getKey().getStartKey());
        }
    }

    // @Test
    public void test1() {
        Map<String, RangeMap<Long, Integer>> keyToPartIdCache = new HashMap<>();
        // graphName + PartitionID form the key
        Map<String, KVPair<Metapb.Partition, Metapb.Shard>> partitionCache = new HashMap<>();

        // Cache all stores for full database queries; optimisation required.
        Map<String, List<Metapb.Store>> allStoresCache = new HashMap<>();

        keyToPartIdCache.put("a", TreeRangeMap.create());

        keyToPartIdCache.get("a")
                        .put(Range.closedOpen(1L, 2L), 1);

        allStoresCache.put("a", new ArrayList<>());
        allStoresCache.get("a").add(Metapb.Store.newBuilder().setId(34).build());

        Map<String, RangeMap<Long, Integer>> keyToPartIdCache2 =
                cloneKeyToPartIdCache(keyToPartIdCache);
        System.out.println(keyToPartIdCache2.size());
    }

    public Map<String, RangeMap<Long, Integer>> cloneKeyToPartIdCache(
            Map<String, RangeMap<Long, Integer>> cache) {
        Map<String, RangeMap<Long, Integer>> cacheClone = new HashMap<>();
        cache.forEach((k1, v1) -> {
            cacheClone.put(k1, TreeRangeMap.create());
            v1.asMapOfRanges().forEach((k2, v2) -> {
                cacheClone.get(k1).put(k2, v2);
            });
        });
        return cacheClone;
    }

    public Map<String, KVPair<Metapb.Partition, Metapb.Shard>>
    clonePartitionCache(Map<String, KVPair<Metapb.Partition, Metapb.Shard>> cache) {
        Map<String, KVPair<Metapb.Partition, Metapb.Shard>> cacheClone = new HashMap<>();
        cacheClone.putAll(cache);
        return cacheClone;
    }

    public Map<String, List<Metapb.Store>>
    cloneStoreCache(Map<String, List<Metapb.Store>> cache) {
        Map<String, List<Metapb.Store>> cacheClone = new HashMap<>();
        cacheClone.putAll(cache);
        return cacheClone;
    }
}
