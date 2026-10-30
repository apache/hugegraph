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

package org.apache.hugegraph.store.options;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.util.SizeUnit;

public class RaftRocksdbOptionsTest {

    @Test
    public void testRaftLogMemtablesShareOneManager() throws IOException, RocksDBException {
        RocksDB.loadLibrary();
        Path dir = Files.createTempDirectory("raft-log-wbm");
        try (Cache cache = new LRUCache(64 * SizeUnit.MB);
             DBOptions registered = RaftRocksdbOptions.raftLogDBOptions(Env.getDefault(),
                                                                        cache);
             // StorageOptionsFactory.getRocksDBOptions hands each log storage a copy like this
             DBOptions first = new DBOptions(registered);
             DBOptions second = new DBOptions(registered)) {
            WriteBufferManager manager = registered.writeBufferManager();
            assertNotNull(manager);
            assertSame(manager, first.writeBufferManager());
            assertSame(manager, second.writeBufferManager());

            // Nothing else uses the cache, so it starts empty
            assertEquals(0L, cache.getUsage());
            // A copy opens a working log DB, and its memtable is charged to the cache
            try (ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                 Options options = new Options(first, cfOptions).setCreateIfMissing(true);
                 RocksDB db = RocksDB.open(options, dir.toString())) {
                byte[] key = "k".getBytes(StandardCharsets.UTF_8);
                byte[] value = "v".getBytes(StandardCharsets.UTF_8);
                db.put(key, value);
                assertArrayEquals(value, db.get(key));
                assertTrue(cache.getUsage() > 0L);
            }
        } finally {
            FileUtils.deleteQuietly(dir.toFile());
        }
    }
}
