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

package org.apache.hugegraph.store.rocksdb;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.junit.After;
import org.junit.BeforeClass;

public class BaseRocksDbTest {

    public static HugeConfig hConfig;

    @BeforeClass
    public static void init() {
        OptionSpace.register("org/apache/hugegraph/store/rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");

        hConfig = new HugeConfig(configMap);
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        rFactory.setHugeConfig(hConfig);

    }

    @After
    public void teardown() throws Exception {
        // pass
    }
}
