//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

import java.nio.charset.StandardCharsets
import java.util.Arrays

import org.rocksdb.Options
import org.rocksdb.RocksDB

rocksdbSmokePath = System.getenv('ROCKSDB_SMOKE_DIR')
if (!rocksdbSmokePath) {
    throw new IllegalArgumentException('ROCKSDB_SMOKE_DIR is required')
}

rocksdbSmokeKey = 'hugegraph-riscv64-key'.getBytes(StandardCharsets.UTF_8)
rocksdbSmokeExpected = 'hugegraph-riscv64-value'.getBytes(StandardCharsets.UTF_8)

RocksDB.loadLibrary()

rocksdbSmokeOptions = new Options().setCreateIfMissing(true)
rocksdbSmokeDb = null
try {
    rocksdbSmokeDb = RocksDB.open(rocksdbSmokeOptions, rocksdbSmokePath)
    rocksdbSmokeDb.put(rocksdbSmokeKey, rocksdbSmokeExpected)
    def actual = rocksdbSmokeDb.get(rocksdbSmokeKey)
    if (!Arrays.equals(rocksdbSmokeExpected, actual)) {
        throw new IllegalStateException('RocksDB value differs after put/get')
    }
    rocksdbSmokeDb.close()
    rocksdbSmokeDb = null

    rocksdbSmokeDb = RocksDB.open(rocksdbSmokeOptions, rocksdbSmokePath)
    actual = rocksdbSmokeDb.get(rocksdbSmokeKey)
    if (!Arrays.equals(rocksdbSmokeExpected, actual)) {
        throw new IllegalStateException('RocksDB value differs after reopen')
    }
    println('rocksdb-jni-smoke-ok')
} finally {
    rocksdbSmokeDb?.close()
    rocksdbSmokeOptions.close()
}
