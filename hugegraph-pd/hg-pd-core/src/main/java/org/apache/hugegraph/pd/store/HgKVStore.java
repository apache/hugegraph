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

package org.apache.hugegraph.pd.store;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;

public interface HgKVStore {

    void init(PDConfig config);

    void put(byte[] key, byte[] value) throws PDException;

    byte[] get(byte[] key) throws PDException;

    List<KV> scanPrefix(byte[] prefix);

    long remove(byte[] bytes) throws PDException;

    long removeByPrefix(byte[] bytes) throws PDException;

    void putWithTTL(byte[] key, byte[] value, long ttl) throws PDException;

    void putWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws PDException;

    byte[] getWithTTL(byte[] key) throws PDException;

    void removeWithTTL(byte[] key) throws PDException;

    List<byte[]> getListWithTTL(byte[] key) throws PDException;

    void clear() throws PDException;

    void saveSnapshot(String snapshotPath) throws PDException;

    void loadSnapshot(String snapshotPath) throws PDException;

    List<KV> scanRange(byte[] start, byte[] end);

    void close();

    /**
     * Wait until a local read sees every write committed before the call. A store that
     * is not replicated has nothing to wait for.
     */
    default void waitReadIndex() throws PDException {
    }
}
