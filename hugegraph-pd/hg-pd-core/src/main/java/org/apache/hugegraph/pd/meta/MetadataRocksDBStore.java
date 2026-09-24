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

package org.apache.hugegraph.pd.meta;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.KV;

import com.google.protobuf.Parser;

public class MetadataRocksDBStore extends MetadataStoreBase {

    HgKVStore store;

    PDConfig pdConfig;

    public MetadataRocksDBStore(PDConfig pdConfig) {
        store = MetadataFactory.getStore(pdConfig);
        this.pdConfig = pdConfig;
    }

    public HgKVStore getStore() {
        if (store == null) {
            store = MetadataFactory.getStore(pdConfig);
        }
        return store;
    }

    @Override
    public byte[] getOne(byte[] key) throws PDException {
        try {
            byte[] bytes = store.get(key);
            return bytes;
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    @Override
    public <E> E getOne(Parser<E> parser, byte[] key) throws PDException {
        try {
            byte[] bytes = store.get(key);
            if (ArrayUtils.isEmpty(bytes)) {
                return null;
            }
            return parser.parseFrom(bytes);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws PDException {
        try {
            getStore().put(key, value);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl) throws PDException {
        this.store.putWithTTL(key, value, ttl);
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws
                                                                                  PDException {
        this.store.putWithTTL(key, value, ttl, timeUnit);
    }

    @Override
    public byte[] getWithTTL(byte[] key) throws PDException {
        return this.store.getWithTTL(key);
    }

    @Override
    public List getListWithTTL(byte[] key) throws PDException {
        return this.store.getListWithTTL(key);
    }

    @Override
    public void removeWithTTL(byte[] key) throws PDException {
        this.store.removeWithTTL(key);
    }

    @Override
    public List<KV> scanPrefix(byte[] prefix) throws PDException {
        try {
            return this.store.scanPrefix(prefix);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    @Override
    public List<KV> scanRange(byte[] start, byte[] end) throws PDException {
        return this.store.scanRange(start, end);
    }

    @Override
    public <E> List<E> scanRange(Parser<E> parser, byte[] start, byte[] end) throws PDException {
        List<E> stores = new LinkedList<>();
        try {
            List<KV> kvs = this.scanRange(start, end);
            for (KV keyValue : kvs) {
                stores.add(parser.parseFrom((byte[]) keyValue.getValue()));
            }
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
        return stores;
    }

    @Override
    public <E> List<E> scanPrefix(Parser<E> parser, byte[] prefix) throws PDException {
        List<E> stores = new LinkedList<>();
        try {
            List<KV> kvs = this.scanPrefix(prefix);
            for (KV keyValue : kvs) {
                stores.add(parser.parseFrom((byte[]) keyValue.getValue()));
            }
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
        return stores;
    }

    @Override
    public boolean containsKey(byte[] key) throws PDException {
        return !ArrayUtils.isEmpty(store.get(key));
    }

    @Override
    public long remove(byte[] key) throws PDException {
        try {
            return this.store.remove(key);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public long removeByPrefix(byte[] prefix) throws PDException {
        try {
            return this.store.removeByPrefix(prefix);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public void clearAllCache() throws PDException {
        this.store.clear();
    }

    @Override
    public void close() {

    }
}
