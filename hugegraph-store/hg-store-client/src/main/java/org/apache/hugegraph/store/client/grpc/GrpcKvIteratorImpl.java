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

package org.apache.hugegraph.store.client.grpc;

import java.util.List;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvOrderedIterator;
import org.apache.hugegraph.store.HgKvPagingIterator;
import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.HgSeekAble;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.HgStoreClientUtil;
import org.apache.hugegraph.store.grpc.common.Kv;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/10/20
 *
 * @version 0.2.1
 */
@Slf4j
class GrpcKvIteratorImpl implements HgKvPagingIterator<HgKvEntry>, HgKvOrderedIterator<HgKvEntry> {

    private final byte[] emptyBytes = HgStoreClientConst.EMPTY_BYTES;
    private final KvCloseableIterator<Kv> iterator;
    private final HgPageSize pageLimiter;
    private final HgStoreNodeSession session;
    private HgKvEntry element;

    private GrpcKvIteratorImpl(HgStoreNodeSession session, KvCloseableIterator<Kv> iterator,
                               HgPageSize pageLimiter) {
        this.iterator = iterator;
        this.pageLimiter = pageLimiter;
        this.session = session;
    }

    public static HgKvIterator<HgKvEntry> of(HgStoreNodeSession nodeSession,
                                             KvCloseableIterator<Kv> iterator) {
        if (iterator instanceof HgPageSize) {
            return of(nodeSession, iterator, (HgPageSize) iterator);
        }
        return new GrpcKvIteratorImpl(nodeSession, iterator, () -> 1);
    }

    public static HgKvIterator<HgKvEntry> of(HgStoreNodeSession nodeSession,
                                             KvCloseableIterator<Kv> iterator,
                                             HgPageSize pageLimiter) {
        return new GrpcKvIteratorImpl(nodeSession, iterator, pageLimiter);
    }

    public static HgKvIterator<HgKvEntry> of(HgStoreNodeSession nodeSession, List<Kv> kvList) {
        int pageSize = kvList.size();
        return new GrpcKvIteratorImpl(nodeSession, new KvListIterator<Kv>(kvList), () -> pageSize);
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public HgKvEntry next() {
        Kv kv = this.iterator.next();
        this.element = new GrpcKvEntryImpl(kv.getKey().toByteArray(), kv.getValue().toByteArray(),
                                           kv.getCode());
        return this.element;
    }

    @Override
    public byte[] key() {
        if (this.element == null) {
            return null;
        }
        return this.element.key();
    }

    @Override
    public byte[] value() {
        if (this.element == null) {
            return null;
        }
        return this.element.value();
    }

    @Override
    public byte[] position() {
        if (this.element == null) {
            return emptyBytes;
        }
        byte[] key = this.element.key();
        if (key == null) {
            return emptyBytes;
        }
        if (!(this.iterator instanceof HgSeekAble)) {
            return emptyBytes;
        }
        byte[] upstream = ((HgSeekAble) this.iterator).position();
        byte[] code = HgStoreClientUtil.toIntBytes(this.element.code());
        byte[] result = new byte[upstream.length + Integer.BYTES + key.length];
        System.arraycopy(upstream, 0, result, 0, upstream.length);
        System.arraycopy(code, 0, result, upstream.length, Integer.BYTES);
        System.arraycopy(key, 0, result, upstream.length + Integer.BYTES, key.length);
        return result;
    }

    @Override
    public void seek(byte[] position) {
        if (this.iterator instanceof HgSeekAble) {
            ((HgSeekAble) this.iterator).seek(position);
        }
    }

    @Override
    public long getPageSize() {
        return pageLimiter.getPageSize();
    }

    @Override
    public boolean isPageEmpty() {
        return !iterator.hasNext();
    }

    @Override
    public int compareTo(HgKvOrderedIterator o) {
        return Long.compare(this.getSequence(), o.getSequence());
    }

    @Override
    public long getSequence() {
        return this.session.getStoreNode().getNodeId().longValue();
    }

    @Override
    public void close() {
        this.iterator.close();
    }
}
