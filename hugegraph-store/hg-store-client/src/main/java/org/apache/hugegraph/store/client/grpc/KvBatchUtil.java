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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.ScanCondition;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/04/23
 */
@Slf4j
@NotThreadSafe
class KvBatchUtil {

    static final byte[] EMPTY_POSITION = HgStoreClientConst.EMPTY_BYTES;

    static ScanStreamBatchReq.Builder getRequestBuilder(HgStoreNodeSession nodeSession) {
        return ScanStreamBatchReq.newBuilder().setHeader(getHeader(nodeSession));
    }

    static ScanQueryRequest createQueryReq(HgScanQuery scanQuery, long pageSize) {

        ScanQueryRequest.Builder qb = ScanQueryRequest.newBuilder();
        ScanCondition.Builder cb = ScanCondition.newBuilder();

        qb.setLimit(getLimit(scanQuery.getLimit()));
        qb.setPerKeyLimit(getLimit(scanQuery.getPerKeyLimit()));
        qb.setPerKeyMax(getLimit(scanQuery.getPerKeyMax()));

        switch (scanQuery.getScanMethod()) {
            case ALL:
                qb.setMethod(ScanMethod.ALL);
                break;
            case PREFIX:
                qb.setMethod(ScanMethod.PREFIX);
                addPrefixCondition(scanQuery, qb, cb);
                break;
            case RANGE:
                qb.setMethod(ScanMethod.RANGE);
                addRangeCondition(scanQuery, qb, cb);
                break;
            default:
                throw new RuntimeException("Unsupported ScanType: " + scanQuery.getScanMethod());
        }

        qb.setTable(scanQuery.getTable());
        qb.setPageSize(pageSize);
        qb.setQuery(toBs(scanQuery.getQuery()));
        qb.setScanType(scanQuery.getScanType());
        qb.setOrderType(scanQuery.getOrderType());
        qb.setSkipDegree(scanQuery.getSkipDegree());

        return qb.build();
    }

    static long getLimit(long limit) {
        return limit <= HgStoreClientConst.NO_LIMIT ? Integer.MAX_VALUE : limit;
    }

    static Header getHeader(HgStoreNodeSession nodeSession) {
        return Header.newBuilder().setGraph(nodeSession.getGraphName()).build();
    }

    static void addPrefixCondition(HgScanQuery scanQuery, ScanQueryRequest.Builder qb,
                                   ScanCondition.Builder cb) {
        List<HgOwnerKey> prefixList = scanQuery.getPrefixList();

        if (prefixList == null || prefixList.isEmpty()) {
            throw new RuntimeException(
                    "The prefix-list of ScanQuery shouldn't to be invalid in ScanMethod.PREFIX " +
                    "mode.");
        }

        prefixList.forEach((e) -> {
            qb.addCondition(cb.clear()
                              .setPrefix(toBs(e.getKey()))
                              .setCode(e.getKeyCode())
                              .setSerialNo(e.getSerialNo())
                              .build()
            );
        });

    }

    static void addRangeCondition(HgScanQuery scanQuery, ScanQueryRequest.Builder qb,
                                  ScanCondition.Builder cb) {
        List<HgOwnerKey> startList = scanQuery.getStartList();
        List<HgOwnerKey> endList = scanQuery.getEndList();

        if (startList == null || startList.isEmpty()) {
            throw new RuntimeException(
                    "The start-list of ScanQuery shouldn't to be invalid in ScanMethod.RANGE mode" +
                    ".");
        }

        if (endList == null || endList.isEmpty()) {
            throw new RuntimeException(
                    "The end-list of ScanQuery shouldn't to be invalid in ScanMethod.RANGE mode.");
        }

        if (startList.size() != endList.size()) {
            throw new RuntimeException("The size of start-list not equals end-list's.");
        }

        for (int i = 0, s = startList.size(); i < s; i++) {
            HgOwnerKey start = startList.get(i);
            HgOwnerKey end = endList.get(i);
            qb.addCondition(cb.clear().setCode(start.getKeyCode())
                              .setStart(toBs(start.getKey()))
                              .setEnd(toBs(end.getKey()))
                              .setSerialNo(start.getSerialNo())
                              .build()
            );
        }

    }

    static HgOwnerKey toOk(HgOwnerKey key) {
        return key == null ? HgStoreClientConst.EMPTY_OWNER_KEY : key;
    }

    static ByteString toBs(byte[] bytes) {
        return ByteString.copyFrom((bytes != null) ? bytes : HgStoreClientConst.EMPTY_BYTES);
    }

}
