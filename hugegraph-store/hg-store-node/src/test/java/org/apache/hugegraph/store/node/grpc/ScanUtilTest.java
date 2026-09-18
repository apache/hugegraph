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

package org.apache.hugegraph.store.node.grpc;

import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.junit.Assert;
import org.junit.Test;

import io.grpc.stub.StreamObserver;

public class ScanUtilTest {

    @Test
    public void testBatchScanRejectsOrderByKey() {
        ScanQueryRequest request = ScanQueryRequest.newBuilder()
                                                   .setMethod(ScanMethod.RANGE)
                                                   .setTable("table")
                                                   .setOrderType(ScanOrderType.ORDER_BY_KEY)
                                                   .build();

        IllegalArgumentException error = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> ScanQueryProducer.requestOf("graph",
                                                  new String[]{"table"},
                                                  request));
        Assert.assertTrue(error.getMessage().contains("batch scan"));
    }

    @Test
    public void testOrderedAllPartitionRangeUsesOrderedScan() {
        RecordingWrapper wrapper = new RecordingWrapper();
        ScanStreamReq request = rangeRequest(ScanOrderType.ORDER_BY_KEY);

        ScanIterator iterator = ScanUtil.getIterator(request, wrapper);
        BackendColumn column = iterator.next();

        Assert.assertEquals(1, column.name[0] & 0xff);
        Assert.assertTrue(wrapper.orderedCalled);
        Assert.assertFalse(wrapper.legacyCalled);
    }

    @Test
    public void testUnorderedAllPartitionRangeUsesLegacyScan() {
        RecordingWrapper wrapper = new RecordingWrapper();
        ScanStreamReq request = rangeRequest(ScanOrderType.ORDER_NONE);

        ScanIterator iterator = ScanUtil.getIterator(request, wrapper);
        BackendColumn column = iterator.next();

        Assert.assertEquals(9, column.name[0] & 0xff);
        Assert.assertFalse(wrapper.orderedCalled);
        Assert.assertTrue(wrapper.legacyCalled);
    }

    @Test
    public void testOrderedScanRejectsOpaquePositionBeforeOpeningIterator() {
        RecordingWrapper wrapper = new RecordingWrapper();
        ScanStreamReq request = rangeRequest(ScanOrderType.ORDER_BY_KEY)
                                .toBuilder()
                                .setPosition(bytes(3))
                                .build();

        try {
            ScanUtil.getIterator(request, wrapper);
            Assert.fail("Expected ordered cursor rejection");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("position"));
        }
        Assert.assertFalse(wrapper.orderedCalled);
        Assert.assertFalse(wrapper.legacyCalled);
    }

    @Test
    public void testOneShotOrderedRangeUsesOrderedScanAndAcknowledgesIt() {
        RecordingWrapper wrapper = new RecordingWrapper();
        ScanStreamReq request = rangeRequest(ScanOrderType.ORDER_BY_KEY);
        AtomicReference<KvPageRes> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        ScanOneShotResponse.scanOneShot(request,
                                        observer(response, error), wrapper);

        Assert.assertNull(error.get());
        Assert.assertNotNull(response.get());
        Assert.assertEquals(1, response.get().getVersion());
        Assert.assertTrue(wrapper.orderedCalled);
        Assert.assertFalse(wrapper.legacyCalled);
    }

    @Test
    public void testOrderedStreamCloseAcknowledgesCapabilityBeforeFirstPage() {
        RecordingWrapper wrapper = new RecordingWrapper();
        AtomicReference<KvPageRes> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AppConfig config = new AppConfig();
        config.setServerWaitTime(1);
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        try {
            ScanStreamResponse stream = ScanStreamResponse.of(
                    observer(response, error), wrapper, executor, config);
            ScanStreamReq close = rangeRequest(ScanOrderType.ORDER_BY_KEY)
                                  .toBuilder()
                                  .setCloseFlag(1)
                                  .build();

            stream.onNext(close);

            Assert.assertNull(error.get());
            Assert.assertNotNull(response.get());
            Assert.assertTrue(response.get().getOver());
            Assert.assertEquals(1, response.get().getVersion());
            Assert.assertFalse(wrapper.orderedCalled);
            Assert.assertFalse(wrapper.legacyCalled);
        } finally {
            executor.shutdownNow();
        }
    }

    private static ScanStreamReq rangeRequest(ScanOrderType orderType) {
        return ScanStreamReq.newBuilder()
                            .setHeader(Header.newBuilder().setGraph("graph"))
                            .setMethod(ScanMethod.RANGE)
                            .setTable("table")
                            .setCode(-1)
                            .setStart(bytes(1))
                            .setEnd(bytes(5))
                            .setScanType(ScanIterator.Trait.SCAN_LT_END)
                            .setLimit(1L)
                            .setOrderType(orderType)
                            .build();
    }

    private static StreamObserver<KvPageRes> observer(
            AtomicReference<KvPageRes> response,
            AtomicReference<Throwable> error) {
        return new StreamObserver<KvPageRes>() {
            @Override
            public void onNext(KvPageRes value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable throwable) {
                error.set(throwable);
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static com.google.protobuf.ByteString bytes(int value) {
        return com.google.protobuf.ByteString.copyFrom(new byte[]{(byte) value});
    }

    private static final class RecordingWrapper extends HgStoreWrapperEx {

        private boolean orderedCalled;
        private boolean legacyCalled;

        private RecordingWrapper() {
            super(null);
            this.orderedCalled = false;
            this.legacyCalled = false;
        }

        @Override
        public ScanIterator scanOrdered(String graph, String table,
                                        byte[] start, byte[] end,
                                        int scanType, byte[] query) {
            this.orderedCalled = true;
            return new SingleColumnIterator(1);
        }

        @Override
        public ScanIterator scan(String graph, int partId, String table,
                                 byte[] start, byte[] end, int scanType,
                                 byte[] query) {
            this.legacyCalled = true;
            return new SingleColumnIterator(9);
        }
    }

    private static final class SingleColumnIterator implements ScanIterator {

        private final BackendColumn column;
        private boolean consumed;

        private SingleColumnIterator(int key) {
            byte[] bytes = new byte[]{(byte) key};
            this.column = BackendColumn.of(bytes, bytes);
            this.consumed = false;
        }

        @Override
        public boolean hasNext() {
            return !this.consumed;
        }

        @Override
        public boolean isValid() {
            return this.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.consumed = true;
            return (T) this.column;
        }

        @Override
        public void close() {
            this.consumed = true;
        }

        @Override
        public byte[] position() {
            return new byte[]{0, 0, 0, 1};
        }

        @Override
        public void seek(byte[] position) {
            if (position.length > 0) {
                throw new IllegalArgumentException(
                        "Ordered scan position is unsupported");
            }
        }
    }
}
