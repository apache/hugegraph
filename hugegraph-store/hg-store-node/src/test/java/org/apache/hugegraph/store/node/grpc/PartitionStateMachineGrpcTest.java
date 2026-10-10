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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.grpc.common.GraphMethod;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.TableMethod;
import org.apache.hugegraph.store.grpc.session.BatchReq;
import org.apache.hugegraph.store.grpc.session.BatchWriteReq;
import org.apache.hugegraph.store.grpc.session.CleanReq;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.GraphReq;
import org.apache.hugegraph.store.grpc.session.TableReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.raft.DefaultRaftClosure;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.junit.Test;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.google.protobuf.GeneratedMessageV3;

public class PartitionStateMachineGrpcTest {

    @Test
    public void testLeaderHandlersDeliverGrpcResults() throws Exception {
        assertGrpcHandlers(false);
    }

    @Test
    public void testLeaderHandlersIgnoreResponseDeliveryFailure() throws Exception {
        assertGrpcHandlers(true);
    }

    private void assertGrpcHandlers(boolean throwFromCallback) throws Exception {
        Header header = Header.newBuilder().setGraph("apply-test").build();
        byte[] methods = {HgStoreNodeService.BATCH_OP, HgStoreNodeService.TABLE_OP,
                          HgStoreNodeService.GRAPH_OP, HgStoreNodeService.CLEAN_OP};
        GeneratedMessageV3[] requests = {
                BatchReq.newBuilder().setHeader(header).setBatchId("batch-1")
                        .setWriteReq(BatchWriteReq.getDefaultInstance()).build(),
                TableReq.newBuilder().setHeader(header).setTableName("table")
                        .setMethod(TableMethod.TABLE_METHOD_CREATE).build(),
                GraphReq.newBuilder().setHeader(header).setGraphName(header.getGraph())
                        .setMethod(GraphMethod.GRAPH_METHOD_DELETE).build(),
                CleanReq.newBuilder().setHeader(header).build()
        };
        for (int i = 0; i < methods.length; i++) {
            HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
            when(wrapper.doTable(0, TableMethod.TABLE_METHOD_CREATE, header.getGraph(), "table"))
                    .thenReturn(true);
            when(wrapper.doGraph(0, GraphMethod.GRAPH_METHOD_DELETE, header.getGraph()))
                    .thenReturn(true);
            when(wrapper.doClean(header.getGraph(), 0)).thenReturn(true);
            HgStoreSessionImpl session = new HgStoreSessionImpl();
            setField(session, "wrapper", wrapper);
            HgStoreNodeService service = new HgStoreNodeService(new AppConfig());
            setField(service, "hgStoreSession", session);
            HgStoreEngine engine = mock(HgStoreEngine.class);
            setField(service, "storeEngine", engine);
            PartitionStateMachine stateMachine = new PartitionStateMachine(
                    0, mock(SnapshotHandler.class));
            stateMachine.addTaskHandler(service);
            AtomicInteger callbacks = new AtomicInteger();
            AtomicReference<Status> callbackStatus = new AtomicReference<>();
            GrpcClosure<FeedbackRes> response = new GrpcClosure<FeedbackRes>() {
                @Override
                public void run(Status status) {
                    callbackStatus.set(status);
                    callbacks.incrementAndGet();
                    if (throwFromCallback) {
                        throw new IllegalStateException("injected response delivery failure");
                    }
                }
            };
            RaftOperation operation = RaftOperation.create(methods[i], requests[i]);
            Iterator iterator = mock(Iterator.class);
            when(iterator.hasNext()).thenReturn(true, false);
            when(iterator.done()).thenReturn(new DefaultRaftClosure(operation, response));
            when(iterator.getData()).thenReturn(ByteBuffer.wrap(operation.getValues()));
            when(iterator.getIndex()).thenReturn(1L);

            stateMachine.onApply(iterator);

            assertNotNull("handler must retain the typed response for op " + methods[i],
                          response.getResult());
            assertEquals(ResCode.RES_CODE_OK, response.getResult().getStatus().getCode());
            assertEquals(1, callbacks.get());
            assertTrue(callbackStatus.get().isOk());
            assertEquals(1L, stateMachine.getCommittedIndex());
            verify(iterator, never()).setErrorAndRollback(anyLong(), any(Status.class));
            verify(iterator).next();
            switch (methods[i]) {
                case HgStoreNodeService.BATCH_OP:
                    verify(wrapper).doBatch(header.getGraph(), 0,
                                            ((BatchReq) requests[i]).getWriteReq().getEntryList());
                    break;
                case HgStoreNodeService.TABLE_OP:
                    verify(wrapper).doTable(0, TableMethod.TABLE_METHOD_CREATE,
                                            header.getGraph(), "table");
                    break;
                case HgStoreNodeService.GRAPH_OP:
                    verify(engine).deletePartition(0, header.getGraph());
                    verify(wrapper).doGraph(0, GraphMethod.GRAPH_METHOD_DELETE, header.getGraph());
                    break;
                case HgStoreNodeService.CLEAN_OP:
                    verify(wrapper).doClean(header.getGraph(), 0);
                    break;
                default:
                    throw new AssertionError("unexpected method");
            }
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

}
