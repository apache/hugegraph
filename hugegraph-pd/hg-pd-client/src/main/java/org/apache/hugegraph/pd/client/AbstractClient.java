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

package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.client.interceptor.Authentication;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.PDGrpc.PDBlockingStub;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.GetMembersRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetMembersResponse;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractClient implements Closeable {

    private static ConcurrentHashMap<String, ManagedChannel> chs = new ConcurrentHashMap<>();
    public static Pdpb.ResponseHeader okHeader = Pdpb.ResponseHeader.newBuilder().setError(
            Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.OK)).build();
    protected final Pdpb.RequestHeader header;
    protected final AbstractClientStubProxy proxy;
    protected final PDConfig config;
    protected ManagedChannel channel = null;
    protected ConcurrentMap<String, AbstractBlockingStub> stubs = null;

    protected AbstractClient(PDConfig config) {
        String[] hosts = config.getServerHost().split(",");
        this.proxy = new AbstractClientStubProxy(hosts);
        this.header = Pdpb.RequestHeader.getDefaultInstance();
        this.config = config;
    }

    public static Pdpb.ResponseHeader newErrorHeader(int errorCode, String errorMsg) {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                Pdpb.Error.newBuilder().setTypeValue(errorCode).setMessage(errorMsg)).build();
        return header;
    }

    protected static void handleErrors(Pdpb.ResponseHeader header) throws PDException {
        Pdpb.Error error = header.getError();
        if (header.hasError() && error.getType() != Pdpb.ErrorType.OK) {
            throw new PDException(error.getTypeValue(),
                                  String.format("PD request error, error code = %d, msg = %s",
                                                error.getTypeValue(),
                                                error.getMessage()));
        }
    }

    public static <T extends AbstractStub> T setBlockingParams(T stub, PDConfig config) {
        stub = (T) stub.withDeadlineAfter(config.getGrpcTimeOut(), TimeUnit.MILLISECONDS)
                       .withMaxInboundMessageSize(PDConfig.getInboundMessageSize());
        return (T) stub.withInterceptors(
                new Authentication(config.getUserName(), config.getAuthority()));

    }

    public static <T extends AbstractStub> T setAsyncParams(T stub, PDConfig config) {
        return (T) stub.withMaxInboundMessageSize(PDConfig.getInboundMessageSize())
                       .withInterceptors(
                               new Authentication(config.getUserName(), config.getAuthority()));
    }

    protected AbstractBlockingStub getBlockingStub() throws PDException {
        if (proxy.getBlockingStub() == null) {
            synchronized (this) {
                if (proxy.getBlockingStub() == null) {
                    String host = resetStub();
                    if (host.isEmpty()) {
                        throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                              "PD unreachable, pd.peers=" + config.getServerHost());
                    }
                }
            }
        }
        return setBlockingParams(proxy.getBlockingStub(), config);
    }

    protected AbstractStub getStub() throws PDException {
        if (proxy.getStub() == null) {
            synchronized (this) {
                if (proxy.getStub() == null) {
                    String host = resetStub();
                    if (host.isEmpty()) {
                        throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                              "PD unreachable, pd.peers=" + config.getServerHost());
                    }
                }
            }
        }
        return setAsyncParams(proxy.getStub(), config);
    }

    protected abstract AbstractStub createStub();

    protected abstract AbstractBlockingStub createBlockingStub();

    private String resetStub() {
        String leaderHost = "";
        Exception ex = null;
        for (int i = 0; i < proxy.getHostCount(); i++) {
            String host = proxy.nextHost();
            closeConnections();

            channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
            PDBlockingStub blockingStub =
                    setBlockingParams(PDGrpc.newBlockingStub(channel), config);
            try {
                GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                                  .setHeader(header).build();
                GetMembersResponse members = blockingStub.getMembers(request);
                Metapb.Member leader = members.getLeader();
                leaderHost = leader.getGrpcUrl();
                if (!host.equals(leaderHost)) {
                    closeConnections();
                    channel = ManagedChannelBuilder.forTarget(leaderHost).usePlaintext().build();
                }
                proxy.setBlockingStub(setBlockingParams(createBlockingStub(), config));
                proxy.setStub(setAsyncParams(createStub(), config));
                log.info("AbstractClient connect to host = {} success", leaderHost);
                break;
            } catch (StatusRuntimeException se) {
                ex = se;
                continue;
            } catch (Exception e) {
                ex = e;
                String msg =
                        String.format("AbstractClient connect to %s with error: %s", host,
                                      e.getMessage());
                log.error(msg, e);
            }
        }
        if (StringUtils.isEmpty(leaderHost) && ex != null) {
            log.error(String.format("connect to %s with error: ", config.getServerHost()), ex);
        }
        return leaderHost;
    }

    protected <ReqT, RespT> RespT blockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req) throws PDException {
        return blockingUnaryCall(method, req, 0);
    }

    protected <ReqT, RespT> RespT blockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req, int retry) throws PDException {
        AbstractBlockingStub stub = getBlockingStub();
        try {
            RespT resp =
                    ClientCalls.blockingUnaryCall(stub.getChannel(), method, stub.getCallOptions(),
                                                  req);
            return resp;
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                if (retry < proxy.getHostCount()) {
                    // Network connection lost. Disconnect from the previous connection and reconnect using a different host.
                    synchronized (this) {
                        proxy.setBlockingStub(null);
                    }
                    return blockingUnaryCall(method, req, ++retry);
                }
            } else {
                log.error(method.getFullMethodName() + " exception, ", e);
            }
        }
        return null;
    }

    // this.stubs = new ConcurrentHashMap<String,AbstractBlockingStub>(hosts.length);
    private AbstractBlockingStub getConcurrentBlockingStub(String address) {
        AbstractBlockingStub stub = stubs.get(address);
        if (stub != null) {
            return stub;
        }
        Channel ch = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        PDBlockingStub blockingStub = setBlockingParams(PDGrpc.newBlockingStub(ch), config);
        stubs.put(address, blockingStub);
        return blockingStub;

    }

    protected <ReqT, RespT> KVPair<Boolean, RespT> concurrentBlockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req, Predicate<RespT> predicate) throws
                                                                                        PDException {
        LinkedList<String> hostList = this.proxy.getHostList();
        if (this.stubs == null) {
            synchronized (this) {
                if (this.stubs == null) {
                    this.stubs = new ConcurrentHashMap<>(hostList.size());
                }
            }
        }
        Stream<RespT> respTStream = hostList.parallelStream().map((address) -> {
            AbstractBlockingStub stub = getConcurrentBlockingStub(address);
            RespT resp = ClientCalls.blockingUnaryCall(stub.getChannel(),
                                                       method, stub.getCallOptions(), req);
            return resp;
        });
        KVPair<Boolean, RespT> pair;
        AtomicReference<RespT> response = new AtomicReference<>();
        boolean result = respTStream.anyMatch((r) -> {
            response.set(r);
            return predicate.test(r);
        });
        if (result) {
            pair = new KVPair<>(true, null);
        } else {
            pair = new KVPair<>(false, response.get());
        }
        return pair;
    }

    protected <ReqT, RespT> void streamingCall(MethodDescriptor<ReqT, RespT> method, ReqT request,
                                               StreamObserver<RespT> responseObserver,
                                               int retry) throws PDException {
        AbstractStub stub = getStub();
        try {
            ClientCall<ReqT, RespT> call = stub.getChannel().newCall(method, stub.getCallOptions());
            ClientCalls.asyncServerStreamingCall(call, request, responseObserver);
        } catch (Exception e) {
            log.error("rpc call with exception :", e);
            if (e instanceof StatusRuntimeException) {
                if (retry < proxy.getHostCount()) {
                    synchronized (this) {
                        proxy.setStub(null);
                    }
                    streamingCall(method, request, responseObserver, ++retry);
                    return;
                }
            }
            throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                  "RPC streaming call failed", e);
        }
    }

    @Override
    public void close() {
        closeConnections();
    }

    private void closeConnections() {
        closeChannel(channel);
        if (stubs != null) {
            for (AbstractBlockingStub stub : stubs.values()) {
                closeChannel((ManagedChannel) stub.getChannel());
            }
        }
    }

    private void closeChannel(ManagedChannel channel) {
        try {
            while (channel != null &&
                   !channel.shutdownNow().awaitTermination(100, TimeUnit.MILLISECONDS)) {
                continue;
            }
        } catch (Exception e) {
            log.info("Close channel with error :.", e);
        }
    }
}
