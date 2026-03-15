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

package org.apache.hugegraph.pd.rest;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;

import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.PeerRestRequest;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class MemberAPI extends API {

    //TODO
    @Autowired
    PDService pdService;

    /**
     * Get member information
     * <p>
     * Retrieves all member information for the current PD cluster via an HTTP GET request and returns it in JSON format.
     *
     * @return A RestApiResponse object containing member information
     * @throws InterruptedException If the thread is interrupted while waiting, this exception is thrown
     * @throws ExecutionException If the task execution fails, this exception is thrown
     */
    @GetMapping(value = "/members", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getMembers() throws InterruptedException, ExecutionException {

        String leaderGrpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress();
        CallStreamObserverWrap<Pdpb.MembersAndClusterState> response =
                new CallStreamObserverWrap<>();
        pdService.getMembersAndClusterState(Pdpb.GetMembersRequest.newBuilder().build(), response);
        List<Member> members = new ArrayList<>();
        Member leader = null;
        Map<String, Integer> stateCountMap = new HashMap<>();
        Pdpb.MembersAndClusterState membersAndClusterState = response.get().get(0);
        for (Metapb.Member member : membersAndClusterState.getMembersList()) {
            String stateKey = member.getState().name();
            stateCountMap.put(stateKey, stateCountMap.getOrDefault(stateKey, 0) + 1);
            Member member1 = new Member(member);
            if ((leaderGrpcAddress != null) && (leaderGrpcAddress.equals(member.getGrpcUrl()))) {
                leader = member1;
            }
            member1.role = member.getRole().name();
            members.add(member1);
        }
        String state = membersAndClusterState.getState().toString();
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("state", state);
        resultMap.put("pdList", members);
        resultMap.put("pdLeader", leader);
        resultMap.put("numOfService", members.size());
        resultMap.put("numOfNormalService",
                      stateCountMap.getOrDefault(Metapb.StoreState.Up.name(), 0));
        resultMap.put("stateCountMap", stateCountMap);
        return new RestApiResponse(resultMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
    }

    /**
     * Change the Peer list in the PD cluster
     * Receive a request body containing the Peer list to be changed via an HTTP POST request, and call the corresponding service to change the Peer list in the PD cluster
     *
     * @param body    Request body containing the list of Peers to be modified, of type PeerRestRequest
     * @param request HTTP request object, of type HttpServletRequest
     * @return Returns a JSON string containing the modification results
     * @throws Exception If an exception occurs during request processing, service invocation, or Peer list modification, it is captured and returned as the JSON representation of the exception
     */
    // TODO: this endpoint has no authentication check — any caller with network
    // access to the management port can trigger a peer list change.
    // Wire authentication here as part of the planned auth refactor.
    @PostMapping(value = "/members/change", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String changePeerList(@RequestBody PeerRestRequest body, HttpServletRequest request) {
        try {
            Pdpb.ChangePeerListRequest rpcRequest =
                    Pdpb.ChangePeerListRequest.newBuilder().setPeerList(
                            body.getPeerList()).build();
            CountDownLatch latch = new CountDownLatch(1);
            final Pdpb.ResponseHeader[] responseHeader = {null};
            StreamObserver<Pdpb.getChangePeerListResponse> observer =
                    new StreamObserver<Pdpb.getChangePeerListResponse>() {
                        @Override
                        public void onNext(Pdpb.getChangePeerListResponse value) {
                            responseHeader[0] = value.getHeader();
                        }

                        @Override
                        public void onError(Throwable t) {
                            responseHeader[0] = Pdpb.ResponseHeader.newBuilder().setError(
                                    Pdpb.Error.newBuilder().setType(
                                            Pdpb.ErrorType.UNKNOWN).setMessage(
                                            t.getMessage()).build()).build();
                            latch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            latch.countDown();
                        }
                    };
            pdService.changePeerList(rpcRequest, observer);
            latch.await();
            return toJSON(responseHeader[0], "changeResult");
        } catch (Exception e) {
            return toJSON(e);
        }
    }

    public static class CallStreamObserverWrap<V> extends CallStreamObserver<V> implements
                                                                                Future<List<V>> {

        CompletableFuture<List<V>> future = new CompletableFuture<>();
        List<V> values = new ArrayList<>();

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable runnable) {

        }

        @Override
        public void disableAutoInboundFlowControl() {

        }

        @Override
        public void request(int i) {

        }

        @Override
        public void setMessageCompression(boolean b) {

        }

        @Override
        public void onNext(V v) {
            values.add(v);
        }

        @Override
        public void onError(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
            future.complete(values);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public List<V> get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public List<V> get(long timeout, TimeUnit unit) throws InterruptedException,
                                                               ExecutionException,
                                                               TimeoutException {
            return future.get(timeout, unit);
        }
    }

    @Data
    class Member {

        String raftUrl;
        String grpcUrl;
        String restUrl;
        String state;
        String dataPath;
        String role;
        String replicateState;
        String serviceName; // Service name, custom attributes
        String serviceVersion; // Static definitions
        long startTimeStamp; // Startup time: temporarily takes the startup time of the process

        public Member(Metapb.Member member) {
            if (member != null) {
                raftUrl = member.getRaftUrl();
                grpcUrl = member.getGrpcUrl();
                restUrl = member.getRestUrl();
                state = String.valueOf(member.getState());
                dataPath = member.getDataPath();
                serviceName = grpcUrl + "-PD";
                serviceVersion = VERSION;
                startTimeStamp = ManagementFactory.getRuntimeMXBean().getStartTime();
                replicateState = member.getReplicatorState();
            }

        }

        @Useless("delete later")
        public Member() {
        }
    }
}
