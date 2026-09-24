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

package org.apache.hugegraph.pd.service;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.DiscoveryServiceGrpc;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.pd.license.LicenseVerifierService;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;

@Useless("discovery related")
@Slf4j
@GRpcService
public class DiscoveryService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase implements
                                                                                    ServiceGrpc {

    static final AtomicLong id = new AtomicLong();
    private static final String CORES = "cores";
    RegistryService register = null;
    LicenseVerifierService licenseVerifierService;
    @Autowired
    private PDConfig pdConfig;

    @PostConstruct
    public void init() throws PDException {
        log.info("PDService init………… {}", pdConfig);
        RaftEngine.getInstance().init(pdConfig.getRaft());
        RaftEngine.getInstance().addStateListener(this);
        register = new RegistryService(pdConfig);
        licenseVerifierService = new LicenseVerifierService(pdConfig);
    }

    private Pdpb.ResponseHeader newErrorHeader(PDException e) {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                                                 Pdpb.Error.newBuilder().setTypeValue(e.getErrorCode()).setMessage(e.getMessage()))
                                                        .build();
        return header;
    }

    @Override
    public void register(NodeInfo request, io.grpc.stub.StreamObserver<RegisterInfo> observer) {
        if (!isLeader()) {
            redirectToLeader(DiscoveryServiceGrpc.getRegisterMethod(), request, observer);
            return;
        }
        int outTimes = pdConfig.getDiscovery().getHeartbeatOutTimes();
        RegisterInfo registerInfo;
        try {
            if (request.getAppName().equals("hg")) {
                Query queryRequest = Query.newBuilder().setAppName(request.getAppName())
                                          .setVersion(request.getVersion()).build();
                NodeInfos nodes = register.getNodes(queryRequest);
                String address = request.getAddress();
                int nodeCount = nodes.getInfoCount() + 1;
                for (NodeInfo node : nodes.getInfoList()) {
                    if (node.getAddress().equals(address)) {
                        nodeCount = nodes.getInfoCount();
                        break;
                    }
                }
                Map<String, String> labelsMap = request.getLabelsMap();
                String coreCount = labelsMap.get(CORES);
                if (StringUtils.isEmpty(coreCount)) {
                    throw new PDException(-1, "core count can not be null");
                }
                int core = Integer.parseInt(coreCount);
                licenseVerifierService.verify(core, nodeCount);
            }
            register.register(request, outTimes);
            String valueId = request.getId();
            registerInfo = RegisterInfo.newBuilder().setNodeInfo(NodeInfo.newBuilder().setId(
                                               "0".equals(valueId) ?
                                               String.valueOf(id.incrementAndGet()) : valueId).build())
                                       .build();

        } catch (PDException e) {
            registerInfo = RegisterInfo.newBuilder().setHeader(newErrorHeader(e)).build();
            log.debug("registerStore exception: ", e);
        } catch (PDRuntimeException ex) {
            Pdpb.Error error = Pdpb.Error.newBuilder().setTypeValue(ex.getErrorCode())
                                         .setMessage(ex.getMessage()).build();
            Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
            registerInfo = RegisterInfo.newBuilder().setHeader(header).build();
            log.debug("registerStore exception: ", ex);
        } catch (Exception e) {
            Pdpb.Error error =
                    Pdpb.Error.newBuilder().setTypeValue(Pdpb.ErrorType.UNKNOWN.getNumber())
                              .setMessage(e.getMessage()).build();
            Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
            registerInfo = RegisterInfo.newBuilder().setHeader(header).build();
        }
        observer.onNext(registerInfo);
        observer.onCompleted();
    }

    public void getNodes(Query request, io.grpc.stub.StreamObserver<NodeInfos> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(DiscoveryServiceGrpc.getGetNodesMethod(), request, responseObserver);
            return;
        }
        responseObserver.onNext(register.getNodes(request));
        responseObserver.onCompleted();
    }

    public boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

}
