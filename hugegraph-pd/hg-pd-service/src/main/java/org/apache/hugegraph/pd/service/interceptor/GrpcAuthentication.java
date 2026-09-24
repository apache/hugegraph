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

package org.apache.hugegraph.pd.service.interceptor;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.common.Consts;
import org.apache.hugegraph.pd.service.ServiceGrpc;
import org.springframework.stereotype.Service;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

@Service
public class GrpcAuthentication extends Authentication implements ServerInterceptor, ServiceGrpc {

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        try {
            if (isLeader()) {
                String authority = headers.get(Consts.CREDENTIAL_KEY);
                String token = headers.get(Consts.TOKEN_KEY);
                Function<String, Listener<ReqT>> tokenCall = t -> {
                    ServerCall<ReqT, RespT> sc = new SimpleForwardingServerCall(call) {
                        @Override
                        public void sendHeaders(Metadata headers) {
                            headers.put(Consts.TOKEN_KEY, t);
                            super.sendHeaders(headers);
                        }
                    };
                    return next.startCall(sc, headers);
                };
                Supplier<Listener<ReqT>> c = () -> next.startCall(call, headers);
                return authenticate(authority, token, tokenCall, c);

            }
            return next.startCall(call, headers);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
