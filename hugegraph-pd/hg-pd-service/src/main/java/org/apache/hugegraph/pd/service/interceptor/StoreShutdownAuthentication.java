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

import org.apache.hugegraph.pd.common.Consts;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.springframework.stereotype.Component;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Require a privileged internal credential for shutdown reports. Existing RPCs
 * retain their transport policy until the general gRPC authentication migration.
 */
@Component
public class StoreShutdownAuthentication extends Authentication implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        if (!PDGrpc.getReportStoreShutdownMethod().getFullMethodName()
                   .equals(call.getMethodDescriptor().getFullMethodName())) {
            return next.startCall(call, headers);
        }
        try {
            // Holders of auth.secret-key are privileged internal callers, not
            // individual Store identities. Validate on followers before redirect.
            authenticate(headers.get(Consts.CREDENTIAL_KEY), null, token -> true, () -> true);
        } catch (RuntimeException denied) {
            call.close(Status.UNAUTHENTICATED.withDescription("Shutdown report requires an internal credential"),
                       new Metadata());
            return new ServerCall.Listener<>() { };
        }
        return next.startCall(call, headers);
    }
}
