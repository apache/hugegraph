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

package org.apache.hugegraph.pd.client.interceptor;

import io.grpc.*;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.Cache;
import org.apache.hugegraph.pd.common.Consts;

@Slf4j
public class Authentication implements ClientInterceptor {

    private static Cache<String> cache = new Cache<>();
    private static long ttl = 3600L;
    private String authority;
    private String name;

    public Authentication(String userName, String authority) {
        assert !StringUtils.isEmpty(userName);
        this.name = userName;
        this.authority = authority;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {

            @Override
            public void sendMessage(ReqT message) {
                super.sendMessage(message);
            }

            @Override
            public void start(Listener<RespT> listener,
                              Metadata headers) {
                if (StringUtils.isEmpty(authority) || StringUtils.isEmpty(name)) {
                    throw new RuntimeException("invalid user name or password,access denied");
                }
                headers.put(Consts.CREDENTIAL_KEY, authority);
                String token = cache.get(name);
                if (token != null) {
                    headers.put(Consts.TOKEN_KEY, cache.get(name));
                }
                SimpleForwardingClientCallListener<RespT> callListener =
                        new SimpleForwardingClientCallListener<>(listener) {
                            @Override
                            public void onMessage(RespT message) {
                                super.onMessage(message);
                            }

                            @Override
                            public void onHeaders(Metadata headers) {
                                super.onHeaders(headers);
                                String t = headers.get(Consts.TOKEN_KEY);
                                cache.put(name, t, ttl);
                            }

                            @Override
                            public void onClose(Status status,
                                                Metadata trailers) {
                                super.onClose(status, trailers);
                            }
                        };
                super.start(callListener, headers);
            }
        };
    }
}
