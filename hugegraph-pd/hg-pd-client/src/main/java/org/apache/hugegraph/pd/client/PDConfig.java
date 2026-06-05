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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;

import org.apache.commons.lang3.StringUtils;

import org.apache.hugegraph.pd.client.interceptor.AuthenticationException;

public final class PDConfig {

    // TODO: multi-server
    private String serverHost = "localhost:9000";

    // The timeout period for grpc call is 10 seconds
    private long grpcTimeOut = 60000;

    // Whether to receive asynchronous PD notifications
    private boolean enablePDNotify = false;

    private boolean enableCache = false;
    // FIXME: need to add AuthCheck
    private String authority = "DEFAULT";
    private String userName = "store";
    private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static final int GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static int inboundMessageSize = GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    private static int outboundMessageSize = GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE;

    private PDConfig() {
    }

    public static PDConfig of() {
        return new PDConfig();
    }

    public static PDConfig of(String serverHost) {
        PDConfig config = new PDConfig();
        config.serverHost = serverHost;
        return config;
    }

    public static PDConfig of(String serverHost, long timeOut) {
        PDConfig config = new PDConfig();
        config.serverHost = serverHost;
        config.grpcTimeOut = timeOut;
        return config;
    }

    public String getServerHost() {
        return serverHost;
    }

    public long getGrpcTimeOut() {
        return grpcTimeOut;
    }

    public void setGrpcTimeOut(long grpcTimeOut) {
        this.grpcTimeOut = grpcTimeOut;
    }

    @Deprecated
    public PDConfig setEnablePDNotify(boolean enablePDNotify) {
        this.enablePDNotify = enablePDNotify;
        this.enableCache = enablePDNotify;
        return this;
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    public PDConfig setEnableCache(boolean enableCache) {
        this.enableCache = enableCache;
        return this;
    }

    @Override
    public String toString() {
        return "PDConfig{" +
               "serverHost='" + serverHost + '\'' +
               '}';
    }

    public PDConfig setAuthority(String userName, String pwd) {
        // If userName is null or empty, keep default values for test environment
        if (StringUtils.isEmpty(userName)) {
            return this;
        }
        this.userName = userName;
        String auth = userName + ':' + pwd;
        this.authority = Base64.getEncoder().encodeToString(auth.getBytes(UTF_8));
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public String getAuthority() {
        if (StringUtils.isEmpty(this.authority)) {
            throw new AuthenticationException("invalid basic authentication info");
        }
        return authority;
    }

    public static int getInboundMessageSize() {
        return inboundMessageSize;
    }

    public static void setInboundMessageSize(int inboundMessageSize) {
        PDConfig.inboundMessageSize = inboundMessageSize;
    }

    public static int getOutboundMessageSize() {
        return outboundMessageSize;
    }

    public static void setOutboundMessageSize(int outboundMessageSize) {
        PDConfig.outboundMessageSize = outboundMessageSize;
    }
}
