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

package org.apache.hugegraph.pd.raft.auth;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class IpAuthHandler extends ChannelDuplexHandler {

    private volatile Set<String> resolvedIps;
    private static volatile IpAuthHandler instance;

    private IpAuthHandler(Set<String> allowedIps) {
        this.resolvedIps = resolveAll(allowedIps);
    }

    public static IpAuthHandler getInstance(Set<String> allowedIps) {
        if (instance == null) {
            synchronized (IpAuthHandler.class) {
                if (instance == null) {
                    instance = new IpAuthHandler(allowedIps);
                }
            }
        }
        return instance;
    }

    /**
     * Returns the existing singleton instance, or null if not yet initialized.
     * Should only be called after getInstance(Set) has been called during startup.
     */
    public static IpAuthHandler getInstance() {
        return instance;
    }

    /**
     * Refreshes the resolved IP allowlist from a new set of hostnames or IPs.
     * Should be called when the Raft peer list changes via RaftEngine#changePeerList().
     * Note: DNS-only changes (e.g. container restart with new IP, same hostname)
     * are not automatically detected and still require a process restart.
     */
    public void refresh(Set<String> newAllowedIps) {
        this.resolvedIps = resolveAll(newAllowedIps);
        log.info("IpAuthHandler allowlist refreshed, resolved {} entries", resolvedIps.size());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String clientIp = getClientIp(ctx);
        if (!isIpAllowed(clientIp)) {
            log.warn("Blocked connection from {}", clientIp);
            ctx.close();
            return;
        }
        super.channelActive(ctx);
    }

    private static String getClientIp(ChannelHandlerContext ctx) {
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        return remoteAddress.getAddress().getHostAddress();
    }

    private boolean isIpAllowed(String ip) {
        Set<String> resolved = this.resolvedIps;
        // Empty allowlist means no restriction is configured — allow all
        return resolved.isEmpty() || resolved.contains(ip);
    }

    private static Set<String> resolveAll(Set<String> entries) {
        Set<String> result = new HashSet<>(entries);

        for (String entry : entries) {
            try {
                for (InetAddress addr : InetAddress.getAllByName(entry)) {
                    result.add(addr.getHostAddress());
                }
            } catch (UnknownHostException e) {
                log.warn("Could not resolve allowlist entry '{}': {}", entry, e.getMessage());
            }
        }

        return Collections.unmodifiableSet(result);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String clientIp = getClientIp(ctx);
        log.warn("Client : {} connection exception : {}", clientIp, cause);
        if (ctx.channel().isActive()) {
            ctx.close().addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("Client: {} connection closed failed: {}",
                             clientIp, future.cause().getMessage());
                }
            });
        }
    }
}
