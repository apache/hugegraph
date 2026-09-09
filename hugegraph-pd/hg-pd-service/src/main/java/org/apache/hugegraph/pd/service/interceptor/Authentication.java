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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.config.PDConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Simple internal authentication component for PD service.
 * <p>
 * <b>WARNING:</b> This class validates a Basic credential for internal modules
 * (hg, store, hubble, vermeer): the service name must be one of the four and the
 * password must match the shared secret configured via `auth.secret-key`. The
 * mechanism is designed for internal service-to-service communication only.
 * </p>
 *
 * <p><b>Important SEC Considerations:</b></p>
 * <ul>
 *   <li><b>DO NOT expose RPC interfaces to external networks</b> - This authentication is NOT
 *       designed for public-facing services and should only be used in trusted internal networks.</li>
 *   <li><b>Production Environment Best Practices:</b> It is STRONGLY RECOMMENDED to configure
 *       IP whitelisting and network-level access control policies (e.g., firewall rules,
 *       security groups) to restrict access to trusted sources only.</li>
 *   <li><b>Future Improvements:</b> This authentication mechanism will be enhanced in future
 *       versions with more robust security features. Do not rely on this as the sole security
 *       measure for production deployments.</li>
 * </ul>
 *
 * <p>
 * For production deployments, ensure proper network isolation and implement defense-in-depth
 * strategies including but not limited to:
 * - VPC isolation
 * - IP whitelisting
 * - TLS/mTLS encryption,
 * and regular security audits.
 * </p>
 */
@Slf4j
@Component
public class Authentication {
    private static final Set<String> innerModules = Set.of("hg", "store", "hubble", "vermeer");

    private static final AtomicBoolean missingSecretLogged = new AtomicBoolean();

    @Autowired
    private PDConfig pdConfig;

    protected <T> T authenticate(String authority, String token, Function<String, T> tokenCall,
                                 Supplier<T> call) {
        try {
            String invalidBasicInfo = "invalid basic authentication info";
            if (StringUtils.isEmpty(authority)) {
                throw new BadCredentialsException(invalidBasicInfo);
            }
            byte[] bytes = authority.getBytes(StandardCharsets.UTF_8);
            byte[] decode = Base64.getDecoder().decode(bytes);
            // RFC 7617: Basic credentials are UTF-8. Decoding with the platform
            // default would compare against a UTF-8 secret only when the host
            // locale happens to agree.
            String info = new String(decode, StandardCharsets.UTF_8);
            int delim = info.indexOf(':');
            if (delim == -1) {
                throw new BadCredentialsException(invalidBasicInfo);
            }

            String name = info.substring(0, delim);
            String pwd = info.substring(delim + 1);
            if (!innerModules.contains(name)) {
                throw new AccessDeniedException("invalid service name");
            }
            if (!verifySecret(pwd)) {
                throw new BadCredentialsException("invalid credential");
            }
            return call.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compare the password of the Basic credential with the shared secret
     * configured via `auth.secret-key`. A missing or empty secret refuses every
     * request instead of falling back to name-only authentication.
     */
    private boolean verifySecret(String pwd) {
        String secret = this.pdConfig == null ? null : this.pdConfig.getSecretKey();
        if (StringUtils.isEmpty(secret)) {
            // Logged once: this path is reachable by unauthenticated callers
            if (missingSecretLogged.compareAndSet(false, true)) {
                log.error("auth.secret-key is not configured, so every authenticated REST " +
                          "request is refused. Add it to conf/application.yml (or set " +
                          "HG_PD_AUTH_SECRET_KEY) and give every REST client the same value.");
            }
            return false;
        }
        return MessageDigest.isEqual(pwd.getBytes(StandardCharsets.UTF_8),
                                     secret.getBytes(StandardCharsets.UTF_8));
    }

    public static String getTokenKey(String name) {
        return "PD/TOKEN/" + name;
    }

}
