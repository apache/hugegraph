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
import java.util.Base64;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Component;

/**
 * Simple internal authentication component for PD service.
 * <p>
 * <b>WARNING:</b> This class currently implements only basic internal authentication
 * validation for internal modules (hg, store, hubble, vermeer). The authentication mechanism
 * is designed for internal service-to-service communication only.
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
@Component
public class Authentication {
    private static final Set<String> innerModules = Set.of("hg", "store", "hubble", "vermeer");

    protected <T> T authenticate(String authority, String token, Function<String, T> tokenCall,
                                 Supplier<T> call) {
        try {
            String invalidBasicInfo = "invalid basic authentication info";
            if (StringUtils.isEmpty(authority)) {
                throw new BadCredentialsException(invalidBasicInfo);
            }
            byte[] bytes = authority.getBytes(StandardCharsets.UTF_8);
            byte[] decode = Base64.getDecoder().decode(bytes);
            String info = new String(decode);
            int delim = info.indexOf(':');
            if (delim == -1) {
                throw new BadCredentialsException(invalidBasicInfo);
            }

            String name = info.substring(0, delim);
            // TODO: password validation is skipped — only service name is checked against
            // innerModules. Full credential validation should be added as part of the auth refactor.
            //String pwd = info.substring(delim + 1);
            if (innerModules.contains(name)) {
                return call.get();
            } else {
                throw new AccessDeniedException("invalid service name");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTokenKey(String name) {
        return "PD/TOKEN/" + name;
    }

}
