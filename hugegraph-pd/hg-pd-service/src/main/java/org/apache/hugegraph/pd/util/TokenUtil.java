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

package org.apache.hugegraph.pd.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.hugegraph.auth.AuthConstant;
import org.apache.hugegraph.auth.TokenGenerator;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

public class TokenUtil {

    private final TokenGenerator generator;
    public static final long AUTH_TOKEN_EXPIRE = 3600 * 24L * 1000;

    public TokenUtil(String secretKey) {
        this.generator = new TokenGenerator(secretKey);
    }

    // public String getToken(String[] info) {
    //    Id id = new IdGenerator.UuidId(UUID.fromString(info[0]));
    //    Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
    //                                             info[0],
    //                                             AuthConstant.TOKEN_USER_ID,
    //                                             id.asString());
    //    return generator.create(payload, AUTH_TOKEN_EXPIRE);
    // }
    public String getToken(String[] info) {
        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME, info[0]);
        byte[] bytes = generator.create(payload, AUTH_TOKEN_EXPIRE).
                                getBytes(StandardCharsets.UTF_8);
        byte[] encode = Base64.getEncoder().encode(bytes);
        return new String(encode, Charsets.UTF_8);
    }

    public boolean verify(String token, String[] info) {
        byte[] decode = Base64.getDecoder().decode(token);
        String d = new String(decode, StandardCharsets.UTF_8);
        return d.equals(info[1]);
    }
}
