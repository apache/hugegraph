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

package org.apache.hugegraph.auth;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import javax.crypto.SecretKey;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

public class TokenGenerator {

    private final SecretKey key;

    public TokenGenerator(String secretKey) {
        this.key = Keys.hmacShaKeyFor(secretKey.getBytes(StandardCharsets.UTF_8));
    }

    public String create(Map<String, ?> payload, long expire) {
        return Jwts.builder()
                   .setClaims(payload)
                   .setExpiration(new Date(System.currentTimeMillis() + expire))
                   .signWith(this.key, SignatureAlgorithm.HS256)
                   .compact();
    }

    /**
     * Verify a signed token. Callers translate JJWT failures into their own
     * authentication or transport errors.
     */
    public Claims verify(String token) {
        Jws<Claims> claimsJws = Jwts.parserBuilder()
                                  .setSigningKey(this.key)
                                  .build()
                                  .parseClaimsJws(token);
        return claimsJws.getBody();
    }
}
