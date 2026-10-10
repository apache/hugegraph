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

package org.apache.hugegraph.unit.auth;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.auth.AuthConstant;
import org.apache.hugegraph.auth.TokenGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.security.WeakKeyException;

public class TokenGeneratorTest {

    private static final String SECRET = "01234567890123456789012345678901";

    @Test
    public void testCreateAndVerifyClaims() {
        TokenGenerator generator = new TokenGenerator(SECRET);
        Map<String, Object> payload = new HashMap<>();
        payload.put(AuthConstant.TOKEN_USER_NAME, "alice");
        payload.put(AuthConstant.TOKEN_USER_ID, "1234");
        long before = System.currentTimeMillis();
        Claims claims = generator.verify(generator.create(payload, 60000L));
        long after = System.currentTimeMillis();

        Assert.assertEquals("alice", claims.get(AuthConstant.TOKEN_USER_NAME));
        Assert.assertEquals("1234", claims.get(AuthConstant.TOKEN_USER_ID));
        Assert.assertFalse(claims.containsKey(AuthConstant.TOKEN_USER_PASSWORD));
        // JWT timestamps have second precision.
        Assert.assertTrue(claims.getExpiration().getTime() >= before + 59000L);
        Assert.assertTrue(claims.getExpiration().getTime() <= after + 60000L);
    }

    @Test
    public void testReadLegacyHs256Token() {
        // Fixed claims and independent HMAC-SHA256 signature, not this class's writer.
        String token = "eyJhbGciOiJIUzI1NiJ9." +
                       "eyJ1c2VyX25hbWUiOiJsZWdhY3ktdXNlciIsInVzZXJfaWQiOiIxMjM0IiwiZXhwIjo0MTAyNDQ0ODAwfQ." +
                       "t2b5xSruyIrZ8c45HOe6OhJt390rm_fdbvft1JLyRTs";
        Claims claims = new TokenGenerator(SECRET).verify(token);
        Assert.assertEquals("legacy-user", claims.get(AuthConstant.TOKEN_USER_NAME));
        Assert.assertEquals("1234", claims.get(AuthConstant.TOKEN_USER_ID));
        Assert.assertEquals(4102444800000L, claims.getExpiration().getTime());
    }

    @Test
    public void testRejectExpiredToken() {
        TokenGenerator generator = new TokenGenerator(SECRET);
        String token = generator.create(Collections.singletonMap("user_name", "alice"),
                                        -60000L);
        Assert.assertThrows(ExpiredJwtException.class, () -> generator.verify(token));
    }

    @Test
    public void testRejectWrongKeyAndMalformedTokens() {
        TokenGenerator generator = new TokenGenerator(SECRET);
        String token = generator.create(Collections.singletonMap("user_name", "alice"),
                                        60000L);
        TokenGenerator other = new TokenGenerator("abcdefghijklmnopqrstuvwxyz012345");
        Assert.assertThrows(JwtException.class, () -> other.verify(token));
        Assert.assertThrows(JwtException.class, () -> generator.verify("not-a-token"));
        Assert.assertThrows(IllegalArgumentException.class, () -> generator.verify(""));
    }

    @Test
    public void testRejectUnsignedToken() {
        TokenGenerator generator = new TokenGenerator(SECRET);
        String token = generator.create(Collections.singletonMap("user_name", "alice"),
                                        60000L);
        String unsigned = "eyJhbGciOiJub25lIn0." + token.split("\\.")[1] + ".";
        Assert.assertThrows(JwtException.class, () -> generator.verify(unsigned));
    }

    @Test
    public void testRejectWeakKey() {
        Assert.assertThrows(WeakKeyException.class, () -> new TokenGenerator("short-key"));
    }
}
