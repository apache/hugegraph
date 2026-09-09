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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.rest.interceptor.RestAuthentication;
import org.junit.Assert;
import org.junit.Test;

/**
 * In-process cover for the REST credential check. The suites that exercise it
 * over HTTP talk to a PD in another JVM, so nothing here is covered by them.
 */
public class AuthenticationTest {

    private static final String SECRET = "unit-test-secret";

    private static Authentication authWithSecret(String secret) throws Exception {
        Authentication auth = new Authentication();
        PDConfig config = new PDConfig();
        config.setSecretKey(secret);
        Field field = Authentication.class.getDeclaredField("pdConfig");
        field.setAccessible(true);
        field.set(auth, config);
        return auth;
    }

    private static RestAuthentication restAuthWithSecret(String secret) throws Exception {
        RestAuthentication auth = new RestAuthentication();
        PDConfig config = new PDConfig();
        config.setSecretKey(secret);
        Field field = Authentication.class.getDeclaredField("pdConfig");
        field.setAccessible(true);
        field.set(auth, config);
        return auth;
    }

    private static boolean acceptsRest(RestAuthentication auth, String authHeader) {
        try {
            HttpServletRequest req = (HttpServletRequest) Proxy.newProxyInstance(
                    HttpServletRequest.class.getClassLoader(),
                    new Class<?>[]{HttpServletRequest.class},
                    (proxy, method, args) -> {
                        if ("getHeader".equals(method.getName())) {
                            return "Authorization".equalsIgnoreCase((String) args[0]) ? authHeader : null;
                        }
                        if ("getMethod".equals(method.getName())) {
                            return "GET";
                        }
                        if ("getRequestURI".equals(method.getName())) {
                            return "/v1/stores";
                        }
                        return null;
                    });
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            HttpServletResponse resp = (HttpServletResponse) Proxy.newProxyInstance(
                    HttpServletResponse.class.getClassLoader(),
                    new Class<?>[]{HttpServletResponse.class},
                    (proxy, method, args) -> {
                        if ("getWriter".equals(method.getName())) {
                            return pw;
                        }
                        return null;
                    });
            return auth.preHandle(req, resp, null);
        } catch (Exception e) {
            return false;
        }
    }

    private static String credential(String name, String pwd) {
        return Base64.getEncoder().encodeToString(
                (name + ":" + pwd).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean accepts(Authentication auth, String authority) {
        try {
            return auth.authenticate(authority, null, t -> Boolean.TRUE, () -> Boolean.TRUE);
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Test
    public void testEveryInnerModuleIsAcceptedWithTheSecret() throws Exception {
        Authentication auth = authWithSecret(SECRET);
        for (String name : new String[]{"hg", "store", "hubble", "vermeer"}) {
            Assert.assertTrue(name + " should be accepted with the right secret",
                              accepts(auth, credential(name, SECRET)));
        }
    }

    @Test
    public void testPasswordIsActuallyChecked() throws Exception {
        Authentication auth = authWithSecret(SECRET);
        Assert.assertFalse("wrong password must be refused",
                           accepts(auth, credential("hg", "wrong-password")));
        Assert.assertFalse("empty password must be refused",
                           accepts(auth, credential("hg", "")));
        Assert.assertFalse("secret as the name must not help",
                           accepts(auth, credential(SECRET, SECRET)));
    }

    @Test
    public void testUnknownServiceNameIsRefused() throws Exception {
        Authentication auth = authWithSecret(SECRET);
        Assert.assertFalse(accepts(auth, credential("nobody", SECRET)));
        Assert.assertFalse(accepts(auth, credential("admin", SECRET)));
    }

    @Test
    public void testMissingOrMalformedCredentialIsRefused() throws Exception {
        Authentication auth = authWithSecret(SECRET);
        Assert.assertFalse(accepts(auth, null));
        Assert.assertFalse(accepts(auth, ""));
        // no colon
        Assert.assertFalse(accepts(auth, Base64.getEncoder().encodeToString(
                "hg".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testUnconfiguredSecretRefusesEveryone() throws Exception {
        for (String secret : new String[]{null, ""}) {
            Authentication auth = authWithSecret(secret);
            Assert.assertFalse("an unset secret must not fall back to a name check",
                               accepts(auth, credential("hg", "")));
            Assert.assertFalse(accepts(auth, credential("hg", SECRET)));
        }
    }

    @Test
    public void testNonAsciiSecretDoesNotDependOnTheDefaultCharset() throws Exception {
        String secret = "sécrèt-2026";
        Authentication auth = authWithSecret(secret);
        Assert.assertTrue(accepts(auth, credential("hg", secret)));
        Assert.assertFalse(accepts(auth, credential("hg", "secret-2026")));
    }

    @Test
    public void testPublishedSecretRefusesStartup() {
        PDConfig config = new PDConfig();
        config.setSecretKey("FXQXbJtbCLxODc6tGci732pkH1cyf8Qg");
        Assert.assertThrows(IllegalStateException.class, config::afterPropertiesSet);
    }

    @Test
    public void testOwnSecretStarts() {
        PDConfig config = new PDConfig();
        config.setSecretKey(SECRET);
        config.afterPropertiesSet();
    }

    @Test
    public void testRestAuthenticationAcceptsCaseInsensitiveBasicScheme() throws Exception {
        RestAuthentication auth = restAuthWithSecret(SECRET);
        for (String name : new String[]{"hg", "store", "hubble", "vermeer"}) {
            String cred = credential(name, SECRET);
            Assert.assertTrue("Basic with uppercase B should be accepted for " + name,
                              acceptsRest(auth, "Basic " + cred));
            Assert.assertTrue("basic with all lowercase should be accepted for " + name,
                              acceptsRest(auth, "basic " + cred));
            Assert.assertTrue("BASIC with all uppercase should be accepted for " + name,
                              acceptsRest(auth, "BASIC " + cred));
            Assert.assertTrue("BaSiC with mixed case should be accepted for " + name,
                              acceptsRest(auth, "BaSiC " + cred));
        }
    }

    @Test
    public void testRestAuthenticationRefusesNonBasicOrMalformedSchemes() throws Exception {
        RestAuthentication auth = restAuthWithSecret(SECRET);
        String cred = credential("hg", SECRET);
        Assert.assertFalse("null Authorization header must be refused",
                           acceptsRest(auth, null));
        Assert.assertFalse("empty Authorization header must be refused",
                           acceptsRest(auth, ""));
        Assert.assertFalse("Basic with empty credentials must be refused",
                           acceptsRest(auth, "Basic "));
        Assert.assertFalse("basic with empty credentials must be refused",
                           acceptsRest(auth, "basic "));
        Assert.assertFalse("Bearer scheme must be refused",
                           acceptsRest(auth, "Bearer " + cred));
        Assert.assertFalse("Digest scheme must be refused",
                           acceptsRest(auth, "Digest " + cred));
        Assert.assertFalse("Basic without trailing space must be refused",
                           acceptsRest(auth, "Basic" + cred));
        Assert.assertFalse("invalid credential with basic prefix must be refused",
                           acceptsRest(auth, "basic " + credential("hg", "wrong-secret")));
        Assert.assertFalse("unknown service name with basic prefix must be refused",
                           acceptsRest(auth, "basic " + credential("unknown", SECRET)));
        Assert.assertFalse("non-base64 token with basic prefix must be refused",
                           acceptsRest(auth, "basic ???"));
    }
}
