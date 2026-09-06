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

package org.apache.hugegraph.pd.rest;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.After;
import org.junit.BeforeClass;

public class BaseServerTest {

    // Must match the auth.secret-key that travis/start-pd.sh gives the PD
    // under test; the shipped config carries no secret by design
    protected static final String SECRET = "pd-ci-test-secret-not-for-production";
    protected static final String AUTH_HEADER = "Authorization";
    protected static final String VALID_AUTH = basicAuth("store", SECRET);

    protected static HttpClient client;
    protected static String pdRestAddr;

    protected static String basicAuth(String name, String pwd) {
        String credential = name + ":" + pwd;
        return "Basic " + Base64.getEncoder()
                                .encodeToString(credential.getBytes(StandardCharsets.UTF_8));
    }

    @BeforeClass
    public static void init() {
        client = HttpClient.newHttpClient();
        pdRestAddr = "http://127.0.0.1:8620";
    }

    @After
    public void teardown() {
        // pass
    }

}
