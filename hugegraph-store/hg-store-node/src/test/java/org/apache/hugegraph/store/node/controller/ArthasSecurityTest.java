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

package org.apache.hugegraph.store.node.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class ArthasSecurityTest {

    private MockMvc mvc;

    @BeforeEach
    public void setup() {
        // No Store engine or Arthas configuration is needed to reject activation.
        this.mvc = MockMvcBuilders.standaloneSetup(new PartitionAPI()).build();
    }

    @Test
    public void testRemoteActivationIsForbidden() throws Exception {
        assertForbidden(requestFrom("192.0.2.1"));
    }

    @Test
    public void testLoopbackActivationIsForbidden() throws Exception {
        assertForbidden(requestFrom("127.0.0.1"));
        assertForbidden(requestFrom("::1"));
    }

    @Test
    public void testSameHostProxyCannotActivateArthas() throws Exception {
        assertForbidden(requestFrom("127.0.0.1")
                        .header("Forwarded", "for=192.0.2.1;host=store.example")
                        .header("X-Forwarded-For", "192.0.2.1"));
    }

    @Test
    public void testSpoofedLoopbackHeadersCannotActivateArthas() throws Exception {
        assertForbidden(requestFrom("192.0.2.1")
                        .header("Forwarded", "for=127.0.0.1")
                        .header("X-Forwarded-For", "127.0.0.1"));
    }

    @Test
    public void testAuthorizationHeaderDoesNotEnableStoreActivation() throws Exception {
        assertForbidden(requestFrom("127.0.0.1")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer admin"));
    }

    private static MockHttpServletRequestBuilder requestFrom(String address) {
        return get("/v1/arthasstart").with(request -> {
            request.setRemoteAddr(address);
            return request;
        });
    }

    private void assertForbidden(MockHttpServletRequestBuilder request) throws Exception {
        this.mvc.perform(request)
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message").value(
                        "HTTP Arthas activation is disabled; run bin/arthas.sh on the Store host"));
    }
}
