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

package org.apache.hugegraph.pd.raft;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class IpAuthHandlerTest {

    @After
    public void tearDown() {
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
    }

    private boolean isIpAllowed(IpAuthHandler handler, String ip) {
        return Whitebox.invoke(IpAuthHandler.class,
                               new Class[]{String.class},
                               "isIpAllowed", handler, ip);
    }

    @Test
    public void testHostnameResolvesToIp() {
        // "localhost" should resolve to "127.0.0.1"
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("localhost"));
        Assert.assertTrue(isIpAllowed(handler, "127.0.0.1"));
    }

    @Test
    public void testUnresolvableHostnameDoesNotCrash() {
        // Should log a warning and skip — no exception thrown
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("nonexistent.invalid.hostname"));
        // unresolvable entry is skipped so 127.0.0.1 should not be allowed
        Assert.assertFalse(isIpAllowed(handler, "127.0.0.1"));
    }

    @Test
    public void testRefreshUpdatesResolvedIps() {
        // Start with 127.0.0.1
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("127.0.0.1"));
        Assert.assertTrue(isIpAllowed(handler, "127.0.0.1"));

        // Refresh with a different IP
        Set<String> newIps = new HashSet<>();
        newIps.add("192.168.0.1");
        handler.refresh(newIps);

        Assert.assertFalse(isIpAllowed(handler, "127.0.0.1"));
        Assert.assertTrue(isIpAllowed(handler, "192.168.0.1"));
    }

    @Test
    public void testEmptyAllowlistAllowsAll() {
        // Empty allowlist = no restriction = allow all
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.emptySet());
        Assert.assertTrue(isIpAllowed(handler, "1.2.3.4"));
        Assert.assertTrue(isIpAllowed(handler, "192.168.99.99"));
    }

    @Test
    public void testGetInstanceReturnsNullBeforeInit() {
        // After tearDown resets singleton, no-arg getInstance returns null
        Assert.assertNull(IpAuthHandler.getInstance());
    }
}
