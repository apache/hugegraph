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

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IpAuthHandlerTest {

    @Before
    public void setUp() {
        // Must reset BEFORE each test — earlier suite classes (e.g. ConfigServiceTest)
        // initialize RaftEngine which creates the IpAuthHandler singleton with their
        // own peer IPs. Without this reset, our getInstance() calls return the stale
        // singleton and ignore the allowlist passed by the test.
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
    }

    @After
    public void tearDown() {
        // Must reset AFTER each test — prevents our test singleton from leaking
        // into later suite classes that also depend on IpAuthHandler state.
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
    }

    private boolean isIpAllowed(IpAuthHandler handler, String ip) {
        return Whitebox.invoke(IpAuthHandler.class,
                               new Class[]{String.class},
                               "isIpAllowed", handler, ip);
    }

    @Test
    public void testHostnameResolvesToIp() throws Exception {
        // "localhost" should resolve to one or more IPs via InetAddress.getAllByName()
        // This verifies the core fix: hostname allowlists match numeric remote addresses
        // Using dynamic resolution avoids hardcoding "127.0.0.1" which may not be
        // returned on IPv6-only or custom resolver environments
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("localhost"));
        InetAddress[] addresses = InetAddress.getAllByName("localhost");
        // All resolved addresses should be allowed — resolveAll() adds every address
        // returned by getAllByName() so none should be blocked
        Assert.assertTrue("Expected at least one resolved address",
                          addresses.length > 0);
        for (InetAddress address : addresses) {
            Assert.assertTrue(
                    "Expected " + address.getHostAddress() + " to be allowed",
                    isIpAllowed(handler, address.getHostAddress()));
        }
    }

    @Test
    public void testUnresolvableHostnameDoesNotCrash() {
        // Should log a warning and skip — no exception thrown during construction
        // Uses .invalid TLD which is RFC-2606 reserved and guaranteed to never resolve
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("nonexistent.invalid"));
        // Handler was still created successfully despite bad hostname
        Assert.assertNotNull(handler);
        // Unresolvable entry is skipped so no IPs should be allowed
        Assert.assertFalse(isIpAllowed(handler, "127.0.0.1"));
        Assert.assertFalse(isIpAllowed(handler, "192.168.0.1"));
    }

    @Test
    public void testRefreshUpdatesResolvedIps() {
        // Start with 127.0.0.1
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("127.0.0.1"));
        Assert.assertTrue(isIpAllowed(handler, "127.0.0.1"));

        // Refresh with a different IP — verifies refresh() swaps the set correctly
        Set<String> newIps = new HashSet<>();
        newIps.add("192.168.0.1");
        handler.refresh(newIps);

        // Old IP should no longer be allowed
        Assert.assertFalse(isIpAllowed(handler, "127.0.0.1"));
        // New IP should now be allowed
        Assert.assertTrue(isIpAllowed(handler, "192.168.0.1"));
    }

    @Test
    public void testEmptyAllowlistAllowsAll() {
        // Empty allowlist = no restriction configured = allow all connections
        // This is intentional fallback behavior and must be explicitly tested
        // because it is a security-relevant boundary
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.emptySet());
        Assert.assertTrue(isIpAllowed(handler, "1.2.3.4"));
        Assert.assertTrue(isIpAllowed(handler, "192.168.99.99"));
    }

    @Test
    public void testGetInstanceReturnsSingletonIgnoresNewAllowlist() {
        // First call creates the singleton with 127.0.0.1
        IpAuthHandler first = IpAuthHandler.getInstance(
                Collections.singleton("127.0.0.1"));
        // Second call with a different set must return the same instance
        // and must NOT reinitialize or override the existing allowlist
        IpAuthHandler second = IpAuthHandler.getInstance(
                Collections.singleton("192.168.0.1"));
        Assert.assertSame(first, second);
        // Original allowlist still in effect
        Assert.assertTrue(isIpAllowed(second, "127.0.0.1"));
        // New set was ignored — 192.168.0.1 should not be allowed
        Assert.assertFalse(isIpAllowed(second, "192.168.0.1"));
    }
}
