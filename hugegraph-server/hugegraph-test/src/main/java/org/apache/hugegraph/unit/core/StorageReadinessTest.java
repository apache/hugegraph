/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.api.filter.AuthenticationFilter;
import org.apache.hugegraph.api.filter.PathFilter;
import org.apache.hugegraph.api.profile.StorageReadiness;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.UriInfo;

public class StorageReadinessTest {

    @Before
    @After
    public void reset() {
        StorageReadiness.resetCache();
    }

    private static Map<String, Object> result(boolean ready, String reason) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("ready", ready);
        map.put("reason", reason);
        map.put("active_stores", 3);
        return map;
    }

    @Test
    public void testReadyBodyAndCacheReuse() {
        AtomicInteger probes = new AtomicInteger();
        StorageReadiness.Probe probe = t -> {
            probes.incrementAndGet();
            return result(true, "ok");
        };
        Map<String, Object> first = StorageReadiness.check(probe, 1000L, 60_000L);
        Map<String, Object> second = StorageReadiness.check(probe, 1000L, 60_000L);
        Assert.assertTrue(StorageReadiness.isReady(first));
        Assert.assertEquals("hstore", first.get("storage"));
        Assert.assertEquals(false, first.get("cached"));
        Assert.assertEquals(true, second.get("cached"));
        Assert.assertEquals(1, probes.get());
    }

    @Test
    public void testZeroTtlProbesEveryTime() {
        AtomicInteger probes = new AtomicInteger();
        StorageReadiness.Probe probe = t -> {
            probes.incrementAndGet();
            return result(false, "no active store registered in pd");
        };
        StorageReadiness.check(probe, 1000L, 0L);
        Map<String, Object> body = StorageReadiness.check(probe, 1000L, 0L);
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals(2, probes.get());
        Assert.assertEquals("no active store registered in pd", body.get("reason"));
    }

    @Test
    public void testProbeFailureIsNotReadyWithReason() {
        StorageReadiness.Probe probe = t -> {
            throw new IllegalStateException("The 'hugegraph' store of hstore has not been opened");
        };
        Map<String, Object> body = StorageReadiness.check(probe, 1000L, 0L);
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals("probe failed: IllegalStateException", body.get("reason"));
        // the endpoint is unauthenticated: no raw message in the body
        Assert.assertFalse(body.toString().contains("has not been opened"));
    }

    @Test
    public void testTimeoutIsPassedToTheProbe() {
        StorageReadiness.Probe probe = t -> result(true, "budget " + t);
        Map<String, Object> body = StorageReadiness.check(probe, 750L, 0L);
        Assert.assertEquals("budget 750", body.get("reason"));
    }

    @Test
    public void testCachedCopyIsIsolated() {
        StorageReadiness.Probe probe = t -> result(true, "ok");
        Map<String, Object> first = StorageReadiness.check(probe, 1000L, 60_000L);
        first.put("ready", false);
        Map<String, Object> second = StorageReadiness.check(probe, 1000L, 60_000L);
        Assert.assertTrue(StorageReadiness.isReady(second));
    }

    /**
     * A Kubernetes httpGet probe carries no credential and no graphspace, so
     * the endpoint must pass both the graphspace path rewrite and the auth
     * filter, exactly like /versions.
     */
    @Test
    public void testReadinessBypassesPathAndAuthFilters() {
        Assert.assertTrue(PathFilter.isWhiteAPI("readiness"));
        Assert.assertTrue(PathFilter.isWhiteAPI("versions"));
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getPath()).thenReturn("readiness");
        ContainerRequestContext ctx = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(ctx.getUriInfo()).thenReturn(uri);
        Assert.assertTrue(AuthenticationFilter.isWhiteAPI(ctx));
        Mockito.when(uri.getPath()).thenReturn("readiness/");
        Assert.assertFalse(AuthenticationFilter.isWhiteAPI(ctx));
    }
}
