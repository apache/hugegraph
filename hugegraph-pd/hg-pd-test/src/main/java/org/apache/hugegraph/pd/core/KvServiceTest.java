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

package org.apache.hugegraph.pd.core;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.kv.V;
import org.junit.Assert;
import org.junit.Test;

public class KvServiceTest extends PDCoreTestBase {

    @Test
    public void testKv() {
        try {
            PDConfig pdConfig = getPdConfig();
            KvService service = new KvService(pdConfig);
            String key = "kvTest";
            String kvTest = service.get(key);
            Assert.assertEquals(kvTest, "");
            service.put(key, "kvTestValue");
            kvTest = service.get(key);
            Assert.assertEquals(kvTest, "kvTestValue");
            service.scanWithPrefix(key);
            service.delete(key);
            service.put(key, "kvTestValue");
            service.deleteWithPrefix(key);
            service.put(key, "kvTestValue", 1000L);
            service.keepAlive(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMember() {
        try {
            PDConfig pdConfig = getPdConfig();
            KvService service = new KvService(pdConfig);
            service.setPdConfig(pdConfig);
            PDConfig config = service.getPdConfig();
            // TODO
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * The TTL cleanup deletes the key an expired marker names, and the marker
     */
    @Test
    public void testTtlCleanupDeletesTheExpiredKey() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/a", "old", 1L);
        byte[] storeKey = service.getStoreKey("ttl-test/a");
        String marker = marker(service, "ttl-test/a");
        Thread.sleep(20L);
        service.clearTTLData();
        Assert.assertNull(service.getMeta().getOne(storeKey));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey(marker)));
    }

    /**
     * A renewal with a longer TTL between the cleanup's marker scan and its read: the cleanup
     * reads the new value, which does not carry the old marker's expiry, so it keeps the value
     * and drops only the old marker
     */
    @Test
    public void testTtlCleanupKeepsAValueRenewedBeforeItsRead() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/b", "old", 1L);
        String marker = marker(service, "ttl-test/b");
        service.put("ttl-test/b", "new", 100_000L);
        String renewed = marker(service, "ttl-test/b");
        byte[] read = service.getMeta().getOne(service.getStoreKey("ttl-test/b"));
        // The cleanup scans only markers whose time has passed
        Thread.sleep(20L);
        service.removeExpired("ttl-test/b", marker, read);
        Assert.assertEquals("new", service.get("ttl-test/b"));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey(marker)));
        Assert.assertNotNull(service.getMeta().getOne(service.getStoreKey(renewed)));
    }

    /**
     * A renewal of the identical string between the cleanup's read and its delete: the key no
     * longer holds the bytes the cleanup read (the expiry differs), so it keeps the value and
     * drops only the old marker
     */
    @Test
    public void testTtlCleanupKeepsAValueRenewedAfterItsRead() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/c", "same", 1L);
        String marker = marker(service, "ttl-test/c");
        byte[] scanned = service.getMeta().getOne(service.getStoreKey("ttl-test/c"));
        service.put("ttl-test/c", "same", 100_000L);
        Thread.sleep(20L);
        service.removeExpired("ttl-test/c", marker, scanned);
        Assert.assertEquals("same", service.get("ttl-test/c"));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey(marker)));
    }

    /**
     * A keepAlive between the cleanup's read and its delete renews the lease with a later
     * expiry; the cleanup keeps it
     */
    @Test
    public void testTtlCleanupKeepsALeaseAKeepAliveRenewedAfterItsRead() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/d", "lease", 1L);
        String marker = marker(service, "ttl-test/d");
        byte[] scanned = service.getMeta().getOne(service.getStoreKey("ttl-test/d"));
        long expireAt = V.parseFrom(scanned).getTtl();
        // The marker is due, and the renewed lease carries a later expiry
        Thread.sleep(20L);
        Assert.assertTrue(service.keepAlive("ttl-test/d"));
        service.removeExpired("ttl-test/d", marker, scanned);
        byte[] kept = service.getMeta().getOne(service.getStoreKey("ttl-test/d"));
        Assert.assertNotNull(kept);
        Assert.assertEquals("lease", V.parseFrom(kept).getValue());
        Assert.assertTrue(V.parseFrom(kept).getTtl() > expireAt);
    }

    /**
     * A plain put without a TTL over a TTL value keeps the marker; the cleanup sees the value
     * is not the one the marker names and drops the marker alone
     */
    @Test
    public void testTtlCleanupKeepsAPlainPutOverATtlValue() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/e", "old", 1L);
        String marker = marker(service, "ttl-test/e");
        service.put("ttl-test/e", "plain");
        Thread.sleep(20L);
        service.clearTTLData();
        Assert.assertEquals("plain", service.get("ttl-test/e"));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey(marker)));
    }

    /**
     * A marker whose value is already gone (a read removes an expired value when it finds
     * one) is dropped by the cleanup
     */
    @Test
    public void testTtlCleanupDropsTheMarkerOfARemovedValue() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/f", "old", 1L);
        String marker = marker(service, "ttl-test/f");
        Thread.sleep(20L);
        Assert.assertNull(service.get("ttl-test/f"));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey("ttl-test/f")));
        service.clearTTLData();
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey(marker)));
    }

    /**
     * The cleanup scans markers by bytes, so a marker whose time has more digits than now can
     * sort below now: a TTL of 10^13 ms is selected today. Its value has not expired, so the
     * cleanup keeps the value and the marker
     */
    @Test
    public void testTtlCleanupKeepsAValueThatHasNotExpired() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("ttl-test/g", "kept", 10_000_000_000_000L);
        String marker = marker(service, "ttl-test/g");
        service.clearTTLData();
        Assert.assertEquals("kept", service.get("ttl-test/g"));
        Assert.assertNotNull(service.getMeta().getOne(service.getStoreKey(marker)));
    }

    /**
     * An ordinary key can look like a marker, and one with a time that is not a number sorts
     * inside the scanned range: the cleanup skips it, keeps it, and still deletes the expired
     * key after it
     */
    @Test
    public void testTtlCleanupSkipsAnOrdinaryKeyInTheMarkerRange() throws Exception {
        KvService service = new KvService(getPdConfig());
        service.put("T@123abc@foo", "ordinary");
        service.put("ttl-test/h", "old", 1L);
        String marker = marker(service, "ttl-test/h");
        Thread.sleep(20L);
        service.clearTTLData();
        Assert.assertEquals("ordinary", service.get("T@123abc@foo"));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey("ttl-test/h")));
        Assert.assertNull(service.getMeta().getOne(service.getStoreKey(marker)));
    }

    /**
     * The marker key of a TTL value, without the K@ prefix
     */
    private static String marker(KvService service, String key) throws Exception {
        byte[] stored = service.getMeta().getOne(service.getStoreKey(key));
        return "T@" + V.parseFrom(stored).getTtl() + "@" + key;
    }
}
