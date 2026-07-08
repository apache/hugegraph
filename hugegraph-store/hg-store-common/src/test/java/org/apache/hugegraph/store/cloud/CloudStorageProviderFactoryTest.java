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

package org.apache.hugegraph.store.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CloudStorageProviderFactoryTest {

    private CloudStorageConfig config;
    private CloudStorageProvider mockProvider;

    @Before
    public void setUp() {
        config = new CloudStorageConfig();
        mockProvider = mock(CloudStorageProvider.class);
        when(mockProvider.providerName()).thenReturn("s3");
    }

    @After
    public void tearDown() {
        // Reset to prevent test interference
        CloudStorageProviderFactory.reset();
    }

    /**
     * Test initialize with disabled cloud storage returns null
     */
    @Test
    public void testInitializeDisabled() {
        config.setEnabled(false);

        CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

        assertNull(result);
        assertNull(CloudStorageProviderFactory.getActiveProvider());
    }

    /**
     * Test that setActiveProviderForTest allows setting a provider
     */
    @Test
    public void testInitializeWithMockProvider() {
        config.setEnabled(true);
        config.setProvider("s3");
        config.getProviderProperties().put("bucket", "test-bucket");

        // Inject the mock provider for testing (bypass SPI discovery)
        CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

        // Verify it's set
        assertEquals(mockProvider, CloudStorageProviderFactory.getActiveProvider());
    }

    /**
     * Test that multiple setActiveProviderForTest calls work
     */
    @Test
    public void testMultipleSetActiveProvider() {
        CloudStorageProvider oldProvider = mock(CloudStorageProvider.class);
        when(oldProvider.providerName()).thenReturn("s3");

        // Set first provider
        CloudStorageProviderFactory.setActiveProviderForTest(oldProvider);
        assertEquals(oldProvider, CloudStorageProviderFactory.getActiveProvider());

        // Switch to new provider
        CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);
        assertEquals(mockProvider, CloudStorageProviderFactory.getActiveProvider());
    }

    /**
     * Test that initialize with unknown provider (no SPI) throws IllegalArgumentException
     */
    @Test
    public void testInitializeUnknownProviderNoSpi() {
        config.setEnabled(true);
        config.setProvider("unknown-provider");

        try {
            CloudStorageProviderFactory.initialize(config);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertNotNull(e.getMessage());
            // Expected - no providers are available via SPI in tests
        }
    }

    /**
     * Test shutdown closes the active provider
     */
    @Test
    public void testShutdown() throws IOException {
        // Inject mock provider without calling initialize
        CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

        assertNotNull(CloudStorageProviderFactory.getActiveProvider());

        CloudStorageProviderFactory.shutdown();

        assertNull(CloudStorageProviderFactory.getActiveProvider());
        verify(mockProvider, times(1)).close();
    }

    /**
     * Test reset clears active provider without closing it
     */
    @Test
    public void testReset() throws IOException {
        CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

        assertNotNull(CloudStorageProviderFactory.getActiveProvider());

        CloudStorageProviderFactory.reset();

        assertNull(CloudStorageProviderFactory.getActiveProvider());
        // close should NOT be called
        verify(mockProvider, times(0)).close();
    }

    /**
     * Test shutdown handles IOException gracefully
     */
    @Test
    public void testShutdownWithException() throws IOException {
        // Inject mock provider without calling initialize
        doThrow(new IOException("Mock close error")).when(mockProvider).close();
        CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

        // Should not throw exception, error is logged
        CloudStorageProviderFactory.shutdown();

        assertNull(CloudStorageProviderFactory.getActiveProvider());
    }

    /**
     * Test setActiveProviderForTest allows injection
     */
    @Test
    public void testSetActiveProviderForTest() {
        assertNull(CloudStorageProviderFactory.getActiveProvider());

        CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

        assertEquals(mockProvider, CloudStorageProviderFactory.getActiveProvider());

        CloudStorageProviderFactory.setActiveProviderForTest(null);

        assertNull(CloudStorageProviderFactory.getActiveProvider());
    }

    /**
     * Test that initialize is idempotent (calling multiple times should work)
     */
    @Test
    public void testInitializeIdempotent() {
        // Test the disabled case which doesn't require SPI providers
        config.setEnabled(false);

        CloudStorageProvider result1 = CloudStorageProviderFactory.initialize(config);
        CloudStorageProvider result2 = CloudStorageProviderFactory.initialize(config);

        assertNull(result1);
        assertNull(result2);
    }
}





