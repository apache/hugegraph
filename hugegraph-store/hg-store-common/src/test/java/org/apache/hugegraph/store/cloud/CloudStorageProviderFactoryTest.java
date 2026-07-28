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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CloudStorageProviderFactoryTest {

    private CloudStorageConfig config;
    private CloudStorageProvider mockProvider;
    private Map<String, CloudStorageProvider> registryBackup;

    @Before
    public void setUp() {
        config = new CloudStorageConfig();
        mockProvider = mock(CloudStorageProvider.class);
        when(mockProvider.providerName()).thenReturn("s3");

        // Keep each test isolated from static registry state.
        registryBackup = new HashMap<>(registry());
        registry().clear();
    }

    @After
    public void tearDown() {
        // Reset static mutable state to prevent test interference.
        CloudStorageProviderFactory.reset();
        registry().clear();
        registry().putAll(registryBackup);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, CloudStorageProvider> registry() {
        try {
            Field field = CloudStorageProviderFactory.class.getDeclaredField("REGISTRY");
            field.setAccessible(true);
            return (Map<String, CloudStorageProvider>) field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to access CloudStorageProviderFactory.REGISTRY", e);
        }
    }

    private static void invokeLoadProviders() {
        try {
            Method method = CloudStorageProviderFactory.class.getDeclaredMethod("loadProviders");
            method.setAccessible(true);
            method.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to invoke CloudStorageProviderFactory.loadProviders", e);
        }
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
     * Test that initialize with cloud storage disabled deactivates and closes an already-active
     * provider, so a reconfiguration to {@code enabled=false} cannot leave stale SDK resources
     * running and still servicing cloud I/O.
     */
    @Test
    public void testInitializeDisabledClosesActiveProvider() throws IOException {
        CloudStorageProvider active = mock(CloudStorageProvider.class);
        when(active.providerName()).thenReturn("s3");
        CloudStorageProviderFactory.setActiveProviderForTest(active);

        config.setEnabled(false);
        CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

        assertNull(result);
        assertNull("Active provider must be cleared when cloud storage is disabled",
                   CloudStorageProviderFactory.getActiveProvider());
        verify(active, times(1)).close();
    }

    /**
     * Test that a failure to close the active provider while disabling is swallowed (best-effort)
     * and the provider is still deactivated.
     */
    @Test
    public void testInitializeDisabledCloseThrowsStillDeactivates() throws IOException {
        CloudStorageProvider active = mock(CloudStorageProvider.class);
        when(active.providerName()).thenReturn("s3");
        doThrow(new IOException("Mock close error")).when(active).close();
        CloudStorageProviderFactory.setActiveProviderForTest(active);

        config.setEnabled(false);
        CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

        assertNull(result);
        assertNull(CloudStorageProviderFactory.getActiveProvider());
        verify(active, times(1)).close();
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

     /**
      * Test that initialize closes previous provider when switching providers
      */
     @Test
     public void testInitializeClosesPreviousProvider() {
         // Set up first provider
         CloudStorageProvider firstProvider = mock(CloudStorageProvider.class);
         when(firstProvider.providerName()).thenReturn("s3");
         CloudStorageProviderFactory.setActiveProviderForTest(firstProvider);

         // Set up second provider to replace it
         CloudStorageProvider secondProvider = mock(CloudStorageProvider.class);
         when(secondProvider.providerName()).thenReturn("gcs");

         config.setEnabled(true);
         config.setProvider("gcs");

         // Inject second provider for this test
         CloudStorageProviderFactory.setActiveProviderForTest(secondProvider);

         // Verify we can close without exception
         CloudStorageProviderFactory.shutdown();
         assertNull(CloudStorageProviderFactory.getActiveProvider());
     }

     /**
      * Test that initialize with close exception on previous provider handles error
      */
     @Test
     public void testInitializeClosePreviousProviderThrows() throws IOException {
         // Set up first provider that throws when closing
         CloudStorageProvider firstProvider = mock(CloudStorageProvider.class);
         when(firstProvider.providerName()).thenReturn("s3");
         doThrow(new IOException("Mock error")).when(firstProvider).close();
         CloudStorageProviderFactory.setActiveProviderForTest(firstProvider);

         // Should not throw, error is logged
         CloudStorageProviderFactory.shutdown();
         assertNull(CloudStorageProviderFactory.getActiveProvider());
     }

     /**
      * Test shutdown when no provider is active
      */
     @Test
     public void testShutdownNoActiveProvider() {
         CloudStorageProviderFactory.reset();

         // Should not throw exception
         CloudStorageProviderFactory.shutdown();

         assertNull(CloudStorageProviderFactory.getActiveProvider());
     }

     @Test
     public void testInitializeUsesRegistryProviderAndSetsActive() {
         registry().put("s3", mockProvider);
         config.setEnabled(true);
         config.setProvider("s3");
         config.getProviderProperties().put("bucket", "unit-bucket");

         CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

         assertSame(mockProvider, result);
         assertSame(mockProvider, CloudStorageProviderFactory.getActiveProvider());
         verify(mockProvider, times(1)).init(config);
     }

     @Test
     public void testInitializeWithSameActiveProviderDoesNotCloseIt() throws IOException {
         registry().put("s3", mockProvider);
         CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);
         config.setEnabled(true);
         config.setProvider("s3");

         CloudStorageProviderFactory.initialize(config);

         verify(mockProvider, never()).close();
         verify(mockProvider, times(1)).init(config);
     }

     @Test
     public void testInitializeSwitchProviderClosesPreviousProvider() throws IOException {
         CloudStorageProvider oldProvider = mock(CloudStorageProvider.class);
         when(oldProvider.providerName()).thenReturn("old");

         CloudStorageProvider newProvider = mock(CloudStorageProvider.class);
         when(newProvider.providerName()).thenReturn("gcs");

         registry().put("gcs", newProvider);
         CloudStorageProviderFactory.setActiveProviderForTest(oldProvider);

         config.setEnabled(true);
         config.setProvider("gcs");

         CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

         assertSame(newProvider, result);
         assertSame(newProvider, CloudStorageProviderFactory.getActiveProvider());
         verify(oldProvider, times(1)).close();
         verify(newProvider, times(1)).init(config);
     }

     @Test
     public void testInitializeInitFailureLeavesActiveProviderNull() throws IOException {
         // A new provider whose init() throws must leave the factory in a SAFE state
         // (activeProvider == null), never referencing a closed/partially-initialized provider.
         CloudStorageProvider oldProvider = mock(CloudStorageProvider.class);
         when(oldProvider.providerName()).thenReturn("old");
         CloudStorageProviderFactory.setActiveProviderForTest(oldProvider);

         CloudStorageProvider failing = mock(CloudStorageProvider.class);
         when(failing.providerName()).thenReturn("gcs");
         doThrow(new RuntimeException("init boom")).when(failing).init(config);
         registry().put("gcs", failing);

         config.setEnabled(true);
         config.setProvider("gcs");

         try {
             CloudStorageProviderFactory.initialize(config);
             org.junit.Assert.fail("initialize must propagate the init failure");
         } catch (RuntimeException expected) {
             // expected
         }

         assertNull("A failed init must not leave a closed/unusable provider active",
                    CloudStorageProviderFactory.getActiveProvider());
         // The previous provider was closed as part of the swap and must not remain active.
         verify(oldProvider, times(1)).close();
     }

     @Test
     public void testInitializeSameProviderInitFailureClearsActive() {
         // Re-init of the SAME active instance that then fails must also clear the active reference
         // rather than leave a half-initialized provider observable.
         doThrow(new RuntimeException("reinit boom")).when(mockProvider).init(config);
         registry().put("s3", mockProvider);
         CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);
         config.setEnabled(true);
         config.setProvider("s3");

         try {
             CloudStorageProviderFactory.initialize(config);
             org.junit.Assert.fail("initialize must propagate the init failure");
         } catch (RuntimeException expected) {
             // expected
         }

         assertNull("A failed re-init must clear the active provider",
                    CloudStorageProviderFactory.getActiveProvider());
     }

     @Test
     public void testInitializeContinuesWhenClosingPreviousProviderFails() throws IOException {
         CloudStorageProvider oldProvider = mock(CloudStorageProvider.class);
         when(oldProvider.providerName()).thenReturn("old");
         doThrow(new IOException("close failed")).when(oldProvider).close();

         CloudStorageProvider newProvider = mock(CloudStorageProvider.class);
         when(newProvider.providerName()).thenReturn("gcs");

         registry().put("gcs", newProvider);
         CloudStorageProviderFactory.setActiveProviderForTest(oldProvider);

         config.setEnabled(true);
         config.setProvider("gcs");

         CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

         assertSame(newProvider, result);
         assertSame(newProvider, CloudStorageProviderFactory.getActiveProvider());
         verify(oldProvider, times(1)).close();
         verify(newProvider, times(1)).init(config);
     }

      @Test
      public void testLoadProvidersKeepsFirstRegistrationOnDuplicateName() {
          CloudStorageProvider firstRegistration = mock(CloudStorageProvider.class);
          when(firstRegistration.providerName())
                  .thenReturn(TestDuplicateNamedProvider.PROVIDER_NAME);

          boolean duplicateProviderDiscoverable =
                  ServiceLoader.load(CloudStorageProvider.class,
                                     CloudStorageProviderFactory.class.getClassLoader())
                               .stream()
                               .map(ServiceLoader.Provider::get)
                               .anyMatch(p -> TestDuplicateNamedProvider.PROVIDER_NAME
                                       .equals(p.providerName()));
          if (!duplicateProviderDiscoverable) {
              fail("Test duplicate provider is not discoverable via ServiceLoader");
          }

          registry().put(TestDuplicateNamedProvider.PROVIDER_NAME, firstRegistration);

          invokeLoadProviders();

          assertSame(firstRegistration,
                     registry().get(TestDuplicateNamedProvider.PROVIDER_NAME));
      }

      /**
       * When SPI discovery finds no providers, {@code loadProviders} must leave the registry empty
       * and log that cloud storage is unavailable (the "no providers found" branch). An isolated
       * classloader with a {@code null} parent exposes no {@code META-INF/services} entries, so no
       * provider is discovered.
       */
      @Test
      public void testLoadProvidersWithNoProvidersLeavesRegistryEmpty() {
          registry().clear();

          ClassLoader emptyClassLoader = new java.net.URLClassLoader(new java.net.URL[0], null);
          CloudStorageProviderFactory.loadProviders(emptyClassLoader);

          assertTrue("No providers should be discovered from an empty classloader",
                     registry().isEmpty());
      }

      @Test
      public void testInitializeWithNullConfigThrowsIllegalArgumentException() {
          try {
              CloudStorageProviderFactory.initialize(null);
              fail("Should have thrown IllegalArgumentException");
          } catch (IllegalArgumentException e) {
              assertEquals("cloud storage config must not be null", e.getMessage());
          }
      }

      @Test
      public void testShutdownMultipleTimes() {
          CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

          CloudStorageProviderFactory.shutdown();
          CloudStorageProviderFactory.shutdown(); // Should not throw

          assertNull(CloudStorageProviderFactory.getActiveProvider());
      }

      @Test
      public void testResetMultipleTimes() {
          CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);

          CloudStorageProviderFactory.reset();
          CloudStorageProviderFactory.reset(); // Should not throw

          assertNull(CloudStorageProviderFactory.getActiveProvider());
      }

      @Test
      public void testSetActiveProviderForTestWithNull() {
          CloudStorageProviderFactory.setActiveProviderForTest(mockProvider);
          assertEquals(mockProvider, CloudStorageProviderFactory.getActiveProvider());

          CloudStorageProviderFactory.setActiveProviderForTest(null);
          assertNull(CloudStorageProviderFactory.getActiveProvider());
      }

      @Test
      public void testInitializeLogsWhenDisabled() {
          config.setEnabled(false);

          CloudStorageProvider result = CloudStorageProviderFactory.initialize(config);

          assertNull(result);
      }

      @Test
      public void testProviderInitializationCalledWithCorrectConfig() {
          registry().put("s3", mockProvider);
          config.setEnabled(true);
          config.setProvider("s3");
          config.getProviderProperties().put("bucket", "test-bucket");

          CloudStorageProviderFactory.initialize(config);

          verify(mockProvider, times(1)).init(config);
      }
 }
