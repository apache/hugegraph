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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.managers.AuthMetaManager;
import org.apache.hugegraph.meta.managers.SpaceMetaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * {@link GraphManager#initAdminUserIfNeeded} is the only bootstrap the
 * built-in admin gets when init-store is disabled, and init-store's
 * fail-closed check assumes it works. Only the already-exists case is benign;
 * every other failure must abort startup rather than leave the server up
 * without a usable administrator.
 */
public class GraphManagerAdminInitTest {

    private static final String CLUSTER = "admin-init-test";

    private Map<String, String> store;
    private MetaDriver driver;
    private Object originalAuthManager;
    private Object originalSpaceManager;
    private GraphManager manager;

    @Before
    public void setup() throws Exception {
        this.store = new HashMap<>();
        this.driver = Mockito.mock(MetaDriver.class);
        Mockito.when(this.driver.get(Mockito.anyString())).thenAnswer(
                i -> this.store.get(i.<String>getArgument(0)));
        Mockito.doAnswer(i -> this.store.put(i.getArgument(0),
                                             i.getArgument(1)))
               .when(this.driver)
               .put(Mockito.anyString(), Mockito.anyString());

        this.originalAuthManager = swapMetaManagerField(
                "authMetaManager", new AuthMetaManager(this.driver, CLUSTER));
        this.originalSpaceManager = swapMetaManagerField(
                "spaceMetaManager", new SpaceMetaManager(this.driver, CLUSTER));
        this.manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                new EventHub("admin-init-test"));
    }

    @After
    public void teardown() throws Exception {
        try {
            if (this.manager != null) {
                this.manager.close();
            }
        } finally {
            swapMetaManagerField("authMetaManager", this.originalAuthManager);
            swapMetaManagerField("spaceMetaManager",
                                 this.originalSpaceManager);
        }
    }

    @Test
    public void testCreatesAdminOnFreshMetadata() throws Exception {
        this.manager.initAdminUserIfNeeded("s3cret");

        HugeUser admin = MetaManager.instance().findUser("admin");
        Assert.assertNotNull("the admin must be created", admin);
    }

    /**
     * Every restart after the first sees the admin already recorded and
     * surfaces the already-exists signal, which must neither fail startup
     * nor rotate the existing password. (True concurrency is weaker than
     * this: createUser is get-then-put without compare-and-set, so a tight
     * race can overwrite rather than throw — benign only because every
     * server writes the admin derived from the same configured password.)
     */
    @Test
    public void testExistingAdminIsKeptWithoutFailing() throws Exception {
        this.manager.initAdminUserIfNeeded("first");
        HugeUser created = MetaManager.instance().findUser("admin");

        this.manager.initAdminUserIfNeeded("second");

        HugeUser kept = MetaManager.instance().findUser("admin");
        Assert.assertNotNull(kept);
        Assert.assertEquals("an existing admin's password must not rotate",
                            created.password(), kept.password());
    }

    /**
     * A PD write, permission or validation failure used to be logged and
     * swallowed, so the server started with no usable administrator. It has
     * to propagate instead: the failure is not the already-exists case,
     * proven by the admin still being absent.
     */
    @Test
    public void testNonDuplicateCreationFailurePropagates() {
        RuntimeException refused = new RuntimeException("pd write refused");
        Mockito.doThrow(refused).when(this.driver)
               .put(Mockito.anyString(), Mockito.anyString());

        Assert.assertThrows(HugeException.class, () -> {
            this.manager.initAdminUserIfNeeded("s3cret");
        }, e -> Assert.assertEquals(refused, e.getCause()));
    }

    @Test
    public void testAdminBootstrapOnlyUsesLocalStandardAuthenticator() {
        Class<?>[] types = {HugeAuthenticator.class, String.class};

        Assert.assertFalse(Whitebox.invokeStatic(
                GraphManager.class, types, "shouldBootstrapAdmin",
                (Object) null, ""));
        Assert.assertTrue(Whitebox.invokeStatic(
                GraphManager.class, types, "shouldBootstrapAdmin",
                new StandardAuthenticator(), ""));
        Assert.assertFalse(Whitebox.invokeStatic(
                GraphManager.class, types, "shouldBootstrapAdmin",
                new StandardAuthenticator(), "pd:8520"));
        Assert.assertFalse(Whitebox.invokeStatic(
                GraphManager.class, types, "shouldBootstrapAdmin",
                Mockito.mock(HugeAuthenticator.class), ""));
    }

    private static Object swapMetaManagerField(String field,
                                               Object replacement)
                                               throws Exception {
        Field f = MetaManager.class.getDeclaredField(field);
        f.setAccessible(true);
        Object previous = f.get(MetaManager.instance());
        f.set(MetaManager.instance(), replacement);
        return previous;
    }
}
