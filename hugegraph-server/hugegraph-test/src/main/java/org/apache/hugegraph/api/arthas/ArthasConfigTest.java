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

package org.apache.hugegraph.api.arthas;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.filter.AuthenticationFilter.Authorizer;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;

public class ArthasConfigTest {

    private final List<LogEvent> events = new ArrayList<>();
    private Logger logger;
    private Level previousLevel;
    private Appender appender;

    @Before
    public void captureWarnings() {
        this.appender = Mockito.mock(Appender.class);
        Mockito.when(this.appender.getName()).thenReturn("ArthasConfigTestAppender");
        Mockito.when(this.appender.isStarted()).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            LogEvent event = invocation.getArgument(0);
            this.events.add(event.toImmutable());
            return null;
        }).when(this.appender).append(Mockito.any(LogEvent.class));
        this.logger = (Logger) LogManager.getLogger(ArthasAPI.class);
        this.previousLevel = this.logger.getLevel();
        this.logger.setLevel(Level.WARN);
        this.logger.addAppender(this.appender);
    }

    @After
    public void restoreLogger() {
        this.logger.removeAppender(this.appender);
        this.logger.setLevel(this.previousLevel);
    }

    @BeforeClass
    public static void registerOptions() {
        OptionSpace.register("server", ServerOptions.instance());
    }

    @Test
    public void testDefaults() {
        Map<String, String> config = ArthasAPI.arthasConfig(new HugeConfig(Collections.emptyMap()));
        assertOptions(config, "8562", "8561", "");
        Assert.assertEquals("127.0.0.1", config.get("arthas.ip"));
        Assert.assertTrue(this.events.isEmpty());
    }

    @Test
    public void testDistributionKeys() {
        Map<String, Object> properties = properties("telnet_port", "http_port", "disabled_commands");
        properties.put("arthas.ip", "127.0.0.1");
        Map<String, String> config = ArthasAPI.arthasConfig(new HugeConfig(properties));
        assertOptions(config, "18562", "18561", "jad,stop");
        Assert.assertEquals("127.0.0.1", config.get("arthas.ip"));
        Assert.assertTrue(this.events.isEmpty());
    }

    @Test
    public void testLegacyKeys() {
        Map<String, Object> properties = properties("telnetPort", "httpPort", "disabledCommands");
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "18562", "18561", "jad,stop");
        Assert.assertTrue(this.events.isEmpty());
    }

    @Test
    public void testCanonicalKeysTakePrecedence() {
        Map<String, Object> properties = properties("telnet_port", "http_port", "disabled_commands");
        properties.put("arthas.telnetPort", "28562");
        properties.put("arthas.httpPort", "28561");
        properties.put("arthas.disabledCommands", "stop");
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "18562", "18561", "jad,stop");
        Assert.assertEquals(3, this.events.size());
        assertConflictWarning(0, "telnet_port", "telnetPort");
        assertConflictWarning(1, "http_port", "httpPort");
        assertConflictWarning(2, "disabled_commands", "disabledCommands");
    }

    @Test
    public void testExplicitCanonicalDefaultsTakePrecedence() {
        Map<String, Object> properties = properties("telnetPort", "httpPort", "disabledCommands");
        properties.put("arthas.telnet_port", "8562");
        properties.put("arthas.http_port", "8561");
        properties.put("arthas.disabled_commands", "");
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "8562", "8561", "");
        Assert.assertEquals(3, this.events.size());
        assertConflictWarning(2, "disabled_commands", "disabledCommands");
    }

    @Test
    public void testMatchingCanonicalAndLegacyKeysDoNotWarn() {
        Map<String, Object> properties = properties("telnet_port", "http_port", "disabled_commands");
        properties.putAll(properties("telnetPort", "httpPort", "disabledCommands"));
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "18562", "18561", "jad,stop");
        Assert.assertTrue(this.events.isEmpty());
    }

    @Test
    public void testCanonicalAndLegacyPortsAndIpRejectEmptyValues() {
        for (String key : new String[]{"telnet_port", "telnetPort", "http_port", "httpPort", "ip"}) {
            Assert.assertThrows(ConfigException.class,
                                () -> new HugeConfig(Collections.singletonMap("arthas." + key, "")));
        }
    }

    @Test
    public void testCanonicalAndLegacyDisabledCommandsCanBeEmpty() {
        for (String key : new String[]{"disabled_commands", "disabledCommands"}) {
            Map<String, String> config = ArthasAPI.arthasConfig(
                    new HugeConfig(Collections.singletonMap("arthas." + key, "")));
            assertOptions(config, "8562", "8561", "");
        }
    }

    @Test
    public void testExplicitBindAddresses() {
        for (String ip : new String[]{"127.0.0.1", "127.0.0.2", "::1", "localhost",
                                      "0.0.0.0", "::", "192.0.2.1", "2001:db8::1", "fe80::1%1"}) {
            Map<String, String> config = ArthasAPI.arthasConfig(
                    new HugeConfig(Collections.singletonMap("arthas.ip", ip)));
            Assert.assertEquals("localhost".equals(ip) ? "127.0.0.1" : ip, config.get("arthas.ip"));
        }
    }

    @Test
    public void testInvalidBindAddressesAreRejected() {
        for (String ip : new String[]{"2001:db8::xyz", "2001:db8:::1", "[::1", "0.0.0.0:3658"}) {
            HugeConfig config = new HugeConfig(Collections.singletonMap("arthas.ip", ip));
            Assert.assertThrows(IllegalArgumentException.class, () -> ArthasAPI.arthasConfig(config));
        }
    }

    @Test
    public void testNoAuthCannotActivateWithAnonymousAdminRole() {
        GraphManager manager = Mockito.mock(GraphManager.class);
        SecurityContext context = authorizer(manager, User.ANONYMOUS);
        Assert.assertTrue(context.isUserInRole("admin"));
        Throwable failure = Assert.assertThrows(ForbiddenException.class,
                                               () -> new ArthasAPI().startArthas(manager, context));
        Assert.assertTrue(failure.getMessage().contains("bin/arthas.sh"));
    }

    @Test
    public void testAuthenticatedNonAdminCannotActivate() {
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.requireAuthentication()).thenReturn(true);
        User user = new User("graph_reader", RolePermission.role("DEFAULT", "hugegraph", HugePermission.READ));
        SecurityContext context = authorizer(manager, user);
        Assert.assertThrows(ForbiddenException.class,
                            () -> new ArthasAPI().startArthas(manager, context));
    }

    @Test
    public void testAuthenticatedAdminIsAllowed() {
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.requireAuthentication()).thenReturn(true);
        ArthasAPI.checkAccess(manager, authorizer(manager, User.ADMIN));
    }

    @Test
    public void testAnonymousOrMissingPrincipalIsRejectedEvenInAuthMode() {
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.requireAuthentication()).thenReturn(true);
        Assert.assertThrows(ForbiddenException.class,
                            () -> new ArthasAPI().startArthas(manager, authorizer(manager, User.ANONYMOUS)));
        Assert.assertThrows(ForbiddenException.class,
                            () -> new ArthasAPI().startArthas(manager, Mockito.mock(SecurityContext.class)));
    }

    private static SecurityContext authorizer(GraphManager manager, User user) {
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
        return new Authorizer(manager, user, uri);
    }

    private void assertConflictWarning(int index, String canonical, String legacy) {
        LogEvent event = this.events.get(index);
        Assert.assertEquals(Level.WARN, event.getLevel());
        Assert.assertEquals(String.format("Conflicting Arthas options 'arthas.%s' and 'arthas.%s': " +
                                         "'arthas.%s' takes precedence", canonical, legacy, canonical),
                            event.getMessage().getFormattedMessage());
    }

    private static Map<String, Object> properties(String telnet, String http, String commands) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("arthas." + telnet, "18562");
        properties.put("arthas." + http, "18561");
        properties.put("arthas." + commands, "jad,stop");
        return properties;
    }

    private static void assertOptions(Map<String, String> config, String telnet, String http,
                                      String commands) {
        Assert.assertEquals(4, config.size());
        Assert.assertEquals(telnet, config.get("arthas.telnetPort"));
        Assert.assertEquals(http, config.get("arthas.httpPort"));
        Assert.assertEquals(commands, config.get("arthas.disabledCommands"));
    }
}
