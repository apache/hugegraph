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

package org.apache.hugegraph.unit.api.auth;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.api.auth.LoginAPI;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import sun.misc.Unsafe;

public class LoginAPITest extends BaseUnitTest {

    private static final String LOGGER_NAME = LoginAPI.class.getName();
    private static final String SECRET = "secret-bearer-token";

    private LoggerContext loggerContext;
    private org.apache.logging.log4j.core.config.Configuration configuration;
    private TestAppender appender;

    @Before
    public void setup() {
        this.appender = new TestAppender();
        this.appender.start();
        this.loggerContext = (LoggerContext) LogManager.getContext(false);
        this.configuration = this.loggerContext.getConfiguration();
        LoggerConfig logger = new LoggerConfig(LOGGER_NAME, Level.DEBUG, false);
        logger.addAppender(this.appender, Level.DEBUG, null);
        this.configuration.addLogger(LOGGER_NAME, logger);
        this.loggerContext.updateLoggers();
    }

    @After
    public void teardown() {
        this.configuration.removeLogger(LOGGER_NAME);
        this.loggerContext.updateLoggers();
        this.appender.stop();
    }

    @Test
    public void testLogoutDoesNotLogBearerToken() {
        AuthManager authManager = Mockito.mock(AuthManager.class);
        GraphManager manager = managerWithAuthManager(authManager);

        new LoginAPI().logout(manager, "Bearer " + SECRET);

        assertSecretNotLogged();
    }

    @Test
    public void testVerifyDoesNotLogBearerToken() {
        AuthManager authManager = Mockito.mock(AuthManager.class);
        GraphManager manager = managerWithAuthManager(authManager);
        Mockito.when(authManager.validateUser(SECRET))
               .thenThrow(new IllegalArgumentException("invalid token"));

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> new LoginAPI().verifyToken(manager,
                                                             "Bearer " + SECRET));

        assertSecretNotLogged();
    }

    private void assertSecretNotLogged() {
        for (LogEvent event : this.appender.events()) {
            Assert.assertFalse(event.getMessage().getFormattedMessage()
                                    .contains(SECRET));
        }
    }

    private static GraphManager managerWithAuthManager(AuthManager authManager) {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            GraphManager manager = (GraphManager) unsafe.allocateInstance(
                                   GraphManager.class);
            HugeAuthenticator authenticator = Mockito.mock(HugeAuthenticator.class);
            Mockito.when(authenticator.authManager()).thenReturn(authManager);
            Whitebox.setInternalState(manager, "authenticator", authenticator);
            return manager;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static class TestAppender extends AbstractAppender {

        private final List<LogEvent> events;

        TestAppender() {
            super("LoginAPITestAppender", (Filter) null,
                  (Layout<?>) null, true, Property.EMPTY_ARRAY);
            this.events = new ArrayList<>();
        }

        @Override
        public void append(LogEvent event) {
            this.events.add(event.toImmutable());
        }

        List<LogEvent> events() {
            return this.events;
        }
    }
}
