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

package org.apache.hugegraph.unit.api.gremlin;

import java.lang.reflect.Method;

import org.apache.hugegraph.api.gremlin.GremlinQueryAPI;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class GremlinQueryAPITest extends BaseUnitTest {

    private static boolean matchBadRequest(String exClass) throws Exception {
        Method m = GremlinQueryAPI.class.getDeclaredMethod(
                "matchBadRequestException", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, exClass);
    }

    @Test
    public void testMatchBadRequestExceptionWithTinkerpop() throws Exception {
        Assert.assertTrue(matchBadRequest(
                "org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException"));
    }

    @Test
    public void testMatchBadRequestExceptionWithAuthExceptions() throws Exception {
        Assert.assertFalse(matchBadRequest(
                "org.apache.tinkerpop.gremlin.server.auth.AuthenticationException"));
        Assert.assertFalse(matchBadRequest(
                "org.apache.tinkerpop.gremlin.server.authz.AuthorizationException"));
    }

    @Test
    public void testMatchBadRequestExceptionWithHugegraph() throws Exception {
        Assert.assertTrue(matchBadRequest("org.apache.hugegraph.exception.NotFoundException"));
        Assert.assertTrue(matchBadRequest("java.lang.IllegalArgumentException"));
        Assert.assertTrue(matchBadRequest("groovy.lang.MissingPropertyException"));
    }

    @Test
    public void testMatchBadRequestExceptionWithOther() throws Exception {
        Assert.assertFalse(matchBadRequest(null));
        Assert.assertFalse(matchBadRequest("java.lang.NullPointerException"));
        Assert.assertFalse(matchBadRequest("java.io.IOException"));
    }
}
