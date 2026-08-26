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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.hugegraph.backend.store.hstore.HstoreSessions;
import org.junit.Assert;
import org.junit.Test;

public class HstoreSessionsTest {

    @Test
    public void testOrderedScanDoesNotAddAbstractSubclassRequirement()
            throws Exception {
        Method method = HstoreSessions.Session.class.getDeclaredMethod(
                "scanOrdered", String.class, byte[].class, byte[].class,
                byte[].class, byte[].class, int.class, byte[].class,
                long.class);

        Assert.assertFalse(Modifier.isAbstract(method.getModifiers()));
    }
}
