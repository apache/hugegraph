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

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class GroovyScriptEngineCompatibilityTest extends BaseUnitTest {

    @Test
    public void testGroovyJsr223EngineCanCompileAndEvaluate()
           throws ScriptException {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName(
                "groovy");

        Assert.assertNotNull(engine);
        Assert.assertEquals("org.codehaus.groovy.jsr223." +
                            "GroovyScriptEngineImpl",
                            engine.getClass().getName());
        Assert.assertTrue(engine instanceof Compilable);

        CompiledScript script = ((Compilable) engine).compile(
                "def add = { a, b -> a + b }; add(2, 3)");

        Assert.assertEquals(5, script.eval());
    }
}
