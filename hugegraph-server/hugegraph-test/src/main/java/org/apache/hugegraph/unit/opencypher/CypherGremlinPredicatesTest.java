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

package org.apache.hugegraph.unit.opencypher;

import java.util.Collections;
import java.util.List;

import javax.script.Bindings;
import javax.script.ScriptException;

import org.apache.hugegraph.opencypher.CypherGremlinPredicates;
import org.apache.hugegraph.opencypher.CypherPlugin;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.Test;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.StatementOption;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.translator.Translator;

public class CypherGremlinPredicatesTest extends BaseUnitTest {

    @Test
    public void testRegexUsesWholeStringMatching() {
        CypherGremlinPredicates predicates = new CypherGremlinPredicates();
        Assert.assertTrue(predicates.regexMatch("m.*o").test("marko"));
        Assert.assertFalse(predicates.regexMatch("ark").test("marko"));
        Assert.assertTrue(predicates.regexMatch("marko").test("marko"));
        Assert.assertFalse(predicates.regexMatch("marko").negate().test("marko"));
        Assert.assertTrue(predicates.regexMatch("marko").negate().test("peter"));
    }

    @Test
    public void testNodeRelationshipAndStringPredicates() {
        CypherGremlinPredicates predicates = new CypherGremlinPredicates();
        ReferenceVertex source = new ReferenceVertex("source", "person");
        ReferenceVertex target = new ReferenceVertex("target", "person");
        ReferenceEdge edge = new ReferenceEdge("edge", "knows", source, target);
        Assert.assertTrue(predicates.isNode().test(source));
        Assert.assertFalse(predicates.isNode().test(edge));
        Assert.assertTrue(predicates.isRelationship().test(edge));
        Assert.assertFalse(predicates.isRelationship().test(source));
        Assert.assertTrue(predicates.isString().test("marko"));
        Assert.assertFalse(predicates.isString().test(42));
        Assert.assertFalse(predicates.isString().test(source));
    }

    @Test
    public void testPublicFactoriesThroughPluginImports() throws ScriptException {
        GremlinGroovyScriptEngine engine = pluginEngine();
        Bindings bindings = engine.createBindings();
        ReferenceVertex source = new ReferenceVertex("source", "person");
        ReferenceVertex target = new ReferenceVertex("target", "person");
        bindings.put("vertex", source);
        bindings.put("edge", new ReferenceEdge("edge", "knows", source, target));
        Assert.assertEquals(true, engine.eval("cypherRegex('mar.*').test('marko')", bindings));
        Assert.assertEquals(false, engine.eval("cypherRegex('ark').test('marko')", bindings));
        Assert.assertEquals(true, engine.eval("cypherIsNode().test(vertex)", bindings));
        Assert.assertEquals(false, engine.eval("cypherIsNode().test(edge)", bindings));
        Assert.assertEquals(true, engine.eval("cypherIsRelationship().test(edge)", bindings));
        Assert.assertEquals(false, engine.eval("cypherIsRelationship().test(vertex)", bindings));
        Assert.assertEquals(true, engine.eval("cypherIsString().test('marko')", bindings));
        Assert.assertEquals(false, engine.eval("cypherIsString().test(42)", bindings));
    }

    @Test
    public void testExplainGremlinReplaysUsingPluginImports() throws ScriptException {
        CypherAst ast = CypherAst.parse("EXPLAIN UNWIND ['marko', 'peter'] AS name " +
                                       "WITH name WHERE name =~ 'mar.*' RETURN name");
        Assert.assertTrue(ast.getOptions().contains(StatementOption.EXPLAIN));
        Translator<String, GroovyPredicate> translator = Translator.builder()
                                                                   .gremlinGroovy()
                                                                   .build("gremlin+cfog_server_extensions+" +
                                                                          "inline_parameters");
        String gremlin = ast.buildTranslation(translator);
        Assert.assertTrue(gremlin.contains("cypherRegex("));
        GremlinGroovyScriptEngine engine = pluginEngine();
        Bindings bindings = engine.createBindings();
        bindings.put("g", EmptyGraph.instance().traversal());
        GraphTraversal<?, ?> traversal = (GraphTraversal<?, ?>) engine.eval(gremlin, bindings);
        List<?> result = traversal.toList();
        Assert.assertEquals(Collections.singletonList(Collections.singletonMap("name", "marko")),
                            result);
    }

    private static GremlinGroovyScriptEngine pluginEngine() {
        Customizer[] customizers = CypherPlugin.instance().getCustomizers("gremlin-groovy").get();
        return new GremlinGroovyScriptEngine(customizers);
    }

}
