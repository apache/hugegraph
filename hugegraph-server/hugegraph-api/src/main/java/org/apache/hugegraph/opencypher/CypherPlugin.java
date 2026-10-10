/*
 * Copyright (c) 2018-2019 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description of the modifications:
 * <p>
 * 1) Changed the package name.
 * <pre>
 * org.opencypher.gremlin.server.jsr223
 * -->
 * org.apache.hugegraph.opencypher
 * </pre>
 * </p>
 */

package org.apache.hugegraph.opencypher;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.opencypher.gremlin.traversal.CustomFunctions;

public class CypherPlugin implements GremlinPlugin {

    private static final ImportCustomizer IMPORTS =
            DefaultImportCustomizer.build()
                                   .addClassImports(CypherGremlinPredicates.class)
                                   .addMethodImports(
                                           getDeclaredPublicMethods(CypherGremlinPredicates.class))
                                   .addClassImports(CustomFunctions.class)
                                   .addMethodImports(
                                           getDeclaredPublicMethods(CustomFunctions.class))
                                   .create();

    private static List<Method> getDeclaredPublicMethods(Class<?> klass) {
        Method[] declaredMethods = klass.getDeclaredMethods();
        return Stream.of(declaredMethods)
                     .filter(method -> Modifier.isPublic(method.getModifiers()) &&
                                       Modifier.isStatic(method.getModifiers()))
                     .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "cypher.extra";
    }

    public static GremlinPlugin instance() {
        return new CypherPlugin();
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<Customizer[]> getCustomizers(String scriptEngineName) {
        return Optional.of(new Customizer[]{IMPORTS});
    }
}
