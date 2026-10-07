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

package org.apache.hugegraph.plugin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.util.ReflectionUtil;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import com.google.common.reflect.ClassPath;

public class HugeGraphGremlinPlugin extends AbstractGremlinPlugin {

    private static final String PACKAGE = "org.apache.hugegraph.type.define";
    private static final String NAME = "HugeGraph";

    private static final HugeGraphGremlinPlugin INSTANCE;
    private static final ImportCustomizer IMPORTS;

    static {
        INSTANCE = new HugeGraphGremlinPlugin();

        Iterator<ClassPath.ClassInfo> classInfos;
        try {
            classInfos = ReflectionUtil.classes(PACKAGE);
        } catch (IOException e) {
            throw new HugeException("Failed to scan classes under package %s",
                                    e, PACKAGE);
        }

        @SuppressWarnings("rawtypes")
        Set<Class> classes = new HashSet<>();
        classInfos.forEachRemaining(classInfo -> classes.add(classInfo.load()));
        // Add entrance class: graph = HugeFactory.open("hugegraph.properties")
        classes.add(HugeFactory.class);

        IMPORTS = DefaultImportCustomizer.build()
                                         .addClassImports(classes)
                                         .create();
    }

    public HugeGraphGremlinPlugin() {
        super(NAME, IMPORTS);
    }

    public static HugeGraphGremlinPlugin instance() {
        return INSTANCE;
    }
}
