/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.analyzer;


import org.apache.hugegraph.exception.HugeException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AnalyzerFactory {

    private static Map<String, Class<? extends Analyzer>> analyzers;

    static {
        analyzers = new ConcurrentHashMap<>();
    }

    public static Analyzer analyzer(String name, String mode) {
        name = name.toLowerCase();
        switch (name) {
            case "word":
                return new WordAnalyzer(mode);
            case "ansj":
                return new AnsjAnalyzer(mode);
            case "hanlp":
                return new HanLPAnalyzer(mode);
            case "smartcn":
                return new SmartCNAnalyzer(mode);
            case "jieba":
                return new JiebaAnalyzer(mode);
            case "jcseg":
                return new JcsegAnalyzer(mode);
            case "mmseg4j":
                return new MMSeg4JAnalyzer(mode);
            case "ikanalyzer":
                return new IKAnalyzer(mode);
            default:
                return customizedAnalyzer(name, mode);
        }
    }

    private static Analyzer customizedAnalyzer(String name, String mode) {
        Class<? extends Analyzer> clazz = analyzers.get(name);
        if (clazz == null) {
            throw new HugeException("Not exists analyzer: %s", name);
        }

        assert Analyzer.class.isAssignableFrom(clazz);
        try {
            return clazz.getConstructor(String.class).newInstance(mode);
        } catch (Exception e) {
            throw new HugeException(
                      "Failed to construct analyzer '%s' with mode '%s'",
                      e, name, mode);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = AnalyzerFactory.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new HugeException("Load class path '%s' failed",
                                    e, classPath);
        }

        // Check subclass
        if (!Analyzer.class.isAssignableFrom(clazz)) {
            throw new HugeException("Class '%s' is not a subclass of " +
                                    "class Analyzer", classPath);
        }

        // Check exists
        if (analyzers.containsKey(name)) {
            throw new HugeException("Exists analyzer: %s(%s)",
                                    name, analyzers.get(name).getName());
        }

        // Register class
        analyzers.put(name, (Class) clazz);
    }
}
