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

package org.apache.hugegraph.backend.serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.config.HugeConfig;

public class SerializerFactory {

    private static final Map<String, Class<? extends AbstractSerializer>> serializers;

    static {
        serializers = new ConcurrentHashMap<>();
    }

    public static AbstractSerializer serializer(HugeConfig config, String name) {
        name = name.toLowerCase();
        switch (name) {
            case "binary":
                return new BinarySerializer(config);
            case "binaryscatter":
                return new BinaryScatterSerializer(config);
            case "text":
                return new TextSerializer(config);
            default:
        }

        Class<? extends AbstractSerializer> clazz = serializers.get(name);
        if (clazz == null) {
            throw new BackendException("Not exists serializer: '%s'", name);
        }

        assert AbstractSerializer.class.isAssignableFrom(clazz);
        try {
            return clazz.getConstructor(HugeConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new BackendException(e);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void register(String name, String classPath) {
        ClassLoader classLoader = SerializerFactory.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException("Invalid class: '%s'", e, classPath);
        }

        // Check subclass
        if (!AbstractSerializer.class.isAssignableFrom(clazz)) {
            throw new BackendException("Class is not a subclass of class " +
                                       "AbstractSerializer: '%s'", classPath);
        }

        // Register atomically: identical re-registration is a no-op
        Class<?> registered = serializers.putIfAbsent(name, (Class) clazz);
        if (registered != null && !registered.equals(clazz)) {
            throw new BackendException("Exists serializer: %s(Class '%s')",
                                       name, registered.getName());
        }
    }
}
