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

package org.apache.hugegraph.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.exception.NotSupportException;
import org.slf4j.Logger;

public class Reflection {

    private static final Logger LOG = Log.logger(Reflection.class);

    private static final Class<?> REFLECTION_CLAZZ;
    private static final Method REGISTER_FILEDS_TO_FILTER_METHOD;
    private static final Method REGISTER_METHODS_TO_FILTER_METHOD;

    static {
        Method registerFieldsToFilterMethodTemp = null;
        Method registerMethodsToFilterMethodTemp = null;
        Class<?> reflectionClazzTemp = null;
        try {
            reflectionClazzTemp = Class.forName("jdk.internal.reflect.Reflection");

            registerFieldsToFilterMethodTemp =
                    reflectionClazzTemp.getMethod("registerFieldsToFilter",
                                                  Class.class, String[].class);

            registerMethodsToFilterMethodTemp =
                    reflectionClazzTemp.getMethod("registerMethodsToFilter",
                                                  Class.class, String[].class);
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find jdk.internal.reflect.Reflection class, " +
                      "please ensure you are using Java 11", e);
        } catch (NoSuchMethodException e) {
            LOG.error("Can't find reflection filter methods", e);
        }

        REFLECTION_CLAZZ = reflectionClazzTemp;
        REGISTER_FILEDS_TO_FILTER_METHOD = registerFieldsToFilterMethodTemp;
        REGISTER_METHODS_TO_FILTER_METHOD = registerMethodsToFilterMethodTemp;
    }

    public static void registerFieldsToFilter(Class<?> containingClass, String... fieldNames) {
        if (REGISTER_FILEDS_TO_FILTER_METHOD == null) {
            throw new NotSupportException("Reflection.registerFieldsToFilter() - " +
                                          "requires Java 11 or higher");
        }

        try {
            REGISTER_FILEDS_TO_FILTER_METHOD.setAccessible(true);
            REGISTER_FILEDS_TO_FILTER_METHOD.invoke(REFLECTION_CLAZZ, containingClass, fieldNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException("Failed to register class '%s' fields to filter: %s",
                                    containingClass, Arrays.toString(fieldNames));
        }
    }

    public static void registerMethodsToFilter(Class<?> containingClass, String... methodNames) {
        if (REGISTER_METHODS_TO_FILTER_METHOD == null) {
            throw new NotSupportException("Reflection.registerMethodsToFilter() - " +
                                          "requires Java 11 or higher");
        }

        try {
            REGISTER_METHODS_TO_FILTER_METHOD.setAccessible(true);
            REGISTER_METHODS_TO_FILTER_METHOD.invoke(REFLECTION_CLAZZ, containingClass,
                                                     methodNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException("Failed to register class '%s' methods to filter: %s",
                                    containingClass, Arrays.toString(methodNames));
        }
    }

    public static Class<?> loadClass(String clazz) {
        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {
            throw new HugeException(e.getMessage(), e);
        }
    }
}
