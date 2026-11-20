/*
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

package org.apache.hugegraph.util;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hugegraph.iterator.ExtendableIterator;
import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

public final class ReflectionUtil {

    public static boolean isSimpleType(Class<?> type) {
        return type.isPrimitive() ||
               type.equals(String.class) ||
               type.equals(Boolean.class) ||
               type.equals(Character.class) ||
               NumericUtil.isNumber(type);
    }

    public static List<Method> getMethodsAnnotatedWith(
                               Class<?> type,
                               Class<? extends Annotation> annotation,
                               boolean withSuperClass) {
        final List<Method> methods = new LinkedList<>();
        Class<?> klass = type;
        do {
            for (Method method : klass.getDeclaredMethods()) {
                if (method.isAnnotationPresent(annotation)) {
                    methods.add(method);
                }
            }
            klass = klass.getSuperclass();
        } while (klass != null && klass != Object.class && withSuperClass); // null for interfaces
        return methods;
    }

    public static List<CtMethod> getMethodsAnnotatedWith(
                                 CtClass type,
                                 Class<? extends Annotation> annotation,
                                 boolean withSuperClass)
                                 throws NotFoundException {
        final List<CtMethod> methods = new LinkedList<>();

        CtClass klass = type;
        do {
            for (CtMethod method : klass.getDeclaredMethods()) {
                if (method.hasAnnotation(annotation)) {
                    methods.add(method);
                }
            }
            klass = klass.getSuperclass();
        } while (klass != null && withSuperClass);
        return methods;
    }

    public static Iterator<ClassInfo> classes(String... packages)
                                              throws IOException {
        ClassPath path = ClassPath.from(ReflectionUtil.class.getClassLoader());
        ExtendableIterator<ClassInfo> results = new ExtendableIterator<>();
        for (String p : packages) {
            results.extend(path.getTopLevelClassesRecursive(p).iterator());
        }
        return results;
    }

    public static List<String> superClasses(String clazz)
                                            throws NotFoundException {
        CtClass klass = ClassPool.getDefault().get(clazz);
        CtClass parent = klass.getSuperclass();

        List<String> results = new LinkedList<>();
        while (parent != null) {
            results.add(parent.getName());
            parent = parent.getSuperclass();
        }
        return Lists.reverse(results);
    }

    public static List<String> nestedClasses(String clazz)
                                             throws NotFoundException {
        CtClass klass = ClassPool.getDefault().get(clazz);

        List<String> results = new LinkedList<>();
        for (CtClass nested : klass.getNestedClasses()) {
            results.add(nested.getName());
        }
        return results;
    }

    public static String packageName(String clazz) {
        int offset = clazz.lastIndexOf(".");
        if (offset > 0) {
            return clazz.substring(0, offset);
        }
        return "";
    }
}
