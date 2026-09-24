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

package org.apache.hugegraph.space.register;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RegisterLoader implements InvocationHandler {

    private IServiceRegister register;

    public void bind(IServiceRegister register) {
        this.register = register;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Object.class.equals(method.getDeclaringClass())) {
            try {
                Object var4 = method.invoke(this, args);
                return var4;
            } catch (Throwable var8) {
                return null;
            } finally {
            }
        } else {
            return this.run(method, args);
        }
    }

    public Object run(Method method, Object[] args) throws IllegalAccessException,
                                                           IllegalArgumentException,
                                                           InvocationTargetException {
        return method.invoke(this.register, args);
    }
}
