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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hugegraph.space.register.registerImpl.PdRegister;

import com.google.common.base.Strings;

public class RegisterPlugin {

    private static final RegisterPlugin INSTANCE = new RegisterPlugin();
    private final Map<Class<?>, IServiceRegister> plugins = new ConcurrentHashMap();

    private RegisterPlugin() {
    }

    public static RegisterPlugin getInstance() {
        return INSTANCE;
    }

    public String loadPlugin(String jarPath, String appName) throws IOException {
        JarFile jarFile = new JarFile(new File(jarPath));
        URL url = new URL("file:" + jarPath);
        URL[] urls = new URL[]{url};
        ClassLoader loader = new URLClassLoader(urls);
        Enumeration<JarEntry> entry = jarFile.entries();

        while (entry.hasMoreElements()) {
            JarEntry jar = entry.nextElement();
            String name = jar.getName();
            if (name.endsWith(".class")) {
                try {
                    int offset = name.lastIndexOf(".class");
                    name = name.substring(0, offset);
                    name = name.replace('/', '.');
                    Class<?> c = loader.loadClass(name);
                    for (Class<?> inter : c.getInterfaces()) {
                        if (inter.equals(IServiceRegister.class)) {
                            IServiceRegister o = (IServiceRegister) c.newInstance();
                            return this.loadPlugin(o, appName);
                        }
                    }
                } catch (Throwable e) {
                    System.out.println(e);
                }
            }
        }

        return "";
    }

    public String loadPlugin(IServiceRegister instance, String appName) {
        IServiceRegister register =
                (IServiceRegister) (new Invoker()).getInstance(IServiceRegister.class, instance);

        try {
            String serviceId = register.init(appName);
            if (!Strings.isNullOrEmpty(serviceId)) {
                String key = register.getClass().getName();
                this.plugins.put(register.getClass(), register);
                return key;
            }
        } catch (Throwable var6) {
        }

        return "";
    }

    public String loadDefaultPlugin(String appName) {
        PdRegister instance = PdRegister.getInstance();
        return this.loadPlugin(instance, appName);
    }

    public void unloadPlugin(String key, String serviceId) {
        IServiceRegister register = this.plugins.get(key);
        if (null != register) {
            register.unregisterAll();
        }

    }
}
