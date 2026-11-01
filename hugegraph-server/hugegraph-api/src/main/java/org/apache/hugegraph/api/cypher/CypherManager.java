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

package org.apache.hugegraph.api.cypher;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.net.URL;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.hugegraph.util.E;

@ThreadSafe
public final class CypherManager {

    private final String configurationFile;
    private YAMLConfiguration configuration;

    public static CypherManager configOf(String configurationFile) {
        E.checkArgument(configurationFile != null && !configurationFile.isEmpty(),
                        "The configurationFile parameter can't be null or empty");
        return new CypherManager(configurationFile);
    }

    private CypherManager(String configurationFile) {
        this.configurationFile = configurationFile;
    }

    public CypherClient getAnonymousClient() {
        return new CypherClient(this::cloneConfig);
    }

    public CypherClient getClient(String userName, String password) {
        E.checkArgument(userName != null && !userName.isEmpty(),
                        "The userName parameter can't be null or empty");
        E.checkArgument(password != null && !password.isEmpty(),
                        "The password parameter can't be null or empty");

        // TODO: Need to cache the client and make it hold the connection.
        return new CypherClient(userName, password, this::cloneConfig);
    }

    public CypherClient getClient(String token) {
        E.checkArgument(token != null && !token.isEmpty(),
                        "The token parameter can't be null or empty");

        // TODO: Need to cache the client and make it hold the connection.
        return new CypherClient(token, this::cloneConfig);
    }

    private Configuration cloneConfig() {
        if (this.configuration == null) {
            this.configuration = loadYaml(this.configurationFile);
        }
        return (Configuration) this.configuration.clone();
    }

    private static YAMLConfiguration loadYaml(String configurationFile) {
        File yamlFile = getConfigFile(configurationFile);
        YAMLConfiguration yaml;
        try {
            Reader reader = new FileReader(yamlFile);
            yaml = new YAMLConfiguration();
            yaml.read(reader);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to load configuration file," +
                                                     " the file at '%s'.", configurationFile), e);
        }
        return yaml;
    }

    private static File getConfigFile(String configurationFile) {
        File systemFile = new File(configurationFile);
        if (!systemFile.exists()) {
            ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            URL resource = currentClassLoader.getResource(configurationFile);
            assert resource != null;
            File resourceFile = new File(resource.getFile());
            if (!resourceFile.exists()) {
                throw new IllegalArgumentException(String.format("Configuration file at '%s' does" +
                                                                 " not exist", configurationFile));
            }
            return resourceFile;
        }
        return systemFile;
    }
}
