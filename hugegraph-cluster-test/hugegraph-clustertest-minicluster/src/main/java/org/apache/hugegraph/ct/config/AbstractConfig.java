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

package org.apache.hugegraph.ct.config;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.ct.base.HGTestLogger;
import org.slf4j.Logger;

public abstract class AbstractConfig {

    protected static final Logger LOG = HGTestLogger.CONFIG_LOG;
    protected String config;
    protected Map<String, String> properties = new HashMap<>();
    protected String fileName;

    protected void readTemplate(Path filePath) {
        try {
            this.config = new String(Files.readAllBytes(filePath));
        } catch (IOException e) {
            LOG.error("failed to get file", e);
        }
    }

    protected void updateConfigs() {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String placeholder = "$" + entry.getKey() + "$";
            this.config = this.config.replace(placeholder, entry.getValue());
        }
    }

    public void writeConfig(String filePath) {
        updateConfigs();
        Path destPath = Paths.get(filePath + File.separator + this.fileName);
        try {
            if (Files.notExists(destPath.getParent())) {
                Files.createDirectories(destPath.getParent());
            }
        } catch (IOException e) {
            LOG.error("Failed to create dir", e);
        }
        try (FileWriter writer = new FileWriter(String.valueOf(destPath))) {
            writer.write(this.config);
        } catch (IOException e) {
            LOG.error("Failed to write in file", e);
        }
    }

    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    protected void setProperty(String propertyName, String value) {
        properties.put(propertyName, value);
    }
}
