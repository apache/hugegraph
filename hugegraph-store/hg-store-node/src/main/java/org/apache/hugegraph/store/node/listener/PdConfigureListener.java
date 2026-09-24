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

package org.apache.hugegraph.store.node.listener;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertiesPropertySource;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Charsets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PdConfigureListener implements
                                 ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    private static final String CONFIG_PREFIX = "S:";
    private static final String CONFIG_FIX_PREFIX = "S:FS";
    private static final String TIMESTAMP_KEY = "S:Timestamp";
    private static final String PD_CONFIG_FILE_NAME = "application-pd.yml";
    private final String workDir = System.getProperty("user.dir");
    private final String fileSeparator = System.getProperty("file.separator");
    private final String configFilePath =
            workDir + fileSeparator + "conf" + fileSeparator + PD_CONFIG_FILE_NAME;
    private final String restartShellPath = workDir + fileSeparator + "bin" + fileSeparator
                                            + "restart-hugegraph-store.sh";
    private ConfigurableApplicationContext context;
    private File pdConfFile;
    // private String restartPath = workDir + fileSeparator + "lib" + fileSeparator;

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        MutablePropertySources sources = event.getEnvironment().getPropertySources();
        String pdAddress = event.getEnvironment().getProperty("pdserver.address");
        pdConfFile = new File(configFilePath);

        KvClient client = new KvClient(PDConfig.of(pdAddress));
        try {
            ScanPrefixResponse response = client.scanPrefix(CONFIG_PREFIX);
            Map<String, String> kvsMap = response.getKvsMap();
            String pdConfig = kvsMap.get(CONFIG_FIX_PREFIX);
            if (!StringUtils.isEmpty(pdConfig)) {
                updatePdConfig(sources, client, pdConfig);
            } else {
                // send local application-pd.yml to pd
                if (pdConfFile.exists()) {
                    String commons = FileUtils.readFileToString(pdConfFile, Charsets.UTF_8);
                    log.info("send local application-pd.yml to pd....{}", commons);
                    client.put(CONFIG_FIX_PREFIX, commons);
                }
            }
            log.info("Start listening for keys :" + TIMESTAMP_KEY);
            client.listen(TIMESTAMP_KEY, (Consumer<WatchResponse>) o -> {
                log.info("receive message to restart :" + o);
                try {
                    // Prioritize updating the latest configuration file to avoid old files being
                    // loaded first when modifying parameters like ports.
                    ScanPrefixResponse responseNew = client.scanPrefix(CONFIG_PREFIX);
                    Map<String, String> kvsMapNew = responseNew.getKvsMap();
                    String config = kvsMapNew.get(CONFIG_FIX_PREFIX);
                    updatePdConfig(sources, client, config);
                    restart();
                } catch (Exception e) {
                    log.error("start listener with error:", e);
                }
            });

        } catch (Exception e) {
            log.error("start listener with error:", e);
        }

    }

    private void updatePdConfig(MutablePropertySources sources, KvClient client,
                                String pdConfig) throws
                                                 PDException,
                                                 IOException {
        Properties configs = getYmlConfig(pdConfig);
        String property = client.get(TIMESTAMP_KEY).getValue();
        long pdLastModified = 0;
        if (!StringUtils.isEmpty(property)) {
            pdLastModified = Long.parseLong(property);
        }
        if (!pdConfFile.exists() || pdConfFile.lastModified() <= pdLastModified) {
            log.info("update local application-pd.yml from pd....{}", pdConfig);
            writeYml(pdConfig);
            PropertiesPropertySource source = new PropertiesPropertySource("pd-config", configs);
            sources.addFirst(source);
        }
    }

    private Properties getYmlConfig(String yml) {
        Yaml yaml = new Yaml();
        Iterable load = yaml.loadAll(yml);
        Iterator iterator = load.iterator();
        Properties properties = new Properties();
        while (iterator.hasNext()) {
            Map<String, Object> next = (Map<String, Object>) iterator.next();
            map2Properties(next, "", properties);
        }
        return properties;
    }

    private void map2Properties(Map<String, Object> map, String prefix, Properties properties) {

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            String newPrefix = StringUtils.isEmpty(prefix) ? key : prefix + "." + key;
            Object value = entry.getValue();
            if (!(value instanceof Map)) {
                properties.put(newPrefix, value);
            } else {
                map2Properties((Map<String, Object>) value, newPrefix, properties);
            }

        }
    }

    public ConfigurableApplicationContext getContext() {
        return context;
    }

    public void setContext(ConfigurableApplicationContext context) {
        this.context = context;
    }

    // private void restartBySpringBootApplication() {
    //   ApplicationArguments args = context.getBean(ApplicationArguments.class);
    //    Thread thread = new Thread(() -> {
    //        context.close();
    //        try {
    //            Thread.sleep(5000L);
    //        } catch (InterruptedException e) {
    //
    //        }
    //        StoreNodeApplication.start();
    //    });
    //    thread.setDaemon(false);
    //    thread.start();
    // }

    private void restart() throws InterruptedException, IOException {
        ProcessBuilder builder;
        String os = System.getProperty("os.name");
        if (os.toLowerCase(Locale.getDefault()).contains("win")) {
            builder = new ProcessBuilder("cmd", "/c", restartShellPath).inheritIO();
        } else {
            log.info("run shell {}", restartShellPath);
            builder = new ProcessBuilder("sh", "-c", restartShellPath).inheritIO();
        }
        SecureRandom random = new SecureRandom();
        int sleepTime = random.nextInt(60);
        log.info("app will restart in {} seconds:", sleepTime);
        Thread.sleep(sleepTime * 1000);
        Process process = builder.start();
        log.info("waiting restart.... {}", restartShellPath);
        process.waitFor();
    }

    private void writeYml(String yml) throws IOException {
        FileUtils.writeStringToFile(pdConfFile, yml, Charset.defaultCharset(), false);
    }
}
