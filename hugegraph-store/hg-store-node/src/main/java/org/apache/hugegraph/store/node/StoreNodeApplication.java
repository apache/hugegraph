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

package org.apache.hugegraph.store.node;

import org.apache.hugegraph.store.node.listener.PdConfigureListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.alipay.remoting.util.StringUtils;

/**
 *
 */
@SpringBootApplication
public class StoreNodeApplication {

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        // Set the log location for the slot usage
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs");
        }
        System.setProperty("com.alipay.remoting.client.log.level", "WARN");
        if (System.getProperty("bolt.channel_write_buf_low_water_mark") == null) {
            System.setProperty("bolt.channel_write_buf_low_water_mark",
                               Integer.toString(4 * 1024 * 1024));
        }
        if (System.getProperty("bolt.channel_write_buf_high_water_mark") == null) {
            System.setProperty("bolt.channel_write_buf_high_water_mark",
                               Integer.toString(8 * 1024 * 1024));
        }
        SpringApplication application = new SpringApplication(StoreNodeApplication.class);
        PdConfigureListener listener = new PdConfigureListener();
        application.addListeners(listener);
        ConfigurableApplicationContext context = application.run();
        listener.setContext(context);
        System.out.println("StoreNodeApplication started.");
    }
}
