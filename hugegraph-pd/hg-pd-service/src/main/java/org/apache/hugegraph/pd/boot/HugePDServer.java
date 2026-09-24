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

package org.apache.hugegraph.pd.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.alipay.remoting.util.StringUtils;

/**
 * PD service startup class
 */
@ComponentScan(basePackages = {"org.apache.hugegraph.pd"})
@SpringBootApplication
public class HugePDServer {

    public static void main(String[] args) {
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            // TODO: enhance logging configuration
            System.setProperty("logging.path", "logs");
            System.setProperty("com.alipay.remoting.client.log.level", "error");
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(Thread.currentThread()));
        SpringApplication.run(HugePDServer.class);
        System.out.println("Hugegraph-pd started.");
    }
}
