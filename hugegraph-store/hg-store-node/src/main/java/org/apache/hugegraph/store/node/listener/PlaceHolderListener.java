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
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/7/17
 **/
@Slf4j
public class PlaceHolderListener implements ApplicationListener<ApplicationReadyEvent> {

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        try {
            AppConfig config = event.getApplicationContext().getBean(AppConfig.class);
            String dataPath = config.getDataPath();
            String[] paths = dataPath.split(",");
            Integer size = config.getPlaceholderSize();
            Arrays.stream(paths).parallel().forEach(path -> {
                if (!StringUtils.isEmpty(path)) {
                    File ph = new File(path + "/" + HgStoreEngineOptions.PLACE_HOLDER_PREFIX);
                    if (!ph.exists() && size > 0) {
                        try {
                            FileUtils.touch(ph);
                            byte[] tmp = new byte[(int) FileUtils.ONE_GB];
                            for (int j = 0; j < size; j++) {
                                FileUtils.writeByteArrayToFile(ph, tmp, true);
                            }
                            RandomAccessFile raf = new RandomAccessFile(ph, "rw");
                            raf.setLength(size * FileUtils.ONE_GB);
                        } catch (Exception e) {
                            log.info("creating placeholder file got exception:", e);
                        }
                    }
                }
            });
        } catch (Exception e) {
            log.error("create placeholder file with error:", e);
        }
    }
}
