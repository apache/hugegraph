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

package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.KvResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
// import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class LicenseClientImplTest {

    // @Test
    public void putLicense() {
        PDConfig pdConfig = PDConfig.of("localhost:8686,localhost:8687,localhost:8688");
        //PDConfig pdConfig = PDConfig.of("localhost:8686");
        pdConfig.setEnableCache(true);
        try (LicenseClient c = new LicenseClient(pdConfig)) {
            File file = new File("../conf/hugegraph.license");
            byte[] bytes = FileUtils.readFileToByteArray(file);
            Pdpb.PutLicenseResponse putLicenseResponse = c.putLicense(bytes);
            Pdpb.Error error = putLicenseResponse.getHeader().getError();
            log.info(error.getMessage());
            assert error.getType().equals(Pdpb.ErrorType.OK);
        } catch (Exception e) {
            log.error("put license with error: {}", e);
        }
    }

    // @Test
    public void getKv() {
        PDConfig pdConfig = PDConfig.of("10.157.12.36:8686");
        pdConfig.setEnableCache(true);
        try (KvClient c = new KvClient(pdConfig)) {
            KResponse kResponse = c.get("S:FS");
            Pdpb.Error error = kResponse.getHeader().getError();
            log.info(error.getMessage());
            assert error.getType().equals(Pdpb.ErrorType.OK);
            Properties ymlConfig = getYmlConfig(kResponse.getValue());
            Object property = ymlConfig.get("rocksdb.write_buffer_size");
            assert property.toString().equals("32000000");
        } catch (Exception e) {
            log.error("put license with error: {}", e);
        }
    }

    // @Test
    public void putKv() {
        PDConfig pdConfig = PDConfig.of("10.14.139.70:8688");
        pdConfig.setEnableCache(true);
        try (KvClient c = new KvClient(pdConfig)) {
            long l = System.currentTimeMillis();
            KvResponse kvResponse = c.put("S:Timestamp", String.valueOf(l));
            Pdpb.Error error = kvResponse.getHeader().getError();
            log.info(error.getMessage());
            assert error.getType().equals(Pdpb.ErrorType.OK);
        } catch (Exception e) {
            log.error("put license with error: {}", e);
        }
    }

    // @Test
    public void putKvLocal() {
        PDConfig pdConfig = PDConfig.of("localhost:8686");
        pdConfig.setEnableCache(true);
        try (KvClient c = new KvClient(pdConfig)) {
            long l = System.currentTimeMillis();
            KvResponse kvResponse = c.put("S:Timestamp", String.valueOf(l));
            Pdpb.Error error = kvResponse.getHeader().getError();
            log.info(error.getMessage());
            assert error.getType().equals(Pdpb.ErrorType.OK);
        } catch (Exception e) {
            log.error("put license with error: {}", e);
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
            String newPrefix = prefix == null || prefix.length() == 0 ? key : prefix + "." + key;
            Object value = entry.getValue();
            if (!(value instanceof Map)) {
                properties.put(newPrefix, value);
            } else {
                map2Properties((Map<String, Object>) value, newPrefix, properties);
            }

        }
    }

}
