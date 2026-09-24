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

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.LOCK_DEFAULT_LEASE;
import static org.apache.hugegraph.meta.MetaManager.LOCK_DEFAULT_TIMEOUT;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.auth.SchemaDefine;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.util.JsonUtil;

public class AbstractMetaManager {

    protected final MetaDriver metaDriver;
    protected final String cluster;

    public AbstractMetaManager(MetaDriver metaDriver, String cluster) {
        this.metaDriver = metaDriver;
        this.cluster = cluster;
    }

    protected static String serialize(SchemaDefine.AuthElement element) {
        Map<String, Object> objectMap = element.asMap();
        return JsonUtil.toJson(objectMap);
    }

    protected static String serialize(SchemaElement element) {
        Map<String, Object> objectMap = element.asMap();
        return JsonUtil.toJson(objectMap);
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> configMap(String config) {
        return JsonUtil.fromJson(config, Map.class);
    }

    protected <T> void listen(String key, Consumer<T> consumer) {
        this.metaDriver.listen(key, consumer);
    }

    protected <T> void listenPrefix(String prefix, Consumer<T> consumer) {
        this.metaDriver.listenPrefix(prefix, consumer);
    }

    public String getRaw(String key) {
        String result = this.metaDriver.get(key);
        return Optional.ofNullable(result).orElse("");
    }

    public void putOrDeleteRaw(String key, String val) {
        if (StringUtils.isEmpty(val)) {
            this.metaDriver.delete(key);
        } else {
            this.metaDriver.put(key, val);
        }
    }

    public LockResult lock(String... keys) {
        return this.lock(LOCK_DEFAULT_LEASE, keys);
    }

    public LockResult lock(long ttl, String... keys) {
        String key = String.join(META_PATH_DELIMITER, keys);
        return this.lock(key, ttl);
    }

    public LockResult lock(String key, long ttl) {
        LockResult lockResult = this.metaDriver.tryLock(key, ttl, LOCK_DEFAULT_TIMEOUT);
        if (!lockResult.lockSuccess()) {
            throw new HugeException("Failed to lock '%s'", key);
        }
        return lockResult;
    }

    public LockResult tryLock(String key) {
        return this.metaDriver.tryLock(key, LOCK_DEFAULT_LEASE,
                                       LOCK_DEFAULT_TIMEOUT);
    }

    public void unlock(LockResult lockResult, String... keys) {
        String key = String.join(META_PATH_DELIMITER, keys);
        this.unlock(key, lockResult);
    }

    public void unlock(String key, LockResult lockResult) {
        this.metaDriver.unlock(key, lockResult);
    }
}
