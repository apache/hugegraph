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

package org.apache.hugegraph.meta.lock;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.LockResponse;

public class PdDistributedLock {

    private static final int poolSize = 8;
    private final KvClient<?> client;
    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(poolSize, r -> {
        Thread t = new Thread(r, "keepalive");
        t.setDaemon(true);
        return t;
    });

    public PdDistributedLock(KvClient<?> client) {
        this.client = client;
    }

    public LockResult lock(String key, long second) {
        long ttl = second * 1000L;
        try {
            LockResponse response = this.client.lockWithoutReentrant(key, ttl);
            boolean succeed = response.getSucceed();
            LockResult result = new LockResult();
            if (succeed) {
                result.setLeaseId(response.getClientId());
                result.lockSuccess(true);
                long period = ttl - ttl / 4;
                ScheduledFuture<?> future = service.scheduleAtFixedRate(() -> {
                    // TODO: why synchronized?
                    synchronized (result) {
                        keepAlive(key);
                    }
                }, period, period, TimeUnit.MILLISECONDS);
                result.setFuture(future);
            }
            return result;
        } catch (PDException e) {
            throw new HugeException("Failed to lock '%s' to pd", e, key);
        }
    }

    public void unLock(String key, LockResult lockResult) {
        try {
            LockResponse response = this.client.unlock(key);
            boolean succeed = response.getSucceed();
            if (!succeed) {
                throw new HugeException("Failed to unlock '%s' to pd", key);
            }
            if (lockResult.getFuture() != null) {
                // TODO: why synchronized?
                synchronized (lockResult) {
                    lockResult.getFuture().cancel(true);
                }
            }
        } catch (PDException e) {
            throw new HugeException("Failed to unlock '%s' to pd", e, key);
        }
    }

    public boolean keepAlive(String key) {
        try {
            LockResponse alive = this.client.keepAlive(key);
            return alive.getSucceed();
        } catch (PDException e) {
            throw new HugeException("Failed to keepAlive '%s' to pd", key);
        }
    }
}
