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

package org.apache.hugegraph.pd.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
public class DefaultThreadFactory implements ThreadFactory {

    private final AtomicInteger number = new AtomicInteger(1);
    private final String namePrefix;
    private boolean daemon;

    public DefaultThreadFactory(String prefix, boolean daemon) {
        this.namePrefix = prefix + "-";
        this.daemon = daemon;
    }

    public DefaultThreadFactory(String prefix) {
        this(prefix, true);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(null, r, namePrefix + number.getAndIncrement(), 0);
        t.setDaemon(daemon);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}
