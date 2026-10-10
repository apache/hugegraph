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

package org.apache.hugegraph.example;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.perf.PerfUtil;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class ExampleUtil {

    private static final Logger LOG = Log.logger(ExampleUtil.class);

    private static boolean registered = false;

    public static void registerPlugins() {
        if (registered) {
            return;
        }
        registered = true;

        RegisterUtil.registerHBase();
        RegisterUtil.registerRocksDB();
    }

    public static HugeGraph loadGraph() {
        return loadGraph(true, false);
    }

    public static HugeGraph loadGraph(boolean needClear, boolean needProfile) {
        if (needProfile) {
            profile();
        }

        registerPlugins();

        String conf = "hugegraph.properties";
        try {
            String path = ExampleUtil.class.getClassLoader()
                                           .getResource(conf).getPath();
            File file = new File(path);
            if (file.exists() && file.isFile()) {
                conf = path;
            }
        } catch (Exception ignored) {
            LOG.warn("loadGraph warn {} ", ignored);
        }

        HugeGraph graph = HugeFactory.open(conf);

        if (needClear) {
            graph.clearBackend();
        }
        graph.initBackend();
        graph.serverStarted(GlobalMasterInfo.master("server1"));

        return graph;
    }

    public static void profile() {
        try {
            PerfUtil.instance().profilePackage("org.apache.hugegraph");
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void waitAllTaskDone(HugeGraph graph) {
        TaskScheduler scheduler = graph.taskScheduler();
        Iterator<HugeTask<Object>> tasks = scheduler.tasks(null, -1L, null);
        while (tasks.hasNext()) {
            try {
                scheduler.waitUntilTaskCompleted(tasks.next().id(), 20L);
            } catch (TimeoutException e) {
                throw new HugeException("Failed to wait task done", e);
            }
        }
    }
}
