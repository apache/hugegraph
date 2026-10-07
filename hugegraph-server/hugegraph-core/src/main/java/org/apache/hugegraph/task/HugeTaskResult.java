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

package org.apache.hugegraph.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;

public class HugeTaskResult {
    private static final Logger LOG = Log.logger(HugeTaskResult.class);
    private static final float DECOMPRESS_RATIO = 10.0F;
    private final String taskResultId;
    private volatile String result;

    public HugeTaskResult(String taskId) {
        this.taskResultId = taskId;
        this.result = null;
    }

    public static String genId(Id taskId) {
        return String.format("task_result_%d", taskId.asLong());
    }

    public static HugeTaskResult fromVertex(Vertex vertex) {
        Id taskResultId = (Id) vertex.id();
        HugeTaskResult taskResult = new HugeTaskResult(taskResultId.asString());
        for (Iterator<VertexProperty<Object>> iter = vertex.properties(); iter.hasNext(); ) {
            VertexProperty<Object> prop = iter.next();
            taskResult.property(prop.key(), prop.value());
        }
        return taskResult;
    }

    public String taskResultId() {
        return this.taskResultId;
    }

    public void result(String result) {
        this.result = result;
    }

    public String result() {
        return this.result;
    }

    protected synchronized Object[] asArray() {

        List<Object> list = new ArrayList<>(6);

        list.add(T.label);
        list.add(HugeTaskResult.P.TASKRESULT);

        list.add(T.id);
        list.add(this.taskResultId);

        if (this.result != null) {
            byte[] bytes = StringEncoding.compress(this.result);
            list.add(HugeTaskResult.P.RESULT);
            list.add(bytes);
        }

        return list.toArray();
    }

    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.RESULT:
                this.result = StringEncoding.decompress(((Blob) value).bytes(), DECOMPRESS_RATIO);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    public static final class P {

        public static final String TASKRESULT = Graph.Hidden.hide("taskresult");

        public static final String RESULT = "~result_result";

        public static String unhide(String key) {
            final String prefix = Graph.Hidden.hide("result_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }
}
