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

package org.apache.hugegraph.vector;

import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.event.Event;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;

public class ServerVectorScheduler implements VectorTaskScheduler {

    private final EventHub hub;

    private static final String VECTOR_INDEX_EVENT = "vector-index-task";

    public ServerVectorScheduler(ExecutorService executor, EventHub hub) {
        this.hub = hub;

        this.hub.listen(VECTOR_INDEX_EVENT, new EventListener() {
            @Override
            public Object event(Event event) {
                Runnable task = (Runnable) event.args()[0];
                task.run();
                return null;
            }
        });
    }

    @Override
    public void execute(Runnable task) {
        hub.notify(VECTOR_INDEX_EVENT, task);
    }
}
