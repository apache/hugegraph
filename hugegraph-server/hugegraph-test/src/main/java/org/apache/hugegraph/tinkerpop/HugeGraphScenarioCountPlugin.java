/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hugegraph.tinkerpop;

import java.util.concurrent.atomic.AtomicInteger;

import io.cucumber.plugin.ConcurrentEventListener;
import io.cucumber.plugin.event.EventPublisher;
import io.cucumber.plugin.event.TestCaseStarted;
import io.cucumber.plugin.event.TestRunFinished;

public final class HugeGraphScenarioCountPlugin
        implements ConcurrentEventListener {

    /*
     * This is the exact number selected by TAGS and NAMES for TinkerPop 3.8.1.
     * Update it together with an intentional filter or TinkerPop change.
     */
    private static final int EXPECTED_SCENARIOS = 361;

    private final AtomicInteger scenarioCount = new AtomicInteger();

    @Override
    public void setEventPublisher(EventPublisher publisher) {
        publisher.registerHandlerFor(TestCaseStarted.class,
                                     event -> this.scenarioCount.incrementAndGet());
        publisher.registerHandlerFor(TestRunFinished.class,
                                     event -> this.finishRun());
    }

    private void finishRun() {
        try {
            assertScenariosExecuted(this.scenarioCount.get());
        } finally {
            HugeGraphWorld.clearProvider();
        }
    }

    static void assertScenariosExecuted(int scenarioCount) {
        if (scenarioCount != EXPECTED_SCENARIOS) {
            throw new AssertionError(
                    scenarioCount + " TinkerPop Gherkin scenarios were " +
                    "executed, expected exactly " + EXPECTED_SCENARIOS +
                    " (check the TAGS/NAMES filters and update the expected " +
                    "count for intentional changes)");
        }
    }
}
