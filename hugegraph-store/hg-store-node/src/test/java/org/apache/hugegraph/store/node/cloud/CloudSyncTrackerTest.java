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

package org.apache.hugegraph.store.node.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CloudSyncTrackerTest {

    @Test
    public void parseSstFileNumber_parsesDigitBaseName() {
        assertEquals(123L, CloudSyncTracker.parseSstFileNumber("/data/db/000123.sst"));
        assertEquals(8L, CloudSyncTracker.parseSstFileNumber("0/000008.sst"));
        assertEquals(0L, CloudSyncTracker.parseSstFileNumber("000000.sst"));
    }

    @Test
    public void parseSstFileNumber_rejectsNonSstAndNonNumeric() {
        assertEquals(-1L, CloudSyncTracker.parseSstFileNumber("/data/db/CURRENT"));
        assertEquals(-1L, CloudSyncTracker.parseSstFileNumber("/data/db/MANIFEST-000001"));
        assertEquals(-1L, CloudSyncTracker.parseSstFileNumber("/data/db/abc.sst"));
        assertEquals(-1L, CloudSyncTracker.parseSstFileNumber(null));
        assertEquals(-1L, CloudSyncTracker.parseSstFileNumber(".sst"));
    }

    @Test
    public void markIsConfirmedAndClear_roundTrip() {
        CloudSyncTracker tracker = new CloudSyncTracker();
        String path = "0/000042.sst";

        assertFalse(tracker.isConfirmed("0", path));
        tracker.markConfirmed("0", path);
        assertTrue(tracker.isConfirmed("0", path));
        assertEquals(1L, tracker.confirmedCount("0"));

        tracker.clearConfirmed("0", path);
        assertFalse(tracker.isConfirmed("0", path));
        assertEquals(0L, tracker.confirmedCount("0"));
    }

    @Test
    public void confirmedBitmapsAreScopedPerDb() {
        CloudSyncTracker tracker = new CloudSyncTracker();
        tracker.markConfirmed("0", "0/000001.sst");
        // Same file number, different DB, must be independent.
        assertFalse(tracker.isConfirmed("1", "1/000001.sst"));
        assertTrue(tracker.isConfirmed("0", "0/000001.sst"));
    }

    @Test
    public void nonSstPathsAreIgnored() {
        CloudSyncTracker tracker = new CloudSyncTracker();
        tracker.markConfirmed("0", "0/CURRENT");
        assertEquals(0L, tracker.confirmedCount("0"));
        assertFalse(tracker.isConfirmed("0", "0/CURRENT"));
    }
}
