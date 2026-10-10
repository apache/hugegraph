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

package org.apache.hugegraph.store.client.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

import org.apache.hugegraph.store.HgKvIterator;
import org.junit.Test;

public class MultiStreamIteratorTest {

    @Test
    public void testCloseCancelsEveryChildBeforeIterationAndIsTerminal() {
        HgKvIterator<Integer> first = mock(HgKvIterator.class);
        HgKvIterator<Integer> second = mock(HgKvIterator.class);
        MultiStreamIterator<Integer> composite = new MultiStreamIterator<>(List.of(first, second, first));
        composite.close();
        composite.close();
        assertFalse(composite.hasNext());
        verify(first).close();
        verify(second).close();
        verifyNoMoreInteractions(first, second);
        try {
            composite.next();
            fail("Closed iterator must not resume a child stream");
        } catch (java.util.NoSuchElementException expected) {
            // Closed is terminal.
        }
    }

    @Test
    public void testCloseAttemptsAllChildrenAndPreservesPrimaryFailure() {
        HgKvIterator<Integer> first = mock(HgKvIterator.class);
        HgKvIterator<Integer> second = mock(HgKvIterator.class);
        HgKvIterator<Integer> third = mock(HgKvIterator.class);
        RuntimeException primary = new IllegalStateException("first");
        RuntimeException later = new IllegalArgumentException("second");
        doThrow(primary).when(first).close();
        doThrow(later).when(second).close();
        MultiStreamIterator<Integer> composite = new MultiStreamIterator<>(List.of(first, second, third));
        try {
            composite.close();
            fail("Cleanup failure must be visible");
        } catch (RuntimeException error) {
            assertSame(primary, error);
            assertEquals(1, error.getSuppressed().length);
            assertSame(later, error.getSuppressed()[0]);
        }
        verify(first).close();
        verify(second).close();
        verify(third).close();
        assertFalse(composite.hasNext());
    }
}
