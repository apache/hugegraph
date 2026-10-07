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

package org.apache.hugegraph.backend.page;

import java.util.Set;

import org.apache.hugegraph.id.Id;

import com.google.common.collect.ImmutableSet;

public final class PageIds {

    public static final PageIds EMPTY = new PageIds(ImmutableSet.of(),
                                                    PageState.EMPTY);

    private final Set<Id> ids;
    private final PageState pageState;

    public PageIds(Set<Id> ids, PageState pageState) {
        this.ids = ids;
        this.pageState = pageState;
    }

    public Set<Id> ids() {
        return this.ids;
    }

    public String page() {
        if (this.pageState == null) {
            return null;
        }
        return this.pageState.toString();
    }

    public PageState pageState() {
        return this.pageState;
    }

    public boolean empty() {
        return this.ids.isEmpty();
    }
}
