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

package org.apache.hugegraph.store.query;

import java.util.List;

import org.apache.hugegraph.id.Id;

public class PropertyList {

    /**
     * If empty or size is zero, do not filter
     */
    private final List<Id> propertyIds;
    /**
     * Not return property
     */
    private final boolean emptyId;

    private PropertyList(List<Id> propertyIds, boolean emptyId) {
        this.propertyIds = propertyIds;
        this.emptyId = emptyId;
    }

    public static PropertyList empty() {
        return new PropertyList(List.of(), true);
    }

    /**
     * default， return all properties
     *
     * @return
     */
    public static PropertyList of() {
        return new PropertyList(List.of(), false);
    }

    public static PropertyList of(List<Id> propertyIds) {
        return new PropertyList(propertyIds, false);
    }

    public List<Id> getPropertyIds() {
        return propertyIds;
    }

    public boolean isEmptyId() {
        return emptyId;
    }

    public boolean needSerialize() {
        return emptyId || (propertyIds != null && propertyIds.size() > 0);
    }

    @Override
    public String toString() {
        return "PropertyList{" +
               "propertyIds=" + propertyIds +
               ", isEmpty=" + emptyId +
               '}';
    }
}
