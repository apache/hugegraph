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

package org.apache.hugegraph.backend.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Action;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;

public class BackendMutation {

    private final MutationTable updates;

    public BackendMutation() {
        this.updates = new MutationTable();
    }

    public BackendMutation(int initialCapacity) {
        this.updates = new MutationTable(initialCapacity);
    }

    /**
     * Add data entry with an action to collection `updates`
     *
     * @param entry  the backend entry
     * @param action operate action on the entry
     */
    @Watched(prefix = "mutation")
    public void add(BackendEntry entry, Action action) {
        Id id = entry.id();
        assert id != null;
        if (this.updates.containsKey(entry.type(), id)) {
            this.optimizeUpdates(entry, action);
        } else {
            // If there is no entity with this id, add it
            this.updates.put(entry.type(), id, BackendAction.of(action, entry));
        }
    }

    /**
     * Put directly without checking and merging
     */
    public void put(BackendEntry entry, Action action) {
        Id id = entry.id();
        this.updates.put(entry.type(), id, BackendAction.of(action, entry));
    }

    /**
     * The optimized scenes include but are not limited to:
     * 1.If you want to delete an entry, the other mutations previously
     * can be ignored.
     * 2.As similar to the No.1 item, If you want to insert an entry,
     * the other mutations previously also can be ignored.
     * 3.If you append an entry and then eliminate it, the new action
     * can override the old one.
     */
    @Watched(prefix = "mutation")
    private void optimizeUpdates(BackendEntry entry, Action action) {
        final Id id = entry.id();
        assert id != null;
        final List<BackendAction> items = this.updates.get(entry.type(), id);
        assert items != null;
        boolean ignoreCurrent = false;
        for (Iterator<BackendAction> iter = items.iterator(); iter.hasNext(); ) {
            BackendAction originItem = iter.next();
            Action originAction = originItem.action();
            switch (action) {
                case INSERT:
                    iter.remove();
                    break;
                case DELETE:
                    if (originAction == Action.INSERT) {
                        throw incompatibleActionException(action, originAction);
                    } else if (originAction == Action.DELETE) {
                        ignoreCurrent = true;
                    } else {
                        iter.remove();
                    }
                    break;
                case APPEND:
                    if (entry.type().isUniqueIndex() &&
                        originAction == Action.APPEND) {
                        throw new IllegalArgumentException(String.format(
                                "Unique constraint conflict is found in" +
                                " transaction between %s and %s",
                                entry, originItem.entry()));
                    }

                    if (originAction == Action.INSERT ||
                        originAction == Action.DELETE) {
                        throw incompatibleActionException(action, originAction);
                    } else {
                        Id subId = entry.subId();
                        Id originSubId = originItem.entry().subId();
                        assert subId != null;
                        if (subId == originSubId || subId.equals(originSubId)) {
                            iter.remove();
                        }
                    }
                    break;
                case ELIMINATE:
                    if (originAction == Action.INSERT ||
                        originAction == Action.DELETE) {
                        throw incompatibleActionException(action, originAction);
                    } else {
                        Id subId = entry.subId();
                        Id originSubId = originItem.entry().subId();
                        assert subId != null;
                        if (subId == originSubId || subId.equals(originSubId)) {
                            iter.remove();
                        }
                    }
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Unknown mutate action: %s", action));
            }
        }
        if (!ignoreCurrent) {
            items.add(BackendAction.of(action, entry));
        }
    }

    private static HugeException incompatibleActionException(
            Action newAction,
            Action originAction) {
        return new HugeException("The action '%s' is incompatible with " +
                                 "action '%s'", newAction, originAction);
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions
     * and deletions are added to this mutation. Does not remove duplicates
     * if such exist - this needs to be ensured by the caller.
     *
     * @param mutation another mutation to be merged
     */
    public void merge(BackendMutation mutation) {
        E.checkNotNull(mutation, "mutation");
        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext(); ) {
            BackendAction item = it.next();
            this.add(item.entry(), item.action());
        }
    }

    public Set<HugeType> types() {
        return this.updates.keys();
    }

    /**
     * Get all mutations
     *
     * @return mutations
     */
    public Iterator<BackendAction> mutation() {
        return this.updates.values();
    }

    /**
     * Get mutations by type
     *
     * @param type entry type
     * @return mutations
     */
    public Iterator<BackendAction> mutation(HugeType type) {
        return this.updates.get(type);
    }

    /**
     * Get mutations by type and id
     *
     * @param type entry type
     * @param id   entry id
     * @return mutations
     */
    public List<BackendAction> mutation(HugeType type, Id id) {
        return this.updates.get(type, id);
    }

    /**
     * Whether mutation contains entry and action
     *
     * @param entry  entry
     * @param action action
     * @return true if exist, otherwise false
     */
    public boolean contains(BackendEntry entry, Action action) {
        List<BackendAction> items = this.updates.get(entry.type(), entry.id());
        if (items == null || items.isEmpty()) {
            return false;
        }
        for (BackendAction item : items) {
            if (item.action().equals(action)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether mutation contains type and action
     *
     * @param type   type
     * @param action action
     * @return true if exist, otherwise false
     */
    public boolean contains(HugeType type, Action action) {
        for (Iterator<BackendAction> i = this.updates.get(type); i.hasNext(); ) {
            BackendAction entry = i.next();
            if (entry.action() == action) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether this mutation is empty
     *
     * @return true if empty, otherwise false
     */
    public boolean isEmpty() {
        return this.updates.size() == 0;
    }

    /**
     * Get size of mutations
     *
     * @return size
     */
    public int size() {
        return this.updates.size();
    }

    public void clear() {
        this.updates.clear();
    }

    @Override
    public String toString() {
        return String.format("BackendMutation{mutations=%s}", this.updates);
    }

    private static class MutationTable {

        // Mapping type => id => mutations
        private final Map<HugeType, Map<Id, List<BackendAction>>> mutations;

        public MutationTable() {
            // NOTE: ensure insert order
            this.mutations = InsertionOrderUtil.newMap();
        }

        public MutationTable(int initialCapacity) {
            // NOTE: ensure insert order
            this.mutations = InsertionOrderUtil.newMap(initialCapacity);
        }

        public void put(HugeType type, Id id, BackendAction mutation) {
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            if (table == null) {
                table = InsertionOrderUtil.newMap();
                this.mutations.put(type, table);
            }

            List<BackendAction> items = table.get(id);
            if (items == null) {
                items = new ArrayList<>();
                table.put(id, items);
            }

            items.add(mutation);
        }

        public boolean containsKey(HugeType type, Id id) {
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            return table != null && table.containsKey(id);
        }

        public List<BackendAction> get(HugeType type, Id id) {
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            if (table == null) {
                return null;
            }
            return table.get(id);
        }

        public Iterator<BackendAction> get(HugeType type) {
            ExtendableIterator<BackendAction> rs = new ExtendableIterator<>();
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            if (table != null) {
                for (List<BackendAction> items : table.values()) {
                    rs.extend(items.iterator());
                }
            }
            return rs;
        }

        public Set<HugeType> keys() {
            return this.mutations.keySet();
        }

        public Iterator<BackendAction> values() {
            ExtendableIterator<BackendAction> rs = new ExtendableIterator<>();
            for (Map<Id, List<BackendAction>> table : this.mutations.values()) {
                for (List<BackendAction> items : table.values()) {
                    rs.extend(items.iterator());
                }
            }
            return rs;
        }

        public int size() {
            int size = 0;
            for (Map<Id, List<BackendAction>> m : this.mutations.values()) {
                // NOTE: Index entry has same id with different subIds
                for (List<BackendAction> actions : m.values()) {
                    size += actions.size();
                }
            }
            return size;
        }

        public void clear() {
            this.mutations.clear();
        }
    }

    public Map<HugeType, Map<Id, List<BackendAction>>> mutations() {
        return this.updates.mutations;
    }
}
