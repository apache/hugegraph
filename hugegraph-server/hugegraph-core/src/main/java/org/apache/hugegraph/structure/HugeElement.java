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

package org.apache.hugegraph.structure;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.GraphType;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.eclipse.collections.api.iterator.IntIterator;

public abstract class HugeElement implements Element, GraphType, Idfiable, Comparable<HugeElement> {

    private final HugeGraph graph;

    public HugeElement(final HugeGraph graph) {
        E.checkArgument(graph != null, "HugeElement graph can't be null");
        this.graph = graph;
    }

    /**
     * Shared state for internal adapters and codecs. As with setProperty(), mutations
     * through this view bypass transaction callbacks; application callers must use
     * the graph/TinkerPop mutation APIs to update persisted elements and indexes.
     */
    public abstract BaseElement element();

    protected abstract <V> HugeProperty<V> wrapProperty(BaseProperty<V> property);

    public abstract SchemaLabel schemaLabel();

    protected abstract GraphTransaction tx();

    protected abstract <V> HugeProperty<V> newProperty(PropertyKey pk, V val);

    protected abstract <V> void onUpdateProperty(Cardinality cardinality,
                                                 HugeProperty<V> prop);

    protected abstract boolean ensureFilledProperties(boolean throwIfNotExist);

    protected void updateToDefaultValueIfNone() {
        this.element().updateDefaultValues(this.graph()::propertyKey);
    }

    @Override
    public HugeGraph graph() {
        return this.graph;
    }

    protected void removed(boolean removed) {
        this.element().removed(removed);
    }

    public boolean removed() {
        return this.element().removed();
    }

    protected void fresh(boolean fresh) {
        this.element().fresh(fresh);
    }

    public boolean fresh() {
        return this.element().fresh();
    }

    public boolean isPropLoaded() {
        return this.element().propLoaded();
    }

    protected void propLoaded() {
        this.element().propLoaded(true);
    }

    public void propNotLoaded() {
        this.element().propLoaded(false);
    }

    public void forceLoad() {
        this.ensureFilledProperties(false);
    }

    public void committed() {
        this.element().fresh(false);
        // Set expired time
        this.setExpiredTimeIfNeeded();
    }

    public void setExpiredTimeIfNeeded() {
        this.element().setExpiredTimeIfNeeded(this.graph.now());
    }

    public long expiredTime() {
        return this.element().expiredTime();
    }

    public void expiredTime(long expiredTime) {
        this.element().expiredTime(expiredTime);
    }

    public boolean expired() {
        return 0L < this.expiredTime() && this.expiredTime() < this.graph.now();
    }

    public long ttl() {
        return this.element().ttl(this.graph.now());
    }

    public boolean hasTtl() {
        return this.element().hasTtl();
    }

    public Set<Id> getPropertyKeys() {
        Set<Id> propKeys = InsertionOrderUtil.newSet();
        IntIterator keys = this.element().properties().keysView().intIterator();
        while (keys.hasNext()) {
            propKeys.add(IdGenerator.of(keys.next()));
        }
        return propKeys;
    }

    public Collection<HugeProperty<?>> getProperties() {
        Collection<BaseProperty<?>> properties = this.element().properties().values();
        return new AbstractCollection<HugeProperty<?>>() {
            @Override
            public Iterator<HugeProperty<?>> iterator() {
                Iterator<BaseProperty<?>> iterator = properties.iterator();
                return new Iterator<HugeProperty<?>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public HugeProperty<?> next() {
                        return HugeElement.this.wrapProperty(iterator.next());
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }

            @Override
            public int size() {
                return properties.size();
            }

            @Override
            public void clear() {
                properties.clear();
            }
        };
    }

    public Collection<HugeProperty<?>> getFilledProperties() {
        this.ensureFilledProperties(true);
        return this.getProperties();
    }

    public Map<Id, Object> getPropertiesMap() {
        return this.element().getPropertiesMap();
    }

    public Collection<HugeProperty<?>> getAggregateProperties() {
        List<HugeProperty<?>> aggrProps = InsertionOrderUtil.newList();
        for (HugeProperty<?> prop : this.getProperties()) {
            if (prop.type().isAggregateProperty()) {
                aggrProps.add(prop);
            }
        }
        return aggrProps;
    }

    public <V> HugeProperty<V> getProperty(Id key) {
        BaseProperty<V> property = this.element().getProperty(key);
        return property == null ? null : this.wrapProperty(property);
    }

    public <V> V getPropertyValue(Id key) {
        return this.element().getPropertyValue(key);
    }

    public boolean hasProperty(Id key) {
        return this.element().hasProperty(key);
    }

    public boolean hasProperties() {
        return this.element().hasProperties();
    }

    public int sizeOfProperties() {
        return this.element().sizeOfProperties();
    }

    public int sizeOfSubProperties() {
        return this.element().sizeOfSubProperties();
    }

    @Watched(prefix = "element")
    public <V> HugeProperty<?> setProperty(HugeProperty<V> prop) {
        BaseProperty<?> previous = this.element().addProperty(prop.baseProperty());
        return previous == null ? null : this.wrapProperty(previous);
    }

    public <V> HugeProperty<?> removeProperty(Id key) {
        BaseProperty<?> previous = this.element().removeProperty(key);
        return previous == null ? null : this.wrapProperty(previous);
    }

    public <V> HugeProperty<V> addProperty(PropertyKey pkey, V value) {
        return this.addProperty(pkey, value, false);
    }

    @Watched(prefix = "element")
    public <V> HugeProperty<V> addProperty(PropertyKey pkey, V value, boolean notify) {
        Consumer<BaseProperty<?>> callback = property -> {
            if (notify) {
                this.onUpdateProperty(pkey.cardinality(), this.wrapProperty(property));
            }
        };
        BaseProperty<V> property = this.element().addProperty(
                pkey, value, (key, val) -> this.newProperty(key, val).baseProperty(),
                callback, callback);
        return this.wrapProperty(property);
    }

    public void resetProperties() {
        this.element().resetProperties();
        this.element().propLoaded(false);
    }

    protected void copyProperties(HugeElement element) {
        this.element().copyProperties(element.element());
    }

    public HugeElement copyAsFresh() {
        HugeElement elem = this.copy();
        elem.fresh(true);
        return elem;
    }

    public abstract HugeElement copy();

    public abstract Object sysprop(HugeKeys key);

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Element)) {
            return false;
        }

        Element other = (Element) obj;
        if (this.id() == null) {
            return false;
        }

        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        E.checkState(this.id() != null, "Element id can't be null");
        return ElementHelper.hashCode(this);
    }

    @Override
    public int compareTo(HugeElement o) {
        return this.id().compareTo(o.id());
    }

    /**
     * Classify parameter list(pairs) from call request
     *
     * @param keyValues The property key-value pair of the vertex or edge
     * @return Key-value pairs that are classified and processed
     */
    @Watched(prefix = "element")
    public static final ElementKeys classifyKeys(Object... keyValues) {
        ElementKeys elemKeys = new ElementKeys();

        if ((keyValues.length & 1) == 1) {
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        }
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            Object val = keyValues[i + 1];

            if (!(key instanceof String) && !(key instanceof T)) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            if (val == null) {
                if (T.label.equals(key)) {
                    throw Element.Exceptions.labelCanNotBeNull();
                }
                // Ignore null value for tinkerpop test compatibility
                continue;
            }

            if (key.equals(T.id)) {
                elemKeys.id = val;
            } else if (key.equals(T.label)) {
                elemKeys.label = val;
            } else {
                elemKeys.keys.add(key.toString());
            }
        }
        return elemKeys;
    }

    public static final Id getIdValue(HugeType type, Object idValue) {
        assert type.isGraph();
        Id id = getIdValue(idValue);
        if (type.isVertex()) {
            return id;
        } else {
            if (id == null || id instanceof EdgeId) {
                return id;
            }
            return EdgeId.parse(id.asString());
        }
    }

    @Watched(prefix = "element")
    protected static Id getIdValue(Object idValue) {
        if (idValue == null) {
            return null;
        }

        if (idValue instanceof String) {
            // String id
            return IdGenerator.of((String) idValue);
        } else if (idValue instanceof Number) {
            // Long id
            return IdGenerator.of(((Number) idValue).longValue());
        } else if (idValue instanceof UUID) {
            // UUID id
            return IdGenerator.of((UUID) idValue);
        } else if (idValue instanceof Id) {
            // Id itself
            return (Id) idValue;
        } else if (idValue instanceof Element) {
            // Element
            return (Id) ((Element) idValue).id();
        }

        // Throw if error type
        throw new UnsupportedOperationException(String.format(
                "Invalid element id: %s(%s)",
                idValue, idValue.getClass().getSimpleName()));
    }

    @Watched(prefix = "element")
    public static final Object getLabelValue(Object... keyValues) {
        Object labelValue = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                labelValue = keyValues[i + 1];
                E.checkArgument(labelValue instanceof String ||
                                labelValue instanceof VertexLabel,
                                "Expect a string or a VertexLabel object " +
                                "as the vertex label argument, but got: '%s'",
                                labelValue);
                if (labelValue instanceof String) {
                    ElementHelper.validateLabel((String) labelValue);
                }
                break;
            }
        }
        return labelValue;
    }

    public static int intFromId(Id id) {
        return BaseElement.intFromId(id);
    }

    public static final class ElementKeys {

        private Object label = null;
        private Object id = null;
        private Set<String> keys = new HashSet<>();

        public Object label() {
            return this.label;
        }

        public void label(Object label) {
            this.label = label;
        }

        public Object id() {
            return this.id;
        }

        public void id(Object id) {
            this.id = id;
        }

        public Set<String> keys() {
            return this.keys;
        }

        public void keys(Set<String> keys) {
            this.keys = keys;
        }
    }
}
