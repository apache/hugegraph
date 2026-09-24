/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.type.GraphType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.slf4j.Logger;


public abstract class BaseElement implements GraphType, Idfiable, Serializable {

    private static final Logger LOG = Log.logger(BaseElement.class);

    public static final MutableIntObjectMap<BaseProperty<?>> EMPTY_MAP =
                                                    new IntObjectHashMap<>();

    private static final int MAX_PROPERTIES = BytesBuffer.UINT16_MAX;

    MutableIntObjectMap<BaseProperty<?>> properties;

    Id id;
    private SchemaLabel schemaLabel;
    long expiredTime; // TODO: move into properties to keep small object

    private boolean removed;
    private boolean fresh;
    private boolean propLoaded;
    private boolean defaultValueUpdated;

    public BaseElement() {
        this.properties = EMPTY_MAP;
        this.removed = false;
        this.fresh = false;
        this.propLoaded = true;
        this.defaultValueUpdated = false;
    }

    public void setProperties(MutableIntObjectMap<BaseProperty<?>> properties) {
        this.properties = properties;
    }

    public Id id(){
        return id;
    }

    public void id(Id id) {
        this.id = id;
    }

    public boolean removed() {
        return removed;
    }

    public void removed(boolean removed) {
        this.removed = removed;
    }

    public boolean fresh() {
        return fresh;
    }

    public void fresh(boolean fresh) {
        this.fresh = fresh;
    }

    public boolean propLoaded() {
        return propLoaded;
    }

    public void propLoaded(boolean propLoaded) {
        this.propLoaded = propLoaded;
    }

    public boolean defaultValueUpdated() {
        return defaultValueUpdated;
    }

    public void defaultValueUpdated(boolean defaultValueUpdated) {
        this.defaultValueUpdated = defaultValueUpdated;
    }
    public SchemaLabel schemaLabel() {
        return schemaLabel;
    }

    public void schemaLabel(SchemaLabel label) {
        this.schemaLabel = label;
    }
    public long expiredTime() {
        return expiredTime;
    }

    public void expiredTime(long expiredTime) {
        this.expiredTime = expiredTime;
    }

    public boolean hasTtl() {
        return this.schemaLabel.ttl() > 0L;
    }
    public boolean expired(long now) {
        boolean expired;
        SchemaLabel label = this.schemaLabel();
        if (label.ttl() == 0L) {
            // No ttl, not expired
            return false;
        }
        if (this.expiredTime() > 0L) {
            // Has ttl and set expiredTime properly
            expired = this.expiredTime() < now;
            LOG.debug("The element {} {} with expired time {} and now {}",
                    this, expired ? "expired" : "not expired",
                    this.expiredTime(), now);
            return expired;
        }
        // Has ttl, but failed to set expiredTime when insert
        LOG.error("The element {} should have positive expired time, " +
                        "but got {}! ttl is {} ttl start time is {}",
                this, this.expiredTime(), label.ttl(), label.ttlStartTimeName());
        if (SchemaLabel.NONE_ID.equals(label.ttlStartTime())) {
            // No ttlStartTime, can't decide whether timeout, treat not expired
            return false;
        }
        Date date = this.getPropertyValue(label.ttlStartTime());
        if (date == null) {
            // No ttlStartTime, can't decide whether timeout, treat not expired
            return false;
        }
        // Has ttlStartTime, re-calc expiredTime to decide whether timeout,
        long expiredTime = date.getTime() + label.ttl();
        expired = expiredTime < now;
        LOG.debug("The element {} {} with expired time {} and now {}",
                this, expired ? "expired" : "not expired",
                expiredTime, now);
        return expired;
    }

    public long ttl(long now) {
        if (this.expiredTime() == 0L || this.expiredTime() < now) {
            return 0L;
        }
        return this.expiredTime() - now;
    }
    protected <V> BaseProperty<V> newProperty(PropertyKey pkey, V val) {
        return new BaseProperty<>(pkey, val);
    }

    public boolean hasProperty(Id key) {
        return this.properties.containsKey(intFromId(key));
    }

    public boolean hasProperties() {
        return this.properties.size() > 0;
    }


    public void setExpiredTimeIfNeeded(long now) {
        SchemaLabel label = this.schemaLabel();
        if (label.ttl() == 0L) {
            return;
        }

        if (SchemaLabel.NONE_ID.equals(label.ttlStartTime())) {
            this.expiredTime(now + label.ttl());
            return;
        }
        Date date = this.getPropertyValue(label.ttlStartTime());
        if (date == null) {
            this.expiredTime(now + label.ttl());
            return;
        }
        long expired = date.getTime() + label.ttl();
        E.checkArgument(expired > now,
                "The expired time '%s' of '%s' is prior to now: %s",
                new Date(expired), this, now);
        this.expiredTime(expired);
    }

    public void resetProperties() {
        this.properties = CollectionFactory.newIntObjectMap();
        this.propLoaded(true);
    }

    public <V> V getPropertyValue(Id key) {
        BaseProperty<?> prop = this.properties.get(intFromId(key));
        if (prop == null) {
            return null;
        }
        return (V) prop.value();
    }
    public MutableIntObjectMap<BaseProperty<?>> properties() {
        return this.properties;
    }

    public void properties(MutableIntObjectMap<BaseProperty<?>> properties) {
        this.properties = properties;
    }
    public <V> BaseProperty<V> getProperty(Id key) {
        return (BaseProperty<V>) this.properties.get(intFromId(key));
    }

    private <V> BaseProperty<V> addProperty(PropertyKey pkey, V value,
                                            Supplier<Collection<V>> supplier) {
        assert pkey.cardinality().multiple();
        BaseProperty<Collection<V>> property;
        if (this.hasProperty(pkey.id())) {
            property = this.getProperty(pkey.id());
        } else {
            property = this.newProperty(pkey, supplier.get());
            this.addProperty(property);
        }

        Collection<V> values;
        if (pkey.cardinality() == Cardinality.SET) {
            if (value instanceof Set) {
                values = (Set<V>) value;
            } else {
                values = CollectionUtil.toSet(value);
            }
        } else {
            assert pkey.cardinality() == Cardinality.LIST;
            if (value instanceof List) {
                values = (List<V>) value;
            } else {
                values = CollectionUtil.toList(value);
            }
        }
        property.value().addAll(values);

        // Any better ways?
        return (BaseProperty) property;
    }

    public <V> BaseProperty<V> addProperty(PropertyKey pkey, V value) {
        BaseProperty<V> prop = null;
        switch (pkey.cardinality()) {
            case SINGLE:
                prop = this.newProperty(pkey, value);
                this.addProperty(prop);
                break;
            case SET:
                prop = this.addProperty(pkey, value, HashSet::new);
                break;
            case LIST:
                prop = this.addProperty(pkey, value, ArrayList::new);
                break;
            default:
                assert false;
                break;
        }
        return prop;
    }

    public <V> BaseProperty<?> addProperty(BaseProperty<V> prop) {
        if (this.properties == EMPTY_MAP) {
            this.properties = new IntObjectHashMap<>(); // change to CollectionFactory.newIntObjectMap();
        }
        PropertyKey pkey = prop.propertyKey();

        E.checkArgument(this.properties.containsKey(intFromId(pkey.id())) ||
                        this.properties.size() < MAX_PROPERTIES,
                "Exceeded the maximum number of properties");
        return this.properties.put(intFromId(pkey.id()), prop);
    }
    public Map<Id, BaseProperty<?>> getProperties() {
        Map<Id, BaseProperty<?>> props = new HashMap<>();
        for (IntObjectPair<BaseProperty<?>> e : this.properties.keyValuesView()) {
            props.put(IdGenerator.of(e.getOne()), e.getTwo());
        }
        return props;
    }

    public <V> BaseProperty<?> removeProperty(Id key) {
        return this.properties.remove(intFromId(key));
    }

    /* a util may be should be moved to other place */
    public static int intFromId(Id id) {
        E.checkArgument(id instanceof IdGenerator.LongId,
                "Can't get number from %s(%s)", id, id.getClass());
        return ((IdGenerator.LongId) id).intValue();
    }

    public abstract Object sysprop(HugeKeys key);

    public Map<Id, Object> getPropertiesMap() {
        Map<Id, Object> props = new HashMap<>();
        for (IntObjectPair<BaseProperty<?>> e : this.properties.keyValuesView()) {
            props.put(IdGenerator.of(e.getOne()), e.getTwo().value());
        }
        return props;
    }

    public int sizeOfProperties() {
        return this.properties.size();
    }

    public int sizeOfSubProperties() {
        int size = 0;
        for (BaseProperty<?> p : this.properties.values()) {
            size++;
            if (p.propertyKey().cardinality() != Cardinality.SINGLE &&
                p.value() instanceof Collection) {
                size += ((Collection<?>) p.value()).size();
            }
        }
        return size;
    }

    @Override
    public BaseElement clone() throws CloneNotSupportedException{
        return (BaseElement) super.clone();
    }
}
