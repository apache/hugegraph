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

import java.util.NoSuchElementException;

import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.type.GraphType;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public abstract class HugeProperty<V> implements Property<V>, GraphType {

    protected final HugeElement owner;
    protected final PropertyKey pkey;
    protected BaseProperty<V> property;

    public HugeProperty(HugeElement owner, PropertyKey pkey, V value) {
        E.checkArgument(owner != null, "Property owner can't be null");
        E.checkArgument(pkey != null, "Property key can't be null");
        E.checkArgument(value != null, "Property value can't be null");

        this.owner = owner;
        this.pkey = pkey;
        this.property = new BaseProperty<>(pkey, pkey.validValueOrThrow(value));
    }

    protected HugeProperty(HugeElement owner, BaseProperty<V> property) {
        E.checkArgument(owner != null, "Property owner can't be null");
        E.checkArgument(property != null, "Property can't be null");
        this.owner = owner;
        this.pkey = property.propertyKey();
        this.property = property;
    }

    public BaseProperty<V> baseProperty() {
        return this.property;
    }

    public PropertyKey propertyKey() {
        return this.pkey;
    }

    public Object id() {
        return SplicingIdGenerator.concat(this.owner.id().asString(), this.key());
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY;
    }

    @Override
    public String name() {
        return this.pkey.name();
    }

    @Override
    public String key() {
        return this.pkey.name();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.property.value();
    }

    public Object serialValue(boolean encodeNumber) {
        return this.property.serialValue(encodeNumber);
    }

    @Override
    public boolean isPresent() {
        return null != this.value();
    }

    public boolean isAggregateType() {
        return !this.pkey.aggregateType().isNone();
    }

    @Override
    public HugeElement element() {
        return this.owner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeProperty)) {
            return false;
        }

        HugeProperty<?> other = (HugeProperty<?>) obj;
        return this.owner.equals(other.owner) && this.pkey.equals(other.pkey);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
