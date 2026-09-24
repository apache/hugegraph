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

package org.apache.hugegraph.util.collection;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.define.CollectionType;

import org.apache.hugegraph.iterator.ExtendableIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

public class IdSet extends AbstractSet<Id> {

    private final LongHashSet numberIds;
    private final Set<Id> nonNumberIds;

    public IdSet(CollectionType type) {
        this.numberIds = new LongHashSet();
        this.nonNumberIds = CollectionFactory.newSet(type);
    }

    @Override
    public int size() {
        return this.numberIds.size() + this.nonNumberIds.size();
    }

    @Override
    public boolean isEmpty() {
        return this.numberIds.isEmpty() && this.nonNumberIds.isEmpty();
    }

    @Override
    public boolean contains(Object object) {
        if (!(object instanceof Id)) {
            return false;
        }
        Id id = (Id) object;
        if (id.type() == Id.IdType.LONG) {
            return this.numberIds.contains(id.asLong());
        } else {
            return this.nonNumberIds.contains(id);
        }
    }

    @Override
    public Iterator<Id> iterator() {
        return new ExtendableIterator<>(
               this.nonNumberIds.iterator(),
               new EcIdIterator(this.numberIds.longIterator()));
    }

    @Override
    public boolean add(Id id) {
        if (id.type() == Id.IdType.LONG) {
            return this.numberIds.add(id.asLong());
        } else {
            return this.nonNumberIds.add(id);
        }
    }

    public boolean remove(Id id) {
        if (id.type() == Id.IdType.LONG) {
            return this.numberIds.remove(id.asLong());
        } else {
            return this.nonNumberIds.remove(id);
        }
    }

    @Override
    public void clear() {
        this.numberIds.clear();
        this.nonNumberIds.clear();
    }

    private static class EcIdIterator implements Iterator<Id> {

        private final MutableLongIterator iterator;

        public EcIdIterator(MutableLongIterator iter) {
            this.iterator = iter;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Id next() {
            return IdGenerator.of(this.iterator.next());
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }
    }
}
