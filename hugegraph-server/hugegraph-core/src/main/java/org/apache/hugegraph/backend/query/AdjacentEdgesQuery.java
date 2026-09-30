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

package org.apache.hugegraph.backend.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Condition.Relation;
import org.apache.hugegraph.backend.query.Condition.RelationType;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;

import com.google.common.collect.ImmutableList;

public class AdjacentEdgesQuery extends ConditionQuery {

    private Id ownerVertex;
    private Directions direction;
    private Id[] edgeLabels;

    private List<Condition> selfConditions;

    private static final Id[] EMPTY = new Id[0];

    public AdjacentEdgesQuery(Id ownerVertex, Directions direction) {
        this(ownerVertex, direction, EMPTY);
    }

    public AdjacentEdgesQuery(Id ownerVertex, Directions direction,
                              Id[] edgeLabels) {
        super(HugeType.EDGE);
        E.checkArgument(ownerVertex != null,
                        "The edge query must contain source vertex");
        E.checkArgument(direction != null,
                        "The edge query must contain direction");
        this.ownerVertex = ownerVertex;
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.selfConditions = null;
    }

    @Override
    public int conditionsSize() {
        int size = super.conditionsSize() + 2;
        if (this.edgeLabels.length > 0) {
            size += 1;
        }
        return size;
    }

    @Override
    public ConditionQuery query(Condition condition) {
        if (!condition.isRelation()) {
            return super.query(condition);
        }

        // Reset this.selfConditions cache
        this.selfConditions = null;

        // Update condition fields
        Relation relation = ((Condition.Relation) condition);
        Object key = relation.key();
        RelationType relationType = relation.relation();

        if (key == HugeKeys.OWNER_VERTEX) {
            this.ownerVertex = (Id) relation.value();
            return this;
        }
        if (key == HugeKeys.DIRECTION) {
            this.direction = (Directions) relation.value();
            return this;
        }
        if (key == HugeKeys.LABEL) {
            Collection<Id> labels = null;
            if (relationType == RelationType.EQ) {
                labels = ImmutableList.of((Id) relation.value());
            } else if (relationType == RelationType.IN) {
                @SuppressWarnings("unchecked")
                Collection<Id> value = (Collection<Id>) relation.value();
                labels = value;
            } else {
                E.checkArgument(false,
                                "Unexpected relation type '%s' for '%s'",
                                relationType, key);
            }

            if (this.edgeLabels.length == 0) {
                this.edgeLabels = labels.toArray(new Id[0]);
            } else {
                Collection<Id> edgeLabels = CollectionUtil.intersect(
                                            Arrays.asList(this.edgeLabels),
                                            labels);
                if (edgeLabels.isEmpty()) {
                    // Returns empty result if conditions are conflicting
                    this.limit(0L);
                }
                this.edgeLabels = edgeLabels.toArray(new Id[0]);
            }
            return this;
        }

        return super.query(condition);
    }

    @Override
    public Collection<Condition> conditions() {
        List<Condition> conds = this.selfConditions();
        if (super.conditionsSize() > 0) {
            conds.addAll(super.conditions());
        }
        return conds;
    }

    private List<Condition> selfConditions() {
        if (this.selfConditions != null) {
            /*
             * Return selfConditions cache if it has been collected before
             * NOTE: it's also to keep the serialized condition value
             */
            return this.selfConditions;
        }

        List<Condition> conds = InsertionOrderUtil.newList();

        conds.add(Condition.eq(HugeKeys.OWNER_VERTEX, this.ownerVertex));

        if (this.direction == Directions.BOTH) {
            conds.add(Condition.in(HugeKeys.DIRECTION, ImmutableList.of(
                                                       Directions.OUT,
                                                       Directions.IN)));
        } else {
            conds.add(Condition.eq(HugeKeys.DIRECTION, this.direction));
        }

        if (this.edgeLabels.length == 1) {
            conds.add(Condition.eq(HugeKeys.LABEL, this.edgeLabels[0]));
        } else if (this.edgeLabels.length > 1) {
            conds.add(Condition.in(HugeKeys.LABEL,
                                   Arrays.asList(this.edgeLabels)));
        }

        this.selfConditions = conds;
        return conds;
    }

    @Override
    public <T> T condition(Object key) {
        T cond = this.selfCondition(key);
        if (cond != null) {
            return cond;
        }
        return super.condition(key);
    }

    @SuppressWarnings("unchecked")
    private <T> T selfCondition(Object key) {
        if (key == HugeKeys.OWNER_VERTEX) {
            return (T) this.ownerVertex;
        }
        if (key == HugeKeys.DIRECTION) {
            return (T) this.direction;
        }
        if (key == HugeKeys.LABEL) {
            if (this.edgeLabels.length == 0) {
                return null;
            }
            if (this.edgeLabels.length == 1) {
                return (T) this.edgeLabels[0];
            }
            E.checkState(false,
                         "Illegal key '%s' with more than one value", key);
        }
        return null;
    }

    private Condition.Relation selfRelation(Object key) {
        if (key == HugeKeys.OWNER_VERTEX) {
            return Condition.eq(HugeKeys.OWNER_VERTEX, this.ownerVertex);
        }
        if (key == HugeKeys.DIRECTION) {
            if (this.direction == Directions.BOTH) {
                return Condition.in(HugeKeys.DIRECTION, ImmutableList.of(
                                                        Directions.OUT,
                                                        Directions.IN));
            } else {
                return Condition.eq(HugeKeys.DIRECTION, this.direction);
            }
        }
        if (key == HugeKeys.LABEL) {
            if (this.edgeLabels.length == 0) {
                return null;
            }
            if (this.edgeLabels.length == 1) {
                return Condition.eq(HugeKeys.LABEL, this.edgeLabels[0]);
            }
            return Condition.in(HugeKeys.LABEL, Arrays.asList(this.edgeLabels));
        }
        return null;
    }

    @Override
    public boolean containsCondition(HugeKeys key) {
        if (key == HugeKeys.OWNER_VERTEX) {
            return true;
        }
        if (key == HugeKeys.DIRECTION) {
            return true;
        }
        if (key == HugeKeys.LABEL) {
            if (this.edgeLabels.length == 0) {
                return false;
            }
            return true;
        }
        return super.containsCondition(key);
    }

    @Override
    public void unsetCondition(Object key) {
        if (key == HugeKeys.OWNER_VERTEX ||
            key == HugeKeys.DIRECTION ||
            key == HugeKeys.LABEL) {
            E.checkArgument(false, "Can't unset condition '%s'", key);
        }
        super.unsetCondition(key);
    }

    @Override
    public List<Condition> syspropConditions() {
        List<Condition> conds = this.selfConditions();
        if (super.conditionsSize() > 0) {
            conds.addAll(super.syspropConditions());
        }
        return conds;
    }

    @Override
    public List<Condition> syspropConditions(HugeKeys key) {
        Condition cond = this.selfRelation(key);
        if (cond != null) {
            return ImmutableList.of(cond);
        }
        return super.syspropConditions(key);
    }

    @Override
    public List<Condition.Relation> relations() {
        // NOTE: selfConditions() actually return a list of Relation
        @SuppressWarnings({ "rawtypes", "unchecked" })
        List<Condition.Relation> relations = (List) this.selfConditions();
        if (super.conditionsSize() > 0) {
            relations.addAll(super.relations());
        }
        return relations;
    }

    @Override
    public Relation relation(Id key){
        Relation relation = this.selfRelation(key);
        if (relation != null) {
            return relation;
        }
        return super.relation(key);
    }

    @Override
    public boolean allSysprop() {
        return super.allSysprop();
    }

    @Override
    public boolean allRelation() {
        if (!this.isFlattened()) {
            return false;
        }
        return super.allRelation();
    }

    @Override
    public AdjacentEdgesQuery copy() {
        return (AdjacentEdgesQuery) super.copy();
    }

    @Override
    public AdjacentEdgesQuery copyAndResetUnshared() {
        return (AdjacentEdgesQuery) super.copyAndResetUnshared();
    }

    @Override
    public boolean isFlattened() {
        if (this.direction == Directions.BOTH) {
            return false;
        }
        if (this.edgeLabels.length > 1) {
            return false;
        }
        return super.isFlattened();
    }

    @Override
    public boolean canFlatten() {
        return super.conditionsSize() == 0;
    }

    @Override
    public List<ConditionQuery> flatten() {
        int labels = this.edgeLabels.length;
        int directions = this.direction == Directions.BOTH ? 2 : 1;
        int size = labels * directions;
        List<ConditionQuery> queries = new ArrayList<>(size);
        if (labels == 0) {
            Id label = null;
            if (this.direction == Directions.BOTH) {
                queries.add(this.copyQuery(Directions.OUT, label));
                queries.add(this.copyQuery(Directions.IN, label));
            } else {
                queries.add(this.copyQuery(this.direction, label));
            }
        } else {
            for (Id label : this.edgeLabels) {
                if (this.direction == Directions.BOTH) {
                    queries.add(this.copyQuery(Directions.OUT, label));
                    queries.add(this.copyQuery(Directions.IN, label));
                } else {
                    queries.add(this.copyQuery(this.direction, label));
                }
            }
        }
        E.checkState(super.conditionsSize() == 0,
                     "Can't flatten query: %s", this);
        return queries;
    }

    private AdjacentEdgesQuery copyQuery(Directions direction,
                                         Id edgeLabel) {
        AdjacentEdgesQuery query = this.copy();
        query.direction = direction;
        if (edgeLabel != null) {
            query.edgeLabels = new Id[]{edgeLabel};
        } else {
            query.edgeLabels = EMPTY;
        }
        query.selfConditions = null;
        return query;
    }

    @Override
    public boolean test(HugeElement element) {
        if (!super.test(element)) {
            return false;
        }
        HugeEdge edge = (HugeEdge) element;
        if (!edge.ownerVertex().id().equals(this.ownerVertex)) {
            return false;
        }
        if (!edge.matchDirection(this.direction)) {
            return false;
        }

        boolean matchedLabel = false;
        if (this.edgeLabels.length == 0) {
            matchedLabel = true;
        } else {
            for (Id label : this.edgeLabels) {
                if (edge.schemaLabel().id().equals(label)) {
                    matchedLabel = true;
                }
            }
        }
        return matchedLabel;
    }
}
