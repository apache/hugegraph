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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.NumericUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public abstract class Condition {

    public enum ConditionType {
        NONE,
        RELATION,
        AND,
        OR,
        NOT
    }

    public enum RelationType implements BiPredicate<Object, Object> {

        EQ("==", RelationType::equals),

        GT(">", (v1, v2) -> {
            return compare(v1, v2) > 0;
        }),

        GTE(">=", (v1, v2) -> {
            return compare(v1, v2) >= 0;
        }),

        LT("<", (v1, v2) -> {
            return compare(v1, v2) < 0;
        }),

        LTE("<=", (v1, v2) -> {
            return compare(v1, v2) <= 0;
        }),

        NEQ("!=", (v1, v2) -> {
            return compare(v1, v2) != 0;
        }),

        IN("in", null, Collection.class, (v1, v2) -> {
            assert v2 != null;
            return ((Collection<?>) v2).contains(v1);
        }),

        NOT_IN("notin", null, Collection.class, (v1, v2) -> {
            assert v2 != null;
            return !((Collection<?>) v2).contains(v1);
        }),

        PREFIX("prefix", Id.class, Id.class, (v1, v2) -> {
            assert v2 != null;
            return v1 != null && Bytes.prefixWith(((Id) v2).asBytes(),
                                                  ((Id) v1).asBytes());
        }),

        TEXT_CONTAINS("textcontains", String.class, String.class, (v1, v2) -> {
            // TODO: support collection-property textcontains
            return v1 != null && ((String) v1).contains((String) v2);
        }),

        TEXT_CONTAINS_ANY("textcontainsany", String.class, Collection.class, (v1, v2) -> {
            assert v2 != null;
            if (v1 == null) {
                return false;
            }

            @SuppressWarnings("unchecked")
            Collection<String> words = (Collection<String>) v2;

            for (String word : words) {
                if (((String) v1).contains(word)) {
                    return true;
                }
            }
            return false;
        }),

        CONTAINS("contains", Collection.class, null, (v1, v2) -> {
            assert v2 != null;
            return v1 != null && ((Collection<?>) v1).contains(v2);
        }),

        CONTAINS_VALUE("containsv", Map.class, null, (v1, v2) -> {
            assert v2 != null;
            return v1 != null && ((Map<?, ?>) v1).containsValue(v2);
        }),

        CONTAINS_KEY("containsk", Map.class, null, (v1, v2) -> {
            assert v2 != null;
            return v1 != null && ((Map<?, ?>) v1).containsKey(v2);
        }),

        SCAN("scan", (v1, v2) -> {
            assert v2 != null;
            /*
             * TODO: we still have no way to determine accurately, since
             *       some backends may scan with token(column) like cassandra.
             */
            return true;
        });

        private static final Set<RelationType> RANGE_TYPES =
                ImmutableSet.of(GT, GTE, LT, LTE);

        private static final Set<RelationType> UNFLATTEN_TYPES =
                ImmutableSet.of(IN, NOT_IN, TEXT_CONTAINS_ANY);

        private final String operator;
        private final BiFunction<Object, Object, Boolean> tester;
        private final Class<?> v1Class;
        private final Class<?> v2Class;

        RelationType(String op,
                     BiFunction<Object, Object, Boolean> tester) {
            this(op, null, null, tester);
        }

        RelationType(String op, Class<?> v1Class, Class<?> v2Class,
                     BiFunction<Object, Object, Boolean> tester) {
            this.operator = op;
            this.tester = tester;
            this.v1Class = v1Class;
            this.v2Class = v2Class;
        }

        public String string() {
            return this.operator;
        }

        /**
         * Determine two values of any type equal
         *
         * @param first  is actual value
         * @param second is value in query condition
         * @return true if equal, otherwise false
         */
        private static boolean equals(final Object first,
                                      final Object second) {
            assert second != null;
            if (first instanceof Id) {
                if (second instanceof String) {
                    return second.equals(((Id) first).asString());
                } else if (second instanceof Long) {
                    return second.equals(((Id) first).asLong());
                }
            } else if (second instanceof Number) {
                return compare(first, second) == 0;
            } else if (second.getClass().isArray()) {
                return ArrayUtils.isEquals(first, second);
            }

            return Objects.equals(first, second);
        }

        /**
         * Determine two numbers equal
         *
         * @param first  is actual value, might be Number/Date or String, It is
         *               probably that the `first` is serialized to String.
         * @param second is value in query condition, must be Number/Date/Boolean
         * @return the value 0 if first is numerically equal to second;
         * a value less than 0 if first is numerically less than
         * second; and a value greater than 0 if first is
         * numerically greater than second.
         */
        private static int compare(final Object first, final Object second) {
            assert second != null;
            if (second instanceof Number) {
                return NumericUtil.compareNumber(first == null ? 0 : first,
                                                 (Number) second);
            } else if (second instanceof Date) {
                return compareDate(first, (Date) second);
            } else if (second instanceof Boolean) {
                return compareBoolean(first, (Boolean) second);
            }

            throw new IllegalArgumentException(String.format(
                    "Can't compare between %s(%s) and %s(%s)", first,
                    first == null ? null : first.getClass().getSimpleName(),
                    second, second.getClass().getSimpleName()));
        }

        private static int compareDate(Object first, Date second) {
            if (first == null) {
                first = DateUtil.DATE_ZERO;
            }
            if (first instanceof Date) {
                return ((Date) first).compareTo(second);
            }

            throw new IllegalArgumentException(String.format(
                    "Can't compare between %s(%s) and %s(%s)",
                    first, first.getClass().getSimpleName(),
                    second, second.getClass().getSimpleName()));
        }

        private static int compareBoolean(Object first, Boolean second) {
            if (first instanceof Boolean) {
                return Boolean.compare((Boolean) first, second);
            }

            throw new IllegalArgumentException(String.format(
                    "Can't compare between %s(%s) and %s(%s)",
                    first, first == null ? null :
                           first.getClass().getSimpleName(),
                    second, second.getClass().getSimpleName()));
        }

        private void checkBaseType(Object value, Class<?> clazz) {
            if (!clazz.isInstance(value)) {
                String valueClass = value == null ? "null" :
                                    value.getClass().getSimpleName();
                E.checkArgument(false,
                                "Can't execute `%s` on type %s, expect %s",
                                this.operator, valueClass,
                                clazz.getSimpleName());
            }
        }

        private void checkValueType(Object value, Class<?> clazz) {
            if (!clazz.isInstance(value)) {
                String valueClass = value == null ? "null" :
                                    value.getClass().getSimpleName();
                E.checkArgument(false,
                                "Can't test '%s'(%s) for `%s`, expect %s",
                                value, valueClass, this.operator,
                                clazz.getSimpleName());
            }
        }

        @Override
        public boolean test(Object first, Object second) {
            E.checkState(this.tester != null, "Can't test %s", this.name());
            E.checkArgument(second != null,
                            "Can't test null value for `%s`", this.operator);
            if (this.v1Class != null) {
                this.checkBaseType(first, this.v1Class);
            }
            if (this.v2Class != null) {
                this.checkValueType(second, this.v2Class);
            }
            return this.tester.apply(first, second);
        }

        public boolean isRangeType() {
            return RANGE_TYPES.contains(this);
        }

        public boolean isSearchType() {
            return this == TEXT_CONTAINS || this == TEXT_CONTAINS_ANY;
        }

        public boolean isSecondaryType() {
            return this == EQ;
        }
    }

    public abstract ConditionType type();

    public abstract boolean isSysprop();

    public abstract List<? extends Relation> relations();

    public abstract boolean test(Object value);

    public abstract boolean test(HugeElement element);

    public abstract Condition copy();

    public abstract Condition replace(Relation from, Relation to);

    public Condition and(Condition other) {
        return new And(this, other);
    }

    public Condition or(Condition other) {
        return new Or(this, other);
    }

    public boolean isRelation() {
        return this.type() == ConditionType.RELATION;
    }

    public boolean isLogic() {
        return this.type() == ConditionType.AND ||
               this.type() == ConditionType.OR ||
               this.type() == ConditionType.NOT;
    }

    public boolean isFlattened() {
        return this.isRelation();
    }

    public static Condition and(Condition left, Condition right) {
        return new And(left, right);
    }

    public static Condition or(Condition left, Condition right) {
        return new Or(left, right);
    }

    public static Condition not(Condition condition) {
        return new Not(condition);
    }

    public static Relation eq(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.EQ, value);
    }

    public static Relation gt(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.GT, value);
    }

    public static Relation gte(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.GTE, value);
    }

    public static Relation lt(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.LT, value);
    }

    public static Relation lte(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.LTE, value);
    }

    public static Relation neq(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.NEQ, value);
    }

    public static Condition in(HugeKeys key, List<?> value) {
        return new SyspropRelation(key, RelationType.IN, value);
    }

    public static Condition nin(HugeKeys key, List<?> value) {
        return new SyspropRelation(key, RelationType.NOT_IN, value);
    }

    public static Condition prefix(HugeKeys key, Id value) {
        return new SyspropRelation(key, RelationType.PREFIX, value);
    }

    public static Condition containsValue(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.CONTAINS_VALUE, value);
    }

    public static Condition containsKey(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.CONTAINS_KEY, value);
    }

    public static Condition contains(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.CONTAINS, value);
    }

    public static Condition scan(String start, String end) {
        Shard value = new Shard(start, end, 0);
        return new SyspropRelation(HugeKeys.ID, RelationType.SCAN, value);
    }

    public static Relation eq(Id key, Object value) {
        return new UserpropRelation(key, RelationType.EQ, value);
    }

    public static Relation gt(Id key, Object value) {
        return new UserpropRelation(key, RelationType.GT, value);
    }

    public static Relation gte(Id key, Object value) {
        return new UserpropRelation(key, RelationType.GTE, value);
    }

    public static Relation lt(Id key, Object value) {
        return new UserpropRelation(key, RelationType.LT, value);
    }

    public static Relation lte(Id key, Object value) {
        return new UserpropRelation(key, RelationType.LTE, value);
    }

    public static Relation neq(Id key, Object value) {
        return new UserpropRelation(key, RelationType.NEQ, value);
    }

    public static Relation in(Id key, List<?> value) {
        return new UserpropRelation(key, RelationType.IN, value);
    }

    public static Relation nin(Id key, List<?> value) {
        return new UserpropRelation(key, RelationType.NOT_IN, value);
    }

    public static Relation textContains(Id key, String word) {
        return new UserpropRelation(key, RelationType.TEXT_CONTAINS, word);
    }

    public static Relation textContainsAny(Id key, Set<String> words) {
        return new UserpropRelation(key, RelationType.TEXT_CONTAINS_ANY, words);
    }

    public static Condition contains(Id key, Object value) {
        return new UserpropRelation(key, RelationType.CONTAINS, value);
    }

    /**
     * Condition defines
     */
    public abstract static class BinCondition extends Condition {

        private Condition left;
        private Condition right;

        public BinCondition(Condition left, Condition right) {
            E.checkNotNull(left, "left condition");
            E.checkNotNull(right, "right condition");
            this.left = left;
            this.right = right;
        }

        public Condition left() {
            return this.left;
        }

        public Condition right() {
            return this.right;
        }

        @Override
        public boolean isSysprop() {
            return this.left.isSysprop() && this.right.isSysprop();
        }

        @Override
        public List<? extends Relation> relations() {
            List<Relation> list = new ArrayList<>(this.left.relations());
            list.addAll(this.right.relations());
            return list;
        }

        @Override
        public Condition replace(Relation from, Relation to) {
            this.left = this.left.replace(from, to);
            this.right = this.right.replace(from, to);
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(64);
            sb.append(this.left).append(' ');
            sb.append(this.type().name()).append(' ');
            sb.append(this.right);
            return sb.toString();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof BinCondition)) {
                return false;
            }
            BinCondition other = (BinCondition) object;
            return this.type().equals(other.type()) &&
                   this.left().equals(other.left()) &&
                   this.right().equals(other.right());
        }

        @Override
        public int hashCode() {
            return this.type().hashCode() ^
                   this.left().hashCode() ^
                   this.right().hashCode();
        }
    }

    public static class And extends BinCondition {

        public And(Condition left, Condition right) {
            super(left, right);
        }

        @Override
        public ConditionType type() {
            return ConditionType.AND;
        }

        @Override
        public boolean test(Object value) {
            return this.left().test(value) && this.right().test(value);
        }

        @Override
        public boolean test(HugeElement element) {
            return this.left().test(element) && this.right().test(element);
        }

        @Override
        public Condition copy() {
            return new And(this.left().copy(), this.right().copy());
        }
    }

    public static class Or extends BinCondition {

        public Or(Condition left, Condition right) {
            super(left, right);
        }

        @Override
        public ConditionType type() {
            return ConditionType.OR;
        }

        @Override
        public boolean test(Object value) {
            return this.left().test(value) || this.right().test(value);
        }

        @Override
        public boolean test(HugeElement element) {
            return this.left().test(element) || this.right().test(element);
        }

        @Override
        public Condition copy() {
            return new Or(this.left().copy(), this.right().copy());
        }
    }

    public static class Not extends Condition {

        Condition condition;

        public Not(Condition condition) {
            super();
            this.condition = condition;
        }

        public Condition condition() {
            return condition;
        }

        @Override
        public ConditionType type() {
            return ConditionType.NOT;
        }

        @Override
        public boolean test(Object value) {
            return !this.condition.test(value);
        }

        @Override
        public boolean test(HugeElement element) {
            return !this.condition.test(element);
        }

        @Override
        public Condition copy() {
            return new Not(this.condition.copy());
        }

        @Override
        public boolean isSysprop() {
            return this.condition.isSysprop();
        }

        @Override
        public List<? extends Relation> relations() {
            return new ArrayList<Relation>(this.condition.relations());
        }

        @Override
        public Condition replace(Relation from, Relation to) {
            this.condition = this.condition.replace(from, to);
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(64);
            sb.append(this.type().name()).append(' ');
            sb.append(this.condition);
            return sb.toString();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Not)) {
                return false;
            }
            Not other = (Not) object;
            return this.type().equals(other.type()) &&
                   this.condition.equals(other.condition());
        }

        @Override
        public int hashCode() {
            return this.type().hashCode() ^
                   this.condition.hashCode();
        }
    }

    public abstract static class Relation extends Condition {

        // Relational operator (like: =, >, <, in, ...)
        protected RelationType relation;
        // Single-type value or a list of single-type value
        protected Object value;

        // The key serialized(code/string) by backend store.
        protected Object serialKey;
        // The value serialized(code/string) by backend store.
        protected Object serialValue;

        @Override
        public ConditionType type() {
            return ConditionType.RELATION;
        }

        public RelationType relation() {
            return this.relation;
        }

        public Object value() {
            return this.value;
        }

        public void value(Object value) {
            this.value = value;
        }

        public void serialKey(Object key) {
            this.serialKey = key;
        }

        public Object serialKey() {
            return this.serialKey != null ? this.serialKey : this.key();
        }

        public void serialValue(Object value) {
            this.serialValue = value;
        }

        public Object serialValue() {
            return this.serialValue != null ? this.serialValue : this.value();
        }

        @Override
        public boolean test(Object value) {
            return this.relation.test(value, this.value());
        }

        @Override
        public boolean isFlattened() {
            return !RelationType.UNFLATTEN_TYPES.contains(this.relation);
        }

        @Override
        public List<? extends Relation> relations() {
            return ImmutableList.of(this);
        }

        @Override
        public Condition replace(Relation from, Relation to) {
            if (this == from) {
                return to;
            } else {
                return this;
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(64);
            sb.append(this.key()).append(' ');
            sb.append(this.relation.string()).append(' ');
            sb.append(this.value);
            return sb.toString();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Relation)) {
                return false;
            }
            Relation other = (Relation) object;
            return this.relation().equals(other.relation()) &&
                   this.key().equals(other.key()) &&
                   this.value().equals(other.value());
        }

        @Override
        public int hashCode() {
            return this.type().hashCode() ^
                   this.relation().hashCode() ^
                   this.key().hashCode() ^
                   this.value().hashCode();
        }

        @Override
        public abstract boolean isSysprop();

        public abstract Object key();

        @Override
        public abstract Relation copy();
    }

    public static class SyspropRelation extends Relation {

        private final HugeKeys key;

        public SyspropRelation(HugeKeys key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public SyspropRelation(HugeKeys key, RelationType op, Object value) {
            E.checkNotNull(op, "relation type");
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public HugeKeys key() {
            return this.key;
        }

        @Override
        public boolean isSysprop() {
            return true;
        }

        @Override
        public boolean test(HugeElement element) {
            E.checkNotNull(element, "element");
            Object value = element.sysprop(this.key);
            return this.relation.test(value, this.value());
        }

        @Override
        public Relation copy() {
            Relation clone = new SyspropRelation(this.key, this.relation(),
                                                 this.value);
            clone.serialKey(this.serialKey);
            clone.serialValue(this.serialValue);
            return clone;
        }
    }

    public static class FlattenSyspropRelation extends SyspropRelation {

        public FlattenSyspropRelation(SyspropRelation relation) {
            super(relation.key(), relation.relation(), relation.value());
        }

        @Override
        public boolean isFlattened() {
            return true;
        }
    }

    public static class UserpropRelation extends Relation {

        // Id of property key
        private final Id key;

        public UserpropRelation(Id key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public UserpropRelation(Id key, RelationType op, Object value) {
            E.checkNotNull(op, "relation type");
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public Id key() {
            return this.key;
        }

        @Override
        public boolean isSysprop() {
            return false;
        }

        @Override
        public boolean test(HugeElement element) {
            HugeProperty<?> prop = element.getProperty(this.key);
            Object value = prop != null ? prop.value() : null;
            if (value == null) {
                /*
                 * Fix #611
                 * TODO: It's possible some scenes can't be returned false
                 * directly, such as: EQ with p1 == null, it should be returned
                 * true, but the query has(p, null) is not allowed by
                 * TraversalUtil.validPredicateValue().
                 */
                return false;
            }
            return this.relation.test(value, this.value());
        }

        @Override
        public Relation copy() {
            Relation clone = new UserpropRelation(this.key, this.relation(),
                                                  this.value);
            clone.serialKey(this.serialKey);
            clone.serialValue(this.serialValue);
            return clone;
        }
    }

    public static class RangeConditions {

        private Object keyEq = null;
        private Object keyMin = null;
        private boolean keyMinEq = false;
        private Object keyMax = null;
        private boolean keyMaxEq = false;

        public RangeConditions(List<? extends Condition> conditions) {
            for (Condition c : conditions) {
                Relation r = (Relation) c;
                switch (r.relation()) {
                    case EQ:
                        this.keyEq = r.value();
                        break;
                    case GTE:
                        this.keyMinEq = true;
                        this.keyMin = r.value();
                        break;
                    case GT:
                        this.keyMin = r.value();
                        break;
                    case LTE:
                        this.keyMaxEq = true;
                        this.keyMax = r.value();
                        break;
                    case LT:
                        this.keyMax = r.value();
                        break;
                    default:
                        E.checkArgument(false, "Unsupported relation '%s'",
                                        r.relation());
                }
            }
        }

        public Object keyEq() {
            return this.keyEq;
        }

        public Object keyMin() {
            return this.keyMin;
        }

        public Object keyMax() {
            return this.keyMax;
        }

        public boolean keyMinEq() {
            return this.keyMinEq;
        }

        public boolean keyMaxEq() {
            return this.keyMaxEq;
        }

        public boolean hasRange() {
            return this.keyMin != null || this.keyMax != null;
        }
    }
}
