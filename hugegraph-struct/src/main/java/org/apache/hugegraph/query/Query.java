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

package org.apache.hugegraph.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Joiner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.collection.IdSet;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class Query implements Cloneable {

    private static final Logger LOG = Log.logger(Query.class);
    // TODO: we should better not use Long.Max as the unify limit number
    public static final long NO_LIMIT = Long.MAX_VALUE;

    public static final long COMMIT_BATCH = 500L;
    public static final long QUERY_BATCH = 100L;

    public static final long NO_CAPACITY = -1L;
    public static final long DEFAULT_CAPACITY = 800000L; // HugeGraph-777

    private static final ThreadLocal<Long> CAPACITY_CONTEXT = new ThreadLocal<>();

    protected static final Query NONE = new Query(HugeType.UNKNOWN);

    private static final Set<Id> EMPTY_OLAP_PKS = ImmutableSet.of();

    private HugeType resultType;
    private Map<HugeKeys, Order> orders;
    private long offset;
    private long actualOffset;
    private long actualStoreOffset;
    private long limit;
    private long skipDegree;
    private String page;
    private long capacity;
    private boolean showHidden;
    private boolean showDeleting;
    private boolean showExpired;
    private boolean olap;
    private boolean withProperties;
    private OrderType orderType;
    private Set<Id> olapPks;

    private List<Id> selects = InsertionOrderUtil.newList();

    @Deprecated
    private transient Aggregate aggregate;

    private Query originQuery;

    private List<Id> groups = InsertionOrderUtil.newList();
    private boolean groupByLabel = false;

    // V3.7 aggs
    private List<ImmutablePair<Id, AggType>> aggs =
            InsertionOrderUtil.newList();

    public Query() {

    }

    private static final ThreadLocal<Long> capacityContext = new ThreadLocal<>();

    private static int indexStringValueLength = 20;

    public Query(HugeType resultType) {
        this(resultType, null);
    }

    public Query(HugeType resultType, Query originQuery) {
        this.resultType = resultType;
        this.originQuery = originQuery;

        this.orders = null;

        this.offset = 0L;
        this.actualOffset = 0L;
        this.actualStoreOffset = 0L;
        this.limit = NO_LIMIT;
        this.skipDegree = NO_LIMIT;
        this.page = null;

        this.capacity = defaultCapacity();

        this.showHidden = false;
        this.showDeleting = false;

        this.withProperties = true;
        this.orderType = OrderType.ORDER_STRICT;

        this.aggregate = null;
        this.showExpired = false;
        this.olap = false;
        this.olapPks = EMPTY_OLAP_PKS;
    }

    public void copyBasic(Query query) {
        E.checkNotNull(query, "query");
        this.offset = query.offset();
        this.limit = query.limit();
        this.skipDegree = query.skipDegree();
        this.page = query.page();
        this.capacity = query.capacity();
        this.showHidden = query.showHidden();
        this.showDeleting = query.showDeleting();
        this.withProperties = query.withProperties();
        this.orderType = query.orderType();
        this.aggregate = query.aggregate();
        this.showExpired = query.showExpired();
        this.olap = query.olap();
        if (query.orders != null) {
            this.orders(query.orders);
        }
    }

    public HugeType resultType() {
        return this.resultType;
    }

    public void resultType(HugeType resultType) {
        this.resultType = resultType;
    }

    public Query originQuery() {
        return this.originQuery;
    }

    public void setOriginQuery(Query query) {
        this.originQuery = query;
    }

    public Query rootOriginQuery() {
        Query root = this;
        while (root.originQuery != null) {
            root = root.originQuery;
        }
        return root;
    }

    protected void originQuery(Query originQuery) {
        this.originQuery = originQuery;
    }

    public Map<HugeKeys, Order> orders() {
        return Collections.unmodifiableMap(this.getOrNewOrders());
    }

    public void orders(Map<HugeKeys, Order> orders) {
        this.orders = InsertionOrderUtil.newMap(orders);
    }

    public void order(HugeKeys key, Order order) {
        this.getOrNewOrders().put(key, order);
    }

    protected Map<HugeKeys, Order> getOrNewOrders() {
        if (this.orders != null) {
            return this.orders;
        }
        this.orders = InsertionOrderUtil.newMap();
        return this.orders;
    }

    public long offset() {
        return this.offset;
    }

    public void offset(long offset) {
        E.checkArgument(offset >= 0L, "Invalid offset %s", offset);
        this.offset = offset;
    }

    public void copyOffset(Query parent) {
        assert this.offset == 0L || this.offset == parent.offset;
        assert this.actualOffset == 0L ||
               this.actualOffset == parent.actualOffset;
        this.offset = parent.offset;
        this.actualOffset = parent.actualOffset;
    }

    public long actualOffset() {
        return this.actualOffset;
    }

    public void resetActualOffset() {
        this.actualOffset = 0L;
        this.actualStoreOffset = 0L;
    }

    public long goOffset(long offset) {
        E.checkArgument(offset >= 0L, "Invalid offset value: %s", offset);
        if (this.originQuery != null) {
            this.goParentOffset(offset);
        }
        return this.goSelfOffset(offset);
    }

    private void goParentOffset(long offset) {
        assert offset >= 0L;
        Query parent = this.originQuery;
        while (parent != null) {
            parent.actualOffset += offset;
            parent = parent.originQuery;
        }
    }

    private long goSelfOffset(long offset) {
        assert offset >= 0L;
        if (this.originQuery != null) {
            this.originQuery.goStoreOffsetBySubQuery(offset);
        }
        this.actualOffset += offset;
        return this.actualOffset;
    }

    private long goStoreOffsetBySubQuery(long offset) {
        Query parent = this.originQuery;
        while (parent != null) {
            parent.actualStoreOffset += offset;
            parent = parent.originQuery;
        }
        this.actualStoreOffset += offset;
        return this.actualStoreOffset;
    }

    public <T> Set<T> skipOffsetIfNeeded(Set<T> elems) {
        /*
         * Skip index(index query with offset) for performance optimization.
         * We assume one result is returned by each index, but if there are
         * overridden index it will cause confusing offset and results.
         */
        long fromIndex = this.offset() - this.actualOffset();
        if (fromIndex < 0L) {
            // Skipping offset is overhead, no need to skip
            fromIndex = 0L;
        } else if (fromIndex > 0L) {
            this.goOffset(fromIndex);
        }
        E.checkArgument(fromIndex <= Integer.MAX_VALUE,
                        "Offset must be <= 0x7fffffff, but got '%s'",
                        fromIndex);

        if (fromIndex >= elems.size()) {
            return ImmutableSet.of();
        }
        long toIndex = this.total();
        if (this.noLimit() || toIndex > elems.size()) {
            toIndex = elems.size();
        }
        if (fromIndex == 0L && toIndex == elems.size()) {
            return elems;
        }
        assert fromIndex < elems.size();
        assert toIndex <= elems.size();
        return CollectionUtil.subSet(elems, (int) fromIndex, (int) toIndex);
    }

    public long remaining() {
        if (this.limit == NO_LIMIT) {
            return NO_LIMIT;
        } else {
            return this.total() - this.actualOffset();
        }
    }

    public long total() {
        if (this.limit == NO_LIMIT) {
            return NO_LIMIT;
        } else {
            return this.offset + this.limit;
        }
    }

    public long limit() {
        if (this.capacity != NO_CAPACITY) {
            E.checkArgument(this.limit == Query.NO_LIMIT ||
                            this.limit <= this.capacity,
                            "Invalid limit %s, must be <= capacity(%s)",
                            this.limit, this.capacity);
        }
        return this.limit;
    }

    public void limit(long limit) {
        E.checkArgument(limit >= 0L || limit == NO_LIMIT,
                        "Invalid limit %s", limit);
        this.limit = limit;
    }

    public boolean noLimit() {
        return this.limit() == NO_LIMIT;
    }

    public boolean noLimitAndOffset() {
        return this.limit() == NO_LIMIT && this.offset() == 0L;
    }

    public boolean reachLimit(long count) {
        long limit = this.limit();
        if (limit == NO_LIMIT) {
            return false;
        }
        return count >= (limit + this.offset());
    }

    /**
     * Set or update the offset and limit by a range [start, end)
     * NOTE: it will use the min range one: max start and min end
     *
     * @param start the range start, include it
     * @param end   the range end, exclude it
     */
    public long range(long start, long end) {
        // Update offset
        long offset = this.offset();
        start = Math.max(start, offset);
        this.offset(start);

        // Update limit
        if (end != -1L) {
            if (!this.noLimit()) {
                end = Math.min(end, offset + this.limit());
            } else {
                assert end < Query.NO_LIMIT;
            }
            E.checkArgument(end >= start,
                            "Invalid range: [%s, %s)", start, end);
            this.limit(end - start);
        } else {
            // Keep the origin limit
            assert this.limit() <= Query.NO_LIMIT;
        }
        return this.limit;
    }

    public String page() {
        if (this.page != null) {
            E.checkState(this.limit() != 0L,
                         "Can't set limit=0 when using paging");
            E.checkState(this.offset() == 0L,
                         "Can't set offset when using paging, but got '%s'",
                         this.offset());
        }
        return this.page;
    }

    public String pageWithoutCheck() {
        return this.page;
    }

    public void page(String page) {
        this.page = page;
    }

    public boolean paging() {
        return this.page != null;
    }

    @Deprecated
    public void olap(boolean olap) {
        this.olap = olap;
    }

    @Deprecated
    public boolean olap() {
        return this.olap;
    }

    public void olapPks(Set<Id> olapPks) {
        for (Id olapPk : olapPks) {
            this.olapPk(olapPk);
        }
    }

    public void olapPk(Id olapPk) {
        if (this.olapPks == EMPTY_OLAP_PKS) {
            this.olapPks = new IdSet(CollectionType.EC);
        }
        this.olapPks.add(olapPk);
    }

    public Set<Id> olapPks() {
        return this.olapPks;
    }

    public long capacity() {
        return this.capacity;
    }

    public void capacity(long capacity) {
        this.capacity = capacity;
    }

    public boolean bigCapacity() {
        return this.capacity == NO_CAPACITY || this.capacity > DEFAULT_CAPACITY;
    }

    public void checkCapacity(long count) throws LimitExceedException {
        // Throw LimitExceedException if reach capacity
        if (this.capacity != Query.NO_CAPACITY && count > this.capacity) {
            final int MAX_CHARS = 256;
            String query = this.toString();
            if (query.length() > MAX_CHARS) {
                query = query.substring(0, MAX_CHARS) + "...";
            }
            throw new LimitExceedException(
                    "Too many records(must <= %s) for the query: %s",
                    this.capacity, query);
        }
    }

    public Aggregate aggregate() {
        return this.aggregate;
    }

    public Aggregate aggregateNotNull() {
        E.checkArgument(this.aggregate != null,
                        "The aggregate must be set for number query");
        return this.aggregate;
    }

    public void aggregate(AggregateFuncDefine func, String property) {
        this.aggregate = new Aggregate(func, property);
    }

    public void aggregate(Aggregate aggregate) {
        this.aggregate = aggregate;
    }

    public boolean showHidden() {
        return this.showHidden;
    }

    public void showHidden(boolean showHidden) {
        this.showHidden = showHidden;
    }

    public boolean showDeleting() {
        return this.showDeleting;
    }

    public void showDeleting(boolean showDeleting) {
        this.showDeleting = showDeleting;
    }

    public long skipDegree() {
        return this.skipDegree;
    }

    public void skipDegree(long skipDegree) {
        this.skipDegree = skipDegree;
    }

    public boolean withProperties() {
        return this.withProperties;
    }

    public void withProperties(boolean withProperties) {
        this.withProperties = withProperties;
    }

    public OrderType orderType() {
        return this.orderType;
    }

    public void orderType(OrderType orderType) {
        this.orderType = orderType;
    }

    public boolean showExpired() {
        return this.showExpired;
    }

    public void showExpired(boolean showExpired) {
        this.showExpired = showExpired;
    }

    public Collection<Id> ids() {
        return ImmutableList.of();
    }

    public Collection<Condition> conditions() {
        return ImmutableList.of();
    }

    public int idsSize() {
        return 0;
    }

    public int conditionsSize() {
        return 0;
    }

    public boolean empty() {
        return this.idsSize() == 0 && this.conditionsSize() == 0;
    }

    public boolean test(BaseElement element) {
        return true;
    }

    public Query copy() {
        try {
            return (Query) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Query)) {
            return false;
        }
        Query other = (Query) object;
        return this.resultType.equals(other.resultType) &&
               this.orders().equals(other.orders()) &&
               this.offset == other.offset &&
               this.limit == other.limit &&
               Objects.equals(this.page, other.page) &&
               this.ids().equals(other.ids()) &&
               this.conditions().equals(other.conditions()) &&
               this.withProperties == other.withProperties;
    }

    @Override
    public int hashCode() {
        int hash = this.orders().hashCode() ^
                   Long.hashCode(this.offset) ^
                   Long.hashCode(this.limit) ^
                   Objects.hashCode(this.page) ^
                   this.ids().hashCode() ^
                   this.conditions().hashCode() ^
                   this.selects().hashCode() ^
                   Boolean.hashCode(this.withProperties);
        if (this.resultType == null) {
            return hash;
        } else {
            return this.resultType.hashCode() ^ hash;
        }
    }

    @Override
    public String toString() {
        Map<String, Object> pairs = InsertionOrderUtil.newMap();
        if (this.page != null) {
            pairs.put("page", String.format("'%s'", this.page));
        }
        if (this.offset != 0) {
            pairs.put("offset", this.offset);
        }
        if (this.limit != NO_LIMIT) {
            pairs.put("limit", this.limit);
        }
        if (!this.orders().isEmpty()) {
            pairs.put("order by", this.orders());
        }

        StringBuilder sb = new StringBuilder(128);
        sb.append("`Query ");
        if (this.aggregate != null) {
            sb.append(this.aggregate);
        } else {
            sb.append('*');
        }
        sb.append(" from ").append(this.resultType);
        for (Map.Entry<String, Object> entry : pairs.entrySet()) {
            sb.append(' ').append(entry.getKey())
              .append(' ').append(entry.getValue()).append(',');
        }
        if (!pairs.isEmpty()) {
            // Delete last comma
            sb.deleteCharAt(sb.length() - 1);
        }

        if (!this.empty()) {
            sb.append(" where");
        }

        // Append ids
        if (!this.ids().isEmpty()) {
            sb.append(" id in ").append(this.ids());
        }

        // Append conditions
        if (!this.conditions().isEmpty()) {
            if (!this.ids().isEmpty()) {
                sb.append(" and");
            }
            sb.append(" ").append(this.conditions());
        }

        if (!this.groups.isEmpty()) {
            sb.append(" group by ").append(Joiner.on(",").join(this.groups));
        }

        sb.append('`');
        return sb.toString();
    }

    public static long defaultCapacity(long capacity) {
        Long old = CAPACITY_CONTEXT.get();
        CAPACITY_CONTEXT.set(capacity);
        return old != null ? old : DEFAULT_CAPACITY;
    }

    public static long defaultCapacity() {
        Long capacity = CAPACITY_CONTEXT.get();
        return capacity != null ? capacity : DEFAULT_CAPACITY;
    }

    public static void checkForceCapacity(long count)
            throws LimitExceedException {
        if (count > DEFAULT_CAPACITY) {
            throw new LimitExceedException(
                    "Too many records(must <= %s) for one query",
                    DEFAULT_CAPACITY);
        }
    }

    public boolean isTaskQuery() {
        if (this.resultType() == HugeType.TASK ||
            this.resultType == HugeType.VARIABLE) {
            return true;
        }

        return false;
    }

    public static int getIndexStringValueLength() {
        return indexStringValueLength;
    }

    public static void setIndexStringValueLength(int indexStringValueLengthTmp) {
        if (indexStringValueLengthTmp <= 1) {
            indexStringValueLengthTmp = 20;
        }
        indexStringValueLength = indexStringValueLengthTmp;
    }

    public void select(Id id) {
        if (!this.selects.contains(id)) {
            this.selects.add(id);
        } else {
            LOG.warn("id already in selects: {}", id);
        }
    }

    public List<Id> selects() {
        return this.selects;
    }

    public void group(Id id) {
        if (!this.groups.contains(id)) {
            this.groups.add(id);
        } else {
            LOG.warn("id already in groups: {}", id);
        }
    }

    public enum OrderType {
        // Under batch interface, the requirement for return order
        ORDER_NONE,           // Allow unordered
        ORDER_WITHIN_VERTEX,  // Edges within a vertex will not be broken, but there is no order between different vertices.
        ORDER_STRICT          // Ensure the original input point order
    }

    public enum Order {
        ASC,
        DESC
    }

    public enum AggType {
        COUNT,
        MAX,
        MIN,
        AVG,
        SUM;
    }


}
