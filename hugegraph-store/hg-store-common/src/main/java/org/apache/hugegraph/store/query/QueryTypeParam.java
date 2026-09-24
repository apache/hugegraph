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

import java.util.Arrays;

import lombok.Data;

/**
 * primary index scan:
 * range scan: start + end
 * id scan: start + isPrefix (false)
 * prefix scan: start + isPrefix (true)
 * <p>
 * secondary index scan:
 * default range: start + end + isSecondaryIndex (true)
 */
//FIXME A QueryParam should not be modified after its creation
@Data
public class QueryTypeParam {

    //FIXME May be modified by setter
    public static final QueryTypeParam EMPTY = new QueryTypeParam();
    /**
     * id scan, the hash code of the key.
     * this code would be calculated by KeyUtil.getOwnerKey
     * default : -1, scan all partitions. if set, would affect scan partitions of prefix scan and
     * range scan.
     */
    int code = -1;
    /**
     * range scan - prefix start, prefix scan, id scan
     * class: org.apache.hugegraph.id.Id
     */
    private byte[] start;
    /**
     * range scan - prefix end,  prefix scan (null)
     * class: org.apache.hugegraph.id.Id
     */
    private byte[] end;
    /**
     * the boundary of range/prefix scan (gt/lt/eq/gte/lte)
     */
    private int boundary = 0;
    /**
     * whether the start key is id or prefix
     */
    private boolean isPrefix = false;
    /**
     * whether lookup index table (g+index)
     */
    private boolean isSecondaryIndex = false;
    /**
     * todo: When deserializing from index to ID, used to check the prefix of id.asBytes()
     */
    private byte[] idPrefix;

    private QueryTypeParam() {

    }

    public QueryTypeParam(byte[] start, byte[] end, int boundary, boolean isPrefix,
                          boolean isSecondaryIndex, int code) {
        this.start = start;
        this.end = end;
        this.boundary = boundary;
        this.isPrefix = isPrefix;
        this.isSecondaryIndex = isSecondaryIndex;
        this.code = code;
    }

    public QueryTypeParam(byte[] start, byte[] end, int boundary, boolean isPrefix,
                          boolean isSecondaryIndex,
                          int code, byte[] idPrefix) {
        this.start = start;
        this.end = end;
        this.boundary = boundary;
        this.isPrefix = isPrefix;
        this.isSecondaryIndex = isSecondaryIndex;
        this.code = code;
        this.idPrefix = idPrefix;
    }

    @Deprecated
    public static QueryTypeParam ofIdScanParam(byte[] start) {
        assert (start != null);
        return new QueryTypeParam(start, null, 0, false, false, -1);
    }

    /**
     * primary : id scan
     *
     * @param start id key
     * @param code  owner code
     * @return param
     */
    public static QueryTypeParam ofIdScanParam(byte[] start, int code) {
        assert (start != null);
        return new QueryTypeParam(start, null, 0, false, false, code);
    }

    /**
     * primary : prefix scan
     *
     * @param start    prefix
     * @param boundary boundary
     * @return param
     */
    public static QueryTypeParam ofPrefixScanParam(byte[] start, int boundary) {
        assert (start != null);
        return new QueryTypeParam(start, null, boundary, true, false, -1);
    }

    /**
     * primary : prefix scan
     *
     * @param start    prefix
     * @param boundary boundary
     * @param code     used for specify partition
     * @return param
     */
    public static QueryTypeParam ofPrefixScanParam(byte[] start, int boundary, int code) {
        assert (start != null);
        return new QueryTypeParam(start, null, boundary, true, false, code);
    }

    /**
     * primary : range scan
     *
     * @param start    start key
     * @param end      end key
     * @param boundary boundary
     * @return param
     */
    public static QueryTypeParam ofRangeScanParam(byte[] start, byte[] end, int boundary) {
        assert (start != null && end != null);
        return new QueryTypeParam(start, end, boundary, false, false, -1);
    }

    /**
     * primary : range scan
     *
     * @param start    start key
     * @param end      end key
     * @param boundary boundary
     * @param code     use for specify partition
     * @return param
     */
    public static QueryTypeParam ofRangeScanParam(byte[] start, byte[] end, int boundary,
                                                  int code) {
        assert (start != null && end != null);
        return new QueryTypeParam(start, end, boundary, false, false, code);
    }

    /**
     * index scan: range scan
     *
     * @param start    range start
     * @param end      range end
     * @param boundary boundary
     * @return param
     */
    public static QueryTypeParam ofIndexScanParam(byte[] start, byte[] end, int boundary) {
        return new QueryTypeParam(start, end, boundary, false, true, -1);
    }

    /**
     * index scan: range scan with id prefix check
     *
     * @param start    range start
     * @param end      range end
     * @param boundary boundary
     * @param idPrefix id prefix
     * @return param
     */
    public static QueryTypeParam ofIndexScanParam(byte[] start, byte[] end, int boundary,
                                                  byte[] idPrefix) {
        return new QueryTypeParam(start, end, boundary, false, true, -1, idPrefix);
    }

    /**
     * index scan : prefix
     *
     * @param start    prefix
     * @param boundary boundary
     * @return param
     */
    public static QueryTypeParam ofIndexScanParam(byte[] start, int boundary) {
        return new QueryTypeParam(start, null, boundary, true, true, -1);
    }

    /**
     * index scan : prefix with id prefix check
     *
     * @param start    prefix
     * @param boundary boundary
     * @param idPrefix idPrefix
     * @return param
     */
    public static QueryTypeParam ofIndexScanParam(byte[] start, int boundary, byte[] idPrefix) {
        return new QueryTypeParam(start, null, boundary, true, true, -1, idPrefix);
    }

    public byte[] getIdPrefix() {
        return idPrefix;
    }

    public void setIdPrefix(byte[] idPrefix) {
        this.idPrefix = idPrefix;
    }

    public boolean isIdScan() {
        return !isPrefix && start != null && start.length > 0 && (end == null || end.length == 0) &&
               !isSecondaryIndex;
    }

    public boolean isRangeScan() {
        return !isPrefix && start != null && start.length > 0 && end != null && end.length > 0 &&
               !isSecondaryIndex;
    }

    public boolean isPrefixScan() {
        return isPrefix && start != null && start.length > 0 && (end == null || end.length == 0) &&
               !isSecondaryIndex;
    }

    public boolean isIndexScan() {
        return isRangeIndexScan() || isPrefixIndexScan();
    }

    public boolean isRangeIndexScan() {
        return isSecondaryIndex && !isPrefix && start != null && start.length > 0 && end != null &&
               end.length > 0;
    }

    public boolean isPrefixIndexScan() {
        return isSecondaryIndex && isPrefix && start != null && start.length > 0;
    }

    @Override
    public String toString() {
        return "QueryTypeParam{" +
               (isSecondaryIndex ? "[S - " : "[P - ") +
               (end != null ? "Range]" : (isPrefix ? "Prefix]" : "ID]")) +
               " start=" + Arrays.toString(start) +
               (end != null ? ", end=" + Arrays.toString(end) : "") +
               ", boundary=" + boundary +
               (isIdScan() ? ", code=" + code : "") +
               (idPrefix != null ? ", idPrefix=" + Arrays.toString(idPrefix) : "") +
               '}';
    }
}
