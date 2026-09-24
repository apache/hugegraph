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

package org.apache.hugegraph.structure.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.analyzer.Analyzer;
import org.apache.hugegraph.analyzer.AnalyzerFactory;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseProperty;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.NumericUtil;
import org.slf4j.Logger;

public class IndexBuilder {
    private static final Logger LOG = Log.logger(IndexBuilder.class);

    private final HugeGraphSupplier graph;
    private final Analyzer textAnalyzer;


    public static final String INDEX_SYM_NULL = "\u0001";
    public static final String INDEX_SYM_EMPTY = "\u0002";
    public static final char INDEX_SYM_MAX = '\u0003';

    private static final String TEXT_ANALYZER = "search.text_analyzer";
    private static final String TEXT_ANALYZER_MODE =
            "search.text_analyzer_mode";

    private static final String DEFAULT_TEXT_ANALYZER = "ikanalyzer";
    private static final String DEFAULT_TEXT_ANALYZER_MODE = "smart";

    public IndexBuilder(HugeGraphSupplier graph) {
        this.graph = graph;

        String name = graph.configuration().get(String.class, TEXT_ANALYZER);
        String mode = graph.configuration().get(String.class,
                                                TEXT_ANALYZER_MODE);

        name = name == null ? DEFAULT_TEXT_ANALYZER : name;
        mode = mode == null ? DEFAULT_TEXT_ANALYZER_MODE : mode;

        LOG.debug("Loading text analyzer '{}' with mode '{}' for graph '{}'",
                  name, mode, graph.name());
        this.textAnalyzer = AnalyzerFactory.analyzer(name, mode);
    }

    public List<Index> buildLabelIndex(BaseElement element) {

        List<Index> indexList = new ArrayList<Index>();
        // Don't Build label index if it's not enabled
        SchemaLabel label = element.schemaLabel();

        // Build label index if backend store not supports label-query
        Index index = new Index(graph,
                                IndexLabel.label(element.type()),
                                true);
        index.fieldValues(element.schemaLabel().id());
        index.elementIds(element.id(), element.expiredTime());

        indexList.add(index);

        /**When adding a sub-type edge, put its edgeID into the parent type's edgeLabelIndex at the same time
         * to support: g.E().hasLabel("parent type")
         * */
        if (element instanceof BaseEdge && ((EdgeLabel) label).hasFather()) {
            Index fatherIndex = new Index(graph,
                                          IndexLabel.label(element.type()));
            fatherIndex.fieldValues(((EdgeLabel) label).fatherId());
            fatherIndex.elementIds(element.id(), element.expiredTime());

            indexList.add(fatherIndex);
        }

        return indexList;
    }

    public List<Index> buildVertexOlapIndex(BaseVertex vertex) {

        List<Index> indexs = new ArrayList<>();

        Id pkId = vertex.getProperties().keySet().iterator().next();
        Collection<IndexLabel> indexLabels = graph.indexLabels();
        for (IndexLabel il : indexLabels) {
            if (il.indexFields().contains(pkId)) {
                indexs.addAll(this.buildIndex(vertex, il));
            }
        }

        return indexs;
    }

    public List<Index> buildVertexIndex(BaseVertex vertex) {
        List<Index> indexs = new ArrayList<>();

        VertexLabel label = vertex.schemaLabel();

        if (label.enableLabelIndex()) {
            indexs.addAll(this.buildLabelIndex(vertex));
        }

        for (Id il : label.indexLabels()) {
            indexs.addAll(this.buildIndex(vertex,  graph.indexLabel(il)));
        }

        return indexs;
    }

    public List<Index> buildEdgeIndex(BaseEdge edge) {
        List<Index> indexs = new ArrayList<>();

        EdgeLabel label = edge.schemaLabel();

        if (label.enableLabelIndex()) {
            indexs.addAll(this.buildLabelIndex(edge));
        }


        for (Id il : label.indexLabels()) {
            indexs.addAll(this.buildIndex(edge, graph.indexLabel(il)));
        }

        return indexs;
    }

    /**
     * Build index(user properties) of vertex or edge
     * Notice: This method does not use unique index validation to check if the current element already exists
     *
     * @param indexLabel the index label
     * @param element    the properties owner
     */
    public List<Index> buildIndex(BaseElement element, IndexLabel indexLabel) {
        E.checkArgument(indexLabel != null,
                        "Not exist index label with id '%s'", indexLabel.id());

        List<Index> indexs = new ArrayList<>();

        // Collect property values of index fields
        List<Object> allPropValues = new ArrayList<>();
        int fieldsNum = indexLabel.indexFields().size();
        int firstNullField = fieldsNum;
        for (Id fieldId : indexLabel.indexFields()) {
            BaseProperty<Object> property = element.getProperty(fieldId);
            if (property == null) {
                E.checkState(hasNullableProp(element, fieldId),
                             "Non-null property '%s' is null for '%s'",
                             graph.propertyKey(fieldId), element);
                if (firstNullField == fieldsNum) {
                    firstNullField = allPropValues.size();
                }
                allPropValues.add(INDEX_SYM_NULL);
            } else {
                E.checkArgument(!INDEX_SYM_NULL.equals(property.value()),
                                "Illegal value of index property: '%s'",
                                INDEX_SYM_NULL);
                allPropValues.add(property.value());
            }
        }

        if (firstNullField == 0 && !indexLabel.indexType().isUnique()) {
            // The property value of first index field is null
            return indexs;
        }
        // Not build index for record with nullable field (except unique index)
        List<Object> propValues = allPropValues.subList(0, firstNullField);

        // Expired time
        long expiredTime = element.expiredTime();

        // Build index for each index type
        switch (indexLabel.indexType()) {
            case RANGE_INT:
            case RANGE_FLOAT:
            case RANGE_LONG:
            case RANGE_DOUBLE:
                E.checkState(propValues.size() == 1,
                             "Expect only one property in range index");
                Object value = NumericUtil.convertToNumber(propValues.get(0));
                indexs.add(this.buildIndex(indexLabel, value, element.id(),
                                           expiredTime));
                break;
            case SEARCH:
                E.checkState(propValues.size() == 1,
                             "Expect only one property in search index");
                value = propValues.get(0);
                Set<String> words =
                        this.segmentWords(propertyValueToString(value));
                for (String word : words) {
                    indexs.add(this.buildIndex(indexLabel, word, element.id(),
                                               expiredTime));
                }
                break;
            case SECONDARY:
                // Secondary index maybe include multi prefix index
                if (isCollectionIndex(propValues)) {
                    /*
                     * Property value is a collection
                     * we should create index for each item
                     */
                    for (Object propValue :
                            (Collection<Object>) propValues.get(0)) {
                        value = ConditionQuery.concatValuesLimitLength(
                                propValue);
                        value = escapeIndexValueIfNeeded((String) value);
                        indexs.add(this.buildIndex(indexLabel, value,
                                                   element.id(),
                                                   expiredTime));
                    }
                } else {
                    for (int i = 0, n = propValues.size(); i < n; i++) {
                        List<Object> prefixValues =
                                propValues.subList(0, i + 1);
                        value = ConditionQuery.concatValuesLimitLength(
                                prefixValues);
                        value = escapeIndexValueIfNeeded((String) value);
                        indexs.add(this.buildIndex(indexLabel, value,
                                                   element.id(),
                                                   expiredTime));
                    }
                }
                break;
            case SHARD:
                value = ConditionQuery.concatValuesLimitLength(propValues);
                value = escapeIndexValueIfNeeded((String) value);
                indexs.add(this.buildIndex(indexLabel, value, element.id(),
                                           expiredTime));
                break;
            case UNIQUE:
                value = ConditionQuery.concatValuesLimitLength(allPropValues);
                assert !"".equals(value);
                indexs.add(this.buildIndex(indexLabel, value, element.id(),
                                           expiredTime));
                break;
            default:
                throw new AssertionError(String.format(
                        "Unknown index type '%s'", indexLabel.indexType()));
        }

        return indexs;
    }

    private Index buildIndex(IndexLabel indexLabel, Object propValue,
                             Id elementId, long expiredTime) {
        Index index = new Index(graph, indexLabel, true);
        index.fieldValues(propValue);
        index.elementIds(elementId, expiredTime);

        return index;
    }


    private static String escapeIndexValueIfNeeded(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch <= INDEX_SYM_MAX) {
                /*
                 * Escape symbols can't be used due to impossible to parse,
                 * and treat it as illegal value for the origin text property
                 */
                E.checkArgument(false, "Illegal char '\\u000%s' " +
                                       "in index property: '%s'", (int) ch,
                                value);
            }
        }
        if (value.isEmpty()) {
            // Escape empty String to INDEX_SYM_EMPTY (char `\u0002`)
            value = INDEX_SYM_EMPTY;
        }
        return value;
    }

    private static boolean hasNullableProp(BaseElement element, Id key) {
        return element.schemaLabel().nullableKeys().contains(key);
    }

    private static boolean isCollectionIndex(List<Object> propValues) {
        return propValues.size() == 1 &&
               propValues.get(0) instanceof Collection;
    }

    private Set<String> segmentWords(String text) {
        return this.textAnalyzer.segment(text);
    }

    private static String propertyValueToString(Object value) {
        /*
         * Join collection items with white space if the value is Collection,
         * or else keep the origin value.
         */
        return value instanceof Collection ?
               StringUtils.join(((Collection<Object>) value).toArray(), " ") :
               value.toString();
    }
}
