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

package org.apache.hugegraph.id;

import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.structure.BaseVertex;

public class SplicingIdGenerator extends IdGenerator {

    private static volatile SplicingIdGenerator instance;

    public static SplicingIdGenerator instance() {
        if (instance == null) {
            synchronized (SplicingIdGenerator.class) {
                if (instance == null) {
                    instance = new SplicingIdGenerator();
                }
            }
        }
        return instance;
    }

    /*
     * The following defines can't be java regex special characters:
     * "\^$.|?*+()[{"
     * See: http://www.regular-expressions.info/characters.html
     */
    private static final char ESCAPE = '`';
    private static final char IDS_SPLITOR = '>';
    private static final char ID_SPLITOR = ':';
    private static final char NAME_SPLITOR = '!';

    public static final String ESCAPE_STR = String.valueOf(ESCAPE);
    public static final String IDS_SPLITOR_STR = String.valueOf(IDS_SPLITOR);
    public static final String ID_SPLITOR_STR = String.valueOf(ID_SPLITOR);

    /****************************** id generate ******************************/

    /**
     * Generate a string id of HugeVertex from Vertex name
     */
    @Override
    public Id generate(BaseVertex vertex) {
        /*
         * Hash for row-key which will be evenly distributed.
         * We can also use LongEncoding.encode() to encode the int/long hash
         * if needed.
         * id = String.format("%s%s%s", HashUtil.hash(id), ID_SPLITOR, id);
         */
        // TODO: use binary Id with binary fields instead of string id
        return splicing(vertex.schemaLabel().id().asString(), vertex.name());
    }

    /**
     * Concat multiple ids into one composite id with IDS_SPLITOR
     * @param ids the string id values to be concatted
     * @return    concatted string value
     */
    public static String concat(String... ids) {
        // NOTE: must support string id when using this method
        return IdUtil.escape(IDS_SPLITOR, ESCAPE, ids);
    }

    /**
     * Split a composite id into multiple ids with IDS_SPLITOR
     * @param ids the string id value to be splitted
     * @return    splitted string values
     */
    public static String[] split(String ids) {
        return IdUtil.unescape(ids, IDS_SPLITOR_STR, ESCAPE_STR);
    }

    /**
     * Concat property values with NAME_SPLITOR
     * @param values the property values to be concatted
     * @return       concatted string value
     */
    public static String concatValues(List<?> values) {
        // Convert the object list to string array
        int valuesSize = values.size();
        String[] parts = new String[valuesSize];
        for (int i = 0; i < valuesSize; i++) {
            parts[i] = values.get(i).toString();
        }
        return IdUtil.escape(NAME_SPLITOR, ESCAPE, parts);
    }

    /**
     * Concat property values with NAME_SPLITOR
     * @param values the property values to be concatted
     * @return       concatted string value
     */
    public static String concatValues(Object... values) {
        return concatValues(Arrays.asList(values));
    }

    /**
     * Concat multiple parts into a single id with ID_SPLITOR
     * @param parts the string id values to be spliced
     * @return      spliced id object
     */
    public static Id splicing(String... parts) {
        String escaped = IdUtil.escape(ID_SPLITOR, ESCAPE, parts);
        return IdGenerator.of(escaped);
    }

    public static Id splicingWithNoEscape(String... parts) {
        String escaped = String.join(ID_SPLITOR_STR, parts);
        return IdGenerator.of(escaped);
    }

    public static Id generateBinaryId(Id id) {
        if (id instanceof BinaryId) {
            return id;
        }
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        BinaryId binaryId = new BinaryId(buffer.writeId(id).bytes(), id);
        return binaryId;
    }

    /**
     * Parse a single id into multiple parts with ID_SPLITOR
     * @param id the id object to be parsed
     * @return   parsed string id parts
     */
    public static String[] parse(Id id) {
        return IdUtil.unescape(id.asString(), ID_SPLITOR_STR, ESCAPE_STR);
    }
}
