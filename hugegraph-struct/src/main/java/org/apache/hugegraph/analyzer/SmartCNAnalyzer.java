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

package org.apache.hugegraph.analyzer;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.apache.hugegraph.exception.HugeException;
import com.google.common.collect.ImmutableList;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class SmartCNAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of();

    private static final SmartChineseAnalyzer ANALYZER =
                                              new SmartChineseAnalyzer();

    public SmartCNAnalyzer(String mode) {
        // pass
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        Reader reader = new StringReader(text);
        try (TokenStream tokenStream = ANALYZER.tokenStream("text", reader)) {
            tokenStream.reset();
            CharTermAttribute term = null;
            while (tokenStream.incrementToken()) {
                term = tokenStream.getAttribute(CharTermAttribute.class);
                result.add(term.toString());
            }
        } catch (Exception e) {
            throw new HugeException("SmartCN segment text '%s' failed",
                                    e, text);
        }
        return result;
    }
}
