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

import java.util.List;
import java.util.Set;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.util.InsertionOrderUtil;

import com.google.common.collect.ImmutableList;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class AnsjAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "BaseAnalysis",
            "IndexAnalysis",
            "ToAnalysis",
            "NlpAnalysis"
    );

    private String analysis;

    public AnsjAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for ansj analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.analysis = mode;
    }

    @Override
    public Set<String> segment(String text) {
        Result terms = null;
        switch (this.analysis) {
            case "BaseAnalysis":
                terms = BaseAnalysis.parse(text);
                break;
            case "ToAnalysis":
                terms = ToAnalysis.parse(text);
                break;
            case "NlpAnalysis":
                terms = NlpAnalysis.parse(text);
                break;
            case "IndexAnalysis":
                terms = IndexAnalysis.parse(text);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported segment mode '%s'", this.analysis));
        }

        assert terms != null;
        Set<String> result = InsertionOrderUtil.newSet();
        for (Term term : terms) {
            result.add(term.getName());
        }
        return result;
    }
}
