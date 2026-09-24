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


import com.google.common.collect.ImmutableList;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.SpeedTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.util.InsertionOrderUtil;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class HanLPAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES =
           ImmutableList.<String>builder()
                        .add("standard")
                        .add("nlp")
                        .add("index")
                        .add("nShort")
                        .add("shortest")
                        .add("speed")
                        .build();

    private static final Segment N_SHORT_SEGMENT =
            new NShortSegment().enableCustomDictionary(false)
                               .enablePlaceRecognize(true)
                               .enableOrganizationRecognize(true);
    private static final Segment DIJKSTRA_SEGMENT =
            new DijkstraSegment().enableCustomDictionary(false)
                                 .enablePlaceRecognize(true)
                                 .enableOrganizationRecognize(true);

    private String tokenizer;

    public HanLPAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for hanlp analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.tokenizer = mode;
    }

    @Override
    public Set<String> segment(String text) {
        List<Term> terms = null;
        switch (this.tokenizer) {
            case "standard":
                terms = StandardTokenizer.segment(text);
                break;
            case "nlp":
                terms = NLPTokenizer.segment(text);
                break;
            case "index":
                terms = IndexTokenizer.segment(text);
                break;
            case "nShort":
                terms = N_SHORT_SEGMENT.seg(text);
                break;
            case "shortest":
                terms = DIJKSTRA_SEGMENT.seg(text);
                break;
            case "speed":
                terms = SpeedTokenizer.segment(text);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported segment mode '%s'", this.tokenizer));
        }

        assert terms != null;
        Set<String> result = InsertionOrderUtil.newSet();
        for (Term term : terms) {
            result.add(term.word);
        }
        return result;
    }
}
