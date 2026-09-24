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

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;

import com.google.common.collect.ImmutableList;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class WordAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES =
           ImmutableList.<String>builder()
                        .add("MaximumMatching")
                        .add("ReverseMaximumMatching")
                        .add("MinimumMatching")
                        .add("ReverseMinimumMatching")
                        .add("BidirectionalMaximumMatching")
                        .add("BidirectionalMinimumMatching")
                        .add("BidirectionalMaximumMinimumMatching")
                        .add("FullSegmentation")
                        .add("MinimalWordCount")
                        .add("MaxNgramScore")
                        .add("PureEnglish")
                        .build();

    private SegmentationAlgorithm algorithm;

    public WordAnalyzer(String mode) {
        try {
            this.algorithm = SegmentationAlgorithm.valueOf(mode);
        } catch (Exception e) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for word analyzer, " +
                      "the available values are %s", e, mode, SUPPORT_MODES);
        }
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        List<Word> words = WordSegmenter.segWithStopWords(text, this.algorithm);
        for (Word word : words) {
            result.add(word.getText());
        }
        return result;
    }
}
