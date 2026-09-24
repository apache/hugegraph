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

import com.google.common.collect.ImmutableList;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class JiebaAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "SEARCH",
            "INDEX"
    );

    private static final JiebaSegmenter JIEBA_SEGMENTER = new JiebaSegmenter();

    private JiebaSegmenter.SegMode segMode;

    public JiebaAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for jieba analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.segMode = JiebaSegmenter.SegMode.valueOf(mode);
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        for (SegToken token : JIEBA_SEGMENTER.process(text, this.segMode)) {
            result.add(token.word);
        }
        return result;
    }
}
