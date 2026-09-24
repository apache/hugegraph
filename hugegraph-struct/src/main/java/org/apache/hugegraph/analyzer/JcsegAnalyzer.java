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

import java.io.StringReader;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.lionsoul.jcseg.tokenizer.core.ADictionary;
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory;
import org.lionsoul.jcseg.tokenizer.core.ISegment;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory;
import com.google.common.collect.ImmutableList;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class JcsegAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "Simple",
            "Complex"
    );

    private static final JcsegTaskConfig CONFIG = new JcsegTaskConfig();
    private static final ADictionary DIC =
            DictionaryFactory.createDefaultDictionary(new JcsegTaskConfig());

    private int segMode;

    public JcsegAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for jcseg analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.segMode = SUPPORT_MODES.indexOf(mode) + 1;
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        try {
            Object[] args = new Object[]{new StringReader(text), CONFIG, DIC};
            ISegment seg = SegmentFactory.createJcseg(this.segMode, args);
            IWord word = null;
            while ((word = seg.next()) != null) {
                result.add(word.getValue());
            }
        } catch (Exception e) {
            throw new HugeException("Jcseg segment text '%s' failed", e, text);
        }
        return result;
    }
}
