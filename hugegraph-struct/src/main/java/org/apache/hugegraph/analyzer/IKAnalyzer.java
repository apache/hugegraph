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

import com.google.common.collect.ImmutableList;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class IKAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "smart",
            "max_word"
    );

    private boolean smartSegMode;
    private final IKSegmenter ik;

    public IKAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for ikanalyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.smartSegMode = SUPPORT_MODES.get(0).equals(mode);
        this.ik = new IKSegmenter(new StringReader(""),
                                  this.smartSegMode);
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        ik.reset(new StringReader(text));
        try {
            Lexeme word = null;
            while ((word = ik.next()) != null) {
                result.add(word.getLexemeText());
            }
        } catch (Exception e) {
            throw new HugeException("IKAnalyzer segment text '%s' failed",
                                    e, text);
        }
        return result;
    }
}
