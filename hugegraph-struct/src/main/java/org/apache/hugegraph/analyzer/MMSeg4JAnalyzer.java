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
import org.apache.hugegraph.util.InsertionOrderUtil;

import org.apache.hugegraph.exception.HugeException;
import com.chenlb.mmseg4j.ComplexSeg;
import com.chenlb.mmseg4j.Dictionary;
import com.chenlb.mmseg4j.MMSeg;
import com.chenlb.mmseg4j.MaxWordSeg;
import com.chenlb.mmseg4j.Seg;
import com.chenlb.mmseg4j.SimpleSeg;
import com.chenlb.mmseg4j.Word;
import com.google.common.collect.ImmutableList;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class MMSeg4JAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "Simple",
            "Complex",
            "MaxWord"
    );

    private static final Dictionary DIC = Dictionary.getInstance();

    private Seg seg;

    public MMSeg4JAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for mmseg4j analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        int index = SUPPORT_MODES.indexOf(mode);
        switch (index) {
            case 0:
                this.seg = new SimpleSeg(DIC);
                break;
            case 1:
                this.seg = new ComplexSeg(DIC);
                break;
            case 2:
                this.seg = new MaxWordSeg(DIC);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported segment mode '%s'", this.seg));
        }
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        MMSeg mmSeg = new MMSeg(new StringReader(text), this.seg);
        try {
            Word word = null;
            while ((word = mmSeg.next()) != null) {
                result.add(word.getString());
            }
        } catch (Exception e) {
            throw new HugeException("MMSeg4j segment text '%s' failed",
                                    e, text);
        }
        return result;
    }
}
