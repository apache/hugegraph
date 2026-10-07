/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.analyzer;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.lionsoul.jcseg.ISegment;
import org.lionsoul.jcseg.IWord;
import org.lionsoul.jcseg.dic.ADictionary;
import org.lionsoul.jcseg.dic.DictionaryFactory;
import org.lionsoul.jcseg.segmenter.SegmenterConfig;

import com.google.common.collect.ImmutableList;

/**
 * Reference from https://github.com/lionsoul2014/jcseg
 */
public class JcsegAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "Simple",
            "Complex"
    );

    private static final SegmenterConfig CONFIG = new SegmenterConfig();
    private static final ADictionary DIC = DictionaryFactory.createDefaultDictionary(CONFIG);

    private final ISegment.Type type;

    public JcsegAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                    "Unsupported segment mode '%s' for jcseg analyzer, " +
                    "the available values are %s", mode, SUPPORT_MODES);
        }

        if ("Simple".equals(mode)) {
            this.type = ISegment.SIMPLE;
        } else {
            this.type = ISegment.COMPLEX;
        }
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        try {
            ISegment segmentor = this.type.factory.create(CONFIG, DIC);
            segmentor.reset(new StringReader(text));

            IWord word;
            while ((word = segmentor.next()) != null) {
                result.add(word.getValue());
            }
        } catch (Exception e) {
            throw new HugeException("Jcseg segment text '%s' failed", e, text);
        }
        return result;
    }
}
