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

package org.apache.hugegraph.unit.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hugegraph.analyzer.Analyzer;
import org.apache.hugegraph.analyzer.AnalyzerFactory;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AnalyzerTest {

    private static final String TEXT_1 = "England wins World Cup";
    private static final String TEXT_2 = "英格兰世界杯夺冠，中华人民共和国国歌，" +
                                         "百度科技园位于北京市海淀区西北旺东路10号院";

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testWordAnalyzerRequiresOptionalDependency() {
        ConfigException exception = Assert.assertThrows(ConfigException.class, () -> {
            AnalyzerFactory.analyzer("word", "PureEnglish");
        });
        Assert.assertTrue(exception.getMessage().contains("org.apdplat:word:1.3"));
        Assert.assertTrue(exception.getCause() instanceof NoClassDefFoundError);
    }

    @Test
    public void testAnsjAnalyzer() {
        // BaseAnalysis mode
        Analyzer analyzer = AnalyzerFactory.analyzer("ansj", "BaseAnalysis");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技", "园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号", "院"),
                            analyzer.segment(TEXT_2));

        // IndexAnalysis mode
        analyzer = AnalyzerFactory.analyzer("ansj", "IndexAnalysis");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技", "园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10号", "院"),
                            analyzer.segment(TEXT_2));
    }

    @Test
    public void testHanlpAnalyzer() {
        // standard mode
        Analyzer analyzer = AnalyzerFactory.analyzer("hanlp", "standard");
        Assert.assertEquals(setOf("England", " ", "wins", "World", "Cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北旺", "东路", "10", "号", "院"),
                            analyzer.segment(TEXT_2));

        // Note latest hanlp portable version not contains model data
        // https://github.com/hankcs/HanLP/tree/portable#%E6%96%B9%E5%BC%8F%E4%B8%80maven
        // So test IndexTokenizer instead
        analyzer = AnalyzerFactory.analyzer("hanlp", "index");
        Assert.assertEquals(setOf("England", " ", "wins", "World", "Cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "英格", "格兰", "世界杯", "世界", "夺冠", "，",
                                  "中华人民共和国", "中华", "华人", "人民", "共和国",
                                  "共和", "国歌", "百度", "科技园", "科技", "位于",
                                  "北京市", "北京", "海淀区", "海淀", "淀区", "西北旺",
                                  "西北", "东路", "10", "号", "院"),
                            analyzer.segment(TEXT_2));
    }

    @Test
    public void testSmartCNAnalyzer() {
        Analyzer analyzer = AnalyzerFactory.analyzer("smartcn", "");
        Assert.assertEquals(setOf("england", "win", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百", "度", "科技", "园", "位于",
                                  "北京市", "海淀区", "西北", "旺", "东", "路",
                                  "10", "号", "院"),
                            analyzer.segment(TEXT_2));
    }

    @Test
    public void testJiebaAnalyzer() {
        // SEARCH mode
        Analyzer analyzer = AnalyzerFactory.analyzer("jieba", "SEARCH");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号院"),
                            analyzer.segment(TEXT_2));

        // INDEX mode
        analyzer = AnalyzerFactory.analyzer("jieba", "INDEX");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界", "世界杯", "夺冠", "，", "中华",
                                  "华人", "人民", "共和", "共和国", "中华人民共和国",
                                  "国歌", "百度", "科技", "科技园", "位于", "北京",
                                  "京市", "北京市", "海淀", "淀区", "海淀区", "西北",
                                  "旺", "东路", "10", "号院"),
                            analyzer.segment(TEXT_2));
    }

    @Test
    public void testJcsegAnalyzer() {
        // Simple mode
        Analyzer analyzer = AnalyzerFactory.analyzer("jcseg", "Simple");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华",
                                  "人民共和国", "国歌", "百度", "科技", "园", "位于",
                                  "北京市", "海淀区", "西北", "旺", "东路", "1", "0",
                                  "号", "院"),
                            analyzer.segment(TEXT_2));

        // Complex mode
        analyzer = AnalyzerFactory.analyzer("jcseg", "Complex");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华",
                                  "人民共和国", "国歌", "百度", "科技", "园", "位于",
                                  "北京市", "海淀区", "西北", "旺", "东路", "1", "0",
                                  "号", "院"),
                            analyzer.segment(TEXT_2));
    }

    @Test
    public void testMMSeg4JAnalyzer() {
        // Simple mode
        Analyzer analyzer = AnalyzerFactory.analyzer("mmseg4j", "Simple");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号",
                                  "院"),
                            analyzer.segment(TEXT_2));

        // Complex mode
        analyzer = AnalyzerFactory.analyzer("mmseg4j", "Complex");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号",
                                  "院"),
                            analyzer.segment(TEXT_2));
    }

    @Test
    public void testIKAnalyzer() {
        // Smart mode
        Analyzer analyzer = AnalyzerFactory.analyzer("ikanalyzer", "smart");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10号", "院"),
                            analyzer.segment(TEXT_2));

        // Max_word mode
        analyzer = AnalyzerFactory.analyzer("ikanalyzer", "max_word");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(TEXT_1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "世界", "杯", "夺冠",
                                  "中华人民共和国", "中华人民", "中华", "华人",
                                  "人民共和国", "人民", "共和国", "共和", "国",
                                  "国歌", "百度", "百", "度", "科技园", "科技",
                                  "园", "位于", "北京市", "北京", "市", "海淀区",
                                  "海淀", "淀区", "西北", "旺", "东路", "10",
                                  "号", "院"),
                            analyzer.segment(TEXT_2));
    }

    private static Set<String> setOf(String... elems) {
        return new HashSet<>(Arrays.asList(elems));
    }

}
