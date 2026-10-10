/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.hugegraph.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

public class AnalyzerFactoryTest {

    @Test
    public void testWordAnalyzerWithProvidedDependency() {
        Analyzer analyzer = AnalyzerFactory.analyzer("word", "PureEnglish");
        Assert.assertFalse(analyzer.segment("hello world").isEmpty());
    }

    @Test
    public void testWordAnalyzerKeepsModeValidation() {
        ConfigException exception = Assert.assertThrows(ConfigException.class, () -> {
            AnalyzerFactory.analyzer("word", "invalid");
        });
        Assert.assertTrue(exception.getMessage().contains("Unsupported segment mode"));
    }
}
