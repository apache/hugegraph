/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hugegraph.tinkerpop;

import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.tinkerpop.gremlin.features.AbstractGuiceFactory;
import org.apache.tinkerpop.gremlin.features.World;
import org.junit.runner.RunWith;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;

import io.cucumber.guice.CucumberModules;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;

@RunWith(Cucumber.class)
@CucumberOptions(
        tags = HugeGraphFeatureTest.TAGS,
        name = HugeGraphFeatureTest.NAMES,
        glue = {"org.apache.tinkerpop.gremlin.features"},
        objectFactory = HugeGraphFeatureTest.HugeGraphGuiceFactory.class,
        features = {
                "classpath:/org/apache/tinkerpop/gremlin/test/features"
        },
        plugin = {
                "progress",
                "junit:target/cucumber-tp38.xml",
                "org.apache.hugegraph.tinkerpop." +
                "HugeGraphScenarioCountPlugin"
        })
public class HugeGraphFeatureTest {

    public static final String NAMES =
            "^g_(?!mergeEXlabel_knows_out_marko_in_vadas_weight_05X_" +
            "exists$)(?!V_hasXperson_name_marko_X_mergeEXlabel_knowsX_" +
            "optionXonCreate_created_YX_optionXonMatch_created_NX_" +
            "exists_updated$)" +
            // HugeGraph rejects limit queries while graph initializer data
            // is still uncommitted, before mergeE can validate its endpoints.
            "(?!V_limitX1X_mergeEXnullvarX$).*";

    public static final String TAGS =
            "(@StepAsString or @StepConcat or @StepFormat or " +
            "@StepLength or @StepSplit or @StepSubstring or " +
            "@StepReplace or @StepReverse or @StepToLower or " +
            "@StepToUpper or @StepTrim or @StepLTrim or @StepRTrim or " +
            "@StepCombine or @StepMerge or @StepIntersect or " +
            "@StepDifference or @StepDisjunct or @StepConjoin or " +
            "@StepProduct or @StepAll or @StepAny or @StepAsDate or " +
            "@StepDateAdd or @StepDateDiff or @StepMergeV or " +
            "@StepMergeE or @StepFail) and " +
            "not @RemoteOnly and not @GraphComputerOnly and " +
            "not @AllowNullPropertyValues and not @MetaProperties and " +
            "not @MultiProperties and " +
            "not @UserSuppliedVertexIds and not @UserSuppliedEdgeIds and " +
            "not @UserSuppliedVertexPropertyIds and " +
            "not @InsertionOrderingRequired";

    public static class HugeGraphGuiceFactory extends AbstractGuiceFactory {

        public HugeGraphGuiceFactory() {
            super(createInjector());
        }

        private static Injector createInjector() {
            RegisterUtil.registerBackends();
            return Guice.createInjector(Stage.PRODUCTION,
                                        CucumberModules.createScenarioModule(),
                                        new ServiceModule());
        }
    }

    public static final class ServiceModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(World.class).to(HugeGraphWorld.class);
        }
    }
}
