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

package org.apache.hugegraph.unit.api.cypher;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hugegraph.opencypher.CypherTemporalFunctions;
import org.junit.Assert;
import org.junit.Test;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.translator.Translator;

public class CypherTemporalFunctionTest {

    @Test
    public void testDateLiteralFoldsInPropertyMatch() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n {d0: date('2024-02-05')}) RETURN count(*)",
                Collections.emptyMap());

        Assert.assertTrue(folded.getKey(), folded.getKey().contains("g.V()"));
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2024-02-05"),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
    }

    @Test
    public void testBareDateFoldsToStartOfToday() throws Exception {
        Map.Entry<String, Map<String, Object>> folded =
                translate("RETURN date()", Collections.emptyMap());

        Assert.assertEquals(Date.from(LocalDate.now()
                                            .atStartOfDay(ZoneId.systemDefault())
                                            .toInstant()),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
    }

    @Test
    public void testDateWithParameterArgumentFolds() throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("d", "2024-02-05");
        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n) WHERE n.d0 = date($d) RETURN count(*)", parameters);

        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2024-02-05"),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
        Assert.assertEquals("2024-02-05", folded.getValue().get("d"));
    }

    @Test
    public void testDatetimeAndLocaldatetimeFold() throws Exception {
        Map.Entry<String, Map<String, Object>> datetime = translate(
                "RETURN datetime('2024-02-05T10:15:30')", Collections.emptyMap());
        Assert.assertEquals(expectedDate("yyyy-MM-dd'T'HH:mm:ss", "2024-02-05T10:15:30"),
                            syntheticBinding(datetime.getValue(), "  cypher.fold.datetime."));

        Map.Entry<String, Map<String, Object>> localdatetime = translate(
                "RETURN localdatetime('2024-02-05T10:15:30.123')", Collections.emptyMap());
        Assert.assertEquals(expectedDate("yyyy-MM-dd'T'HH:mm:ss.SSS", "2024-02-05T10:15:30.123"),
                            syntheticBinding(localdatetime.getValue(), "  cypher.fold.localdatetime."));
    }

    @Test
    public void testDateInsideAnotherCallFolds() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "RETURN coalesce(date('2024-02-05'), 'x')", Collections.emptyMap());

        Assert.assertNotNull(folded.getKey());
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2024-02-05"),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
    }

    @Test
    public void testDateInCreatePropertyFolds() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "CREATE (n:person {birthday: date('2000-01-01')}) RETURN n",
                Collections.emptyMap());

        Assert.assertNotNull(folded.getKey());
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2000-01-01"),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
    }

    @Test
    public void testDateInLabelledMatchWhereFolds() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n:cypher_person) WHERE n.birthday = date('2000-01-01') RETURN n.name AS name",
                Collections.emptyMap());

        Assert.assertTrue(folded.getKey(), folded.getKey().contains("g.V()"));
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2000-01-01"),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
    }

    @Test
    public void testDateInLabelledPropertyMapFolds() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n:cypher_person {note: date('2024-02-05')}) RETURN count(n) AS count",
                Collections.emptyMap());

        Assert.assertTrue(folded.getKey(), folded.getKey().contains("g.V()"));
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2024-02-05"),
                            syntheticBinding(folded.getValue(), "  cypher.fold.date."));
    }

    @Test
    public void testDateUnderAndRangeFolds() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n) WHERE n.d0 > date('2024-02-05') AND n.d0 < date('2025-01-01') RETURN n",
                Collections.emptyMap());

        List<Object> foldedDates = new ArrayList<>();
        for (Map.Entry<String, Object> entry : folded.getValue().entrySet()) {
            if (entry.getKey().startsWith("  cypher.fold.date.")) {
                foldedDates.add(entry.getValue());
            }
        }
        Assert.assertEquals(2, foldedDates.size());
        Assert.assertTrue(foldedDates.remove(expectedDate("yyyy-MM-dd", "2024-02-05")));
        Assert.assertTrue(foldedDates.remove(expectedDate("yyyy-MM-dd", "2025-01-01")));
    }

    @Test
    public void testUnfoldableDateStaysUnknownFunction() {
        for (String query : new String[]{"MATCH (n) RETURN date(n.x)",
                                         "RETURN date('tomorrow')"}) {
            Exception failure = Assert.assertThrows(Exception.class,
                                                    () -> translate(query, Collections.emptyMap()));
            Assert.assertTrue(query, failure.getMessage().contains("Unknown function 'date'"));
        }
    }

    @Test
    public void testDurationStaysUnknownFunction() {
        Exception failure = Assert.assertThrows(Exception.class,
                () -> translate("RETURN duration('P1Y')", Collections.emptyMap()));
        Assert.assertTrue(failure.getMessage(), failure.getMessage()
                                                  .contains("Unknown function 'duration'"));
    }

    @Test
    public void testQueryWithoutTemporalFunctionsKeepsParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("name", "marko");
        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n) WHERE n.name = $name RETURN n", parameters);

        Assert.assertSame(parameters, folded.getValue());
        Assert.assertTrue(folded.getKey(), folded.getKey().contains("marko"));
    }

    private static Map.Entry<String, Map<String, Object>> translate(String query,
                                                                    Map<String, Object> parameters) {
        CypherAst ast = CypherAst.parse(query, parameters);
        Map<String, Object> effective = CypherTemporalFunctions.fold(ast, parameters);
        String gremlin = ast.buildTranslation(Translator.builder().gremlinGroovy()
                                                        .build("gremlin+cfog_server_extensions" +
                                                               "+inline_parameters"));
        return new java.util.AbstractMap.SimpleImmutableEntry<>(gremlin, effective);
    }

    private static Date expectedDate(String pattern, String literal) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.ROOT);
        format.setLenient(false);
        return format.parse(literal);
    }

    private static Date syntheticBinding(Map<String, Object> parameters, String prefix) {
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                Assert.assertTrue(entry.getKey(), entry.getValue() instanceof Date);
                return (Date) entry.getValue();
            }
        }
        Assert.fail("Missing synthetic binding with prefix " + prefix);
        return null;
    }
}
