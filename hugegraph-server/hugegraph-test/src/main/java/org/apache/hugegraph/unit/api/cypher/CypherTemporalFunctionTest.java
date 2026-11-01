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
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hugegraph.opencypher.CypherTemporalFunctions;
import org.junit.Assert;
import org.junit.Test;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.ir.TranslationWriter;
import org.opencypher.gremlin.translation.ir.model.GremlinStep;
import org.opencypher.gremlin.translation.translator.Translator;
import org.opencypher.gremlin.traversal.ProcedureContext;
import scala.collection.Seq;

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
    public void testFractionalSecondsFoldAsFractionOfSecond() throws Exception {
        Map.Entry<String, Map<String, Object>> datetime = translate(
                "RETURN datetime('2024-02-05T10:15:30.5')", Collections.emptyMap());
        Assert.assertEquals(expectedDate(2024, 2, 5, 10, 15, 30, 500),
                            syntheticBinding(datetime.getValue(), "  cypher.fold.datetime."));

        Map.Entry<String, Map<String, Object>> localdatetime = translate(
                "RETURN localdatetime('2024-02-05T10:15:30.12')", Collections.emptyMap());
        Assert.assertEquals(expectedDate(2024, 2, 5, 10, 15, 30, 120),
                            syntheticBinding(localdatetime.getValue(),
                                             "  cypher.fold.localdatetime."));
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
                                         "RETURN date('tomorrow')",
                                         "RETURN date('2024-2-5')"}) {
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

    @Test
    public void testUpperAndMixedCaseConstructorsFold() throws Exception {
        Map.Entry<String, Map<String, Object>> upper = translate(
                "MATCH (n) WHERE n.d0 = DATE('2000-01-01') RETURN n",
                Collections.emptyMap());
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2000-01-01"),
                            syntheticBinding(upper.getValue(), "  cypher.fold.date."));

        Map.Entry<String, Map<String, Object>> mixed = translate(
                "RETURN DateTime('2024-02-05T10:15:30')", Collections.emptyMap());
        Assert.assertEquals(expectedDate("yyyy-MM-dd'T'HH:mm:ss", "2024-02-05T10:15:30"),
                            syntheticBinding(mixed.getValue(), "  cypher.fold.datetime."));

        Map.Entry<String, Map<String, Object>> local = translate(
                "RETURN localDateTime('2024-02-05T10:15:30')", Collections.emptyMap());
        Assert.assertEquals(expectedDate("yyyy-MM-dd'T'HH:mm:ss", "2024-02-05T10:15:30"),
                            syntheticBinding(local.getValue(), "  cypher.fold.localdatetime."));
    }

    @Test
    public void testSyntheticNameSkipsOccupiedLiteralBinding() throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("  cypher.fold.date.0", "original");
        Map.Entry<String, Map<String, Object>> folded = translate(
                "RETURN date('2024-02-05') AS d, $`  cypher.fold.date.0` AS other", parameters);

        Assert.assertEquals("original", folded.getValue().get("  cypher.fold.date.0"));
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2024-02-05"),
                            folded.getValue().get("  cypher.fold.date.1"));
        Assert.assertTrue(folded.getKey(), folded.getKey().contains("original"));
    }

    @Test
    public void testSyntheticNameSkipsOccupiedParameterBinding() throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("d", "2024-02-05");
        parameters.put("  cypher.fold.date.0", "original");
        Map.Entry<String, Map<String, Object>> folded = translate(
                "RETURN date($d) AS d, $`  cypher.fold.date.0` AS other", parameters);

        Assert.assertEquals("original", folded.getValue().get("  cypher.fold.date.0"));
        Assert.assertEquals(expectedDate("yyyy-MM-dd", "2024-02-05"),
                            folded.getValue().get("  cypher.fold.date.1"));
        Assert.assertEquals("2024-02-05", folded.getValue().get("d"));
    }

    @Test
    public void testBareConstructorsShareOneInstant() throws Exception {
        Map.Entry<String, Map<String, Object>> folded = translate(
                "CREATE (a {t: datetime()}), (b {t: datetime()}) RETURN count(*)",
                Collections.emptyMap());

        List<Date> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : folded.getValue().entrySet()) {
            if (entry.getKey().startsWith("  cypher.fold.datetime.")) {
                values.add((Date) entry.getValue());
            }
        }
        Assert.assertEquals(2, values.size());
        Assert.assertEquals(values.get(0), values.get(1));
    }

    @Test
    public void testExplainTranslationPrintsFoldedDatesAsConstructors() {
        String query = "MATCH (n) WHERE n.d0 = date('2000-01-01') RETURN n";
        Map<String, Object> empty = Collections.emptyMap();
        CypherAst ast = CypherAst.parse(query, empty);
        Map<String, Object> effective = CypherTemporalFunctions.fold(ast, empty);
        Date folded = syntheticBinding(effective, "  cypher.fold.date.");

        Translator<String, GroovyPredicate> strTranslator = Translator.builder()
                                                                      .gremlinGroovy()
                                                                      .build("gremlin+cfog_server_extensions" +
                                                                             "+inline_parameters");
        Seq<GremlinStep> ir = ast.translate(strTranslator.flavor(),
                                            strTranslator.features(),
                                            ProcedureContext.global());
        String gremlin = TranslationWriter.write(ir, strTranslator,
                CypherTemporalFunctions.explainParameters(effective));

        Assert.assertTrue(gremlin, gremlin.contains("new Date(" + folded.getTime() + "L)"));
        Assert.assertFalse(gremlin, gremlin.contains(folded.toString()));
    }

    @Test
    public void testCaseReturnWithFoldedDateKeepsValueMap() throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("d", expectedDate("yyyy-MM-dd", "2000-01-01"));
        Map.Entry<String, Map<String, Object>> bound = translate(
                "MATCH (n) RETURN CASE WHEN n.d > $d THEN n END AS m", parameters);
        Assert.assertTrue(bound.getKey(), bound.getKey().contains("valueMap"));

        Map.Entry<String, Map<String, Object>> folded = translate(
                "MATCH (n) RETURN CASE WHEN n.d > date('2000-01-01') THEN n END AS m",
                Collections.emptyMap());
        Assert.assertTrue(folded.getKey(), folded.getKey().contains("valueMap"));

        Map.Entry<String, Map<String, Object>> relationship = translate(
                "MATCH (a)-[r]->(b) RETURN CASE WHEN r.since > date('2000-01-01') THEN r END AS rel",
                Collections.emptyMap());
        Assert.assertTrue(relationship.getKey(), relationship.getKey().contains("valueMap"));
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

    private static Date expectedDate(int year, int month, int day, int hour,
                                     int minute, int second, int millis) {
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
        calendar.clear();
        calendar.set(year, month - 1, day, hour, minute, second);
        calendar.set(Calendar.MILLISECOND, millis);
        return calendar.getTime();
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
