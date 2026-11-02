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

package org.apache.hugegraph.opencypher;

import static org.slf4j.LoggerFactory.getLogger;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.v9_0.expressions.Expression;
import org.opencypher.v9_0.expressions.FunctionInvocation;
import org.opencypher.v9_0.expressions.Parameter;
import org.opencypher.v9_0.expressions.StringLiteral;
import org.opencypher.v9_0.util.InputPosition;
import org.opencypher.v9_0.util.symbols.AnyType;
import org.opencypher.v9_0.util.symbols.CypherType;
import org.slf4j.Logger;

import scala.Option;
import scala.Product;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Rewrites the Cypher temporal constructor functions that the cypher-for-gremlin
 * translator does not implement into parameters bound to native date values.
 * <p>
 * The translator's function list is fixed inside its own dependency, so the
 * supported constructors (date, datetime, localdatetime) are folded into the
 * parsed statement right before translation: each call with a foldable argument
 * becomes a synthetic bound parameter and the date value is added to the
 * bindings. Calls that cannot be folded are left in place and keep failing
 * with the translator's original error, as do all other temporal functions.
 */
public final class CypherTemporalFunctions {

    private static final Logger logger = getLogger(CypherTemporalFunctions.class);

    private static final String DATE = "date";
    private static final String DATETIME = "datetime";
    private static final String LOCALDATETIME = "localdatetime";

    // Two leading spaces keep the prefix away from ordinary parameter names;
    // backtick-escaped Cypher parameters can still carry it, so generated
    // names skip every one the request already occupies.
    private static final String SYNTHETIC_PREFIX = "  cypher.fold.";

    private CypherTemporalFunctions() {
    }

    /**
     * Folds the supported temporal constructors in the parsed statement and
     * returns the effective request parameters for translation: the same
     * instance when nothing was folded, a copy carrying the synthetic
     * bindings otherwise. The parsed statement is only replaced when the
     * rewrite completed without errors, so a fold failure leaves the query
     * exactly as it was parsed. The inferred types of the rebuilt expressions
     * are carried over with the statement.
     */
    public static Map<String, Object> fold(CypherAst ast, Map<String, Object> parameters) {
        TemporalFold fold = new TemporalFold(parameters);
        try {
            Field statementField = CypherAst.class.getDeclaredField("statement");
            Field parametersField = CypherAst.class.getDeclaredField("parameters");
            Field typesField = CypherAst.class.getDeclaredField("expressionTypes");
            statementField.setAccessible(true);
            parametersField.setAccessible(true);
            typesField.setAccessible(true);

            Object folded = fold.copy(ast.statement());
            if (fold.bindings.isEmpty()) {
                return parameters;
            }
            Object merged = mergeParameters(parametersField.get(ast), fold.bindings);
            Object types = carryTypes(typesField.get(ast), fold.copies);
            statementField.set(ast, folded);
            parametersField.set(ast, merged);
            typesField.set(ast, types);
        } catch (Exception | LinkageError e) {
            logger.warn("Failed to fold temporal functions, keeping the parsed statement", e);
            return parameters;
        }
        Map<String, Object> effective = new LinkedHashMap<>(parameters);
        effective.putAll(fold.bindings);
        return effective;
    }

    @SuppressWarnings("unchecked")
    private static Object mergeParameters(Object parameters, Map<String, Object> bindings) {
        scala.collection.immutable.Map<String, Object> merged =
                (scala.collection.immutable.Map<String, Object>) parameters;
        for (Map.Entry<String, Object> binding : bindings.entrySet()) {
            merged = merged.$plus(new Tuple2<>(binding.getKey(), binding.getValue()));
        }
        return merged;
    }

    /**
     * Returns the expression type map extended with the types of the rebuilt
     * expressions, so a statement the fold touched still finds the type of
     * every expression, for example the valueMap() decision for a RETURN
     * item that holds a node or relationship.
     */
    @SuppressWarnings("unchecked")
    private static Object carryTypes(Object types, Map<Object, Object> copies) {
        scala.collection.immutable.Map<Expression, CypherType> expressionTypes =
                (scala.collection.immutable.Map<Expression, CypherType>) types;
        scala.collection.immutable.Map<Expression, CypherType> carried = expressionTypes;
        for (Map.Entry<Object, Object> pair : copies.entrySet()) {
            if (pair.getKey() == pair.getValue()) {
                continue;
            }
            Option<CypherType> type = expressionTypes.get((Expression) pair.getKey());
            if (type.isDefined()) {
                carried = carried.$plus(new Tuple2<>((Expression) pair.getValue(), type.get()));
            }
        }
        return carried;
    }

    /**
     * Returns a copy of the given parameters in which folded temporal values
     * are replaced by a printable constructor form, so the string translation
     * stays executable Groovy instead of rendering a date through toString.
     */
    public static Map<String, Object> explainParameters(Map<String, Object> parameters) {
        boolean folded = false;
        for (String name : parameters.keySet()) {
            if (name.startsWith(SYNTHETIC_PREFIX)) {
                folded = true;
                break;
            }
        }
        if (!folded) {
            return parameters;
        }
        Map<String, Object> explainable = new LinkedHashMap<>(parameters);
        for (Map.Entry<String, Object> entry : explainable.entrySet()) {
            if (entry.getKey().startsWith(SYNTHETIC_PREFIX) &&
                entry.getValue() instanceof Date) {
                entry.setValue(new GroovyDate((Date) entry.getValue()));
            }
        }
        return explainable;
    }

    /**
     * Renders a folded date as an explicit constructor so that the EXPLAIN
     * translation of the string writer stays valid Groovy.
     */
    private static final class GroovyDate {

        private final Date date;

        private GroovyDate(Date date) {
            this.date = date;
        }

        @Override
        public String toString() {
            return "new Date(" + this.date.getTime() + "L)";
        }
    }

    private static final class TemporalFold {

        private final Map<String, Object> parameters;
        private final Map<String, Object> bindings = new LinkedHashMap<>();
        // Identity pairs of every visited expression and its rebuilt copy;
        // the inferred types travel to the copies after the fold.
        private final Map<Object, Object> copies = new IdentityHashMap<>();
        // One clock read per statement: the bare constructors of a query
        // derive from a single instant.
        private final Instant now = Instant.now();
        private int counter = 0;

        private TemporalFold(Map<String, Object> parameters) {
            this.parameters = parameters;
        }

        /**
         * Returns a copy of the node with temporal constructors folded in, or
         * the same instance when the subtree does not change. Subtrees that
         * cannot be rebuilt are shared with the original statement, so the
         * copy stays minimal.
         */
        private Object copy(Object node) throws Exception {
            Object copied = copyNode(node);
            if (node instanceof Expression) {
                this.copies.put(node, copied);
            }
            return copied;
        }

        private Object copyNode(Object node) throws Exception {
            if (node == null || node instanceof String || node instanceof Number ||
                node instanceof Boolean || node instanceof Character) {
                return node;
            }
            if (node instanceof FunctionInvocation functionInvocation) {
                Object folded = foldCall(functionInvocation);
                if (folded != null) {
                    return folded;
                }
            }
            if (node instanceof Option<?> option) {
                if (!option.isEmpty()) {
                    Object value = option.get();
                    Object copied = copy(value);
                    if (copied != value) {
                        return new Some<>(copied);
                    }
                }
                return node;
            }
            if (node instanceof scala.collection.IndexedSeq<?> indexedSeq) {
                return copyIndexedSeq(indexedSeq);
            }
            if (node instanceof Seq<?> matchedSeq) {
                return copySeq(matchedSeq);
            }
            if (node instanceof scala.collection.immutable.Set<?> matchedSet) {
                return copySet(matchedSet);
            }
            if (node instanceof Tuple2<?, ?> tuple) {
                Object first = copy(tuple._1());
                Object second = copy(tuple._2());
                return first == tuple._1() && second == tuple._2() ? node : new Tuple2<>(first, second);
            }
            if (node instanceof Product matchedProduct) {
                return rebuild(matchedProduct);
            }
            return node;
        }

        private Object foldCall(FunctionInvocation call) {
            // Function names resolve case-insensitively, like the parser and
            // the translator do, so match the lower-case spelling.
            String name = call.name().toLowerCase(Locale.ROOT);
            if (!DATE.equals(name) && !DATETIME.equals(name) && !LOCALDATETIME.equals(name)) {
                return null;
            }
            if (call.distinct() || !call.namespace().parts().isEmpty()) {
                return null;
            }
            Seq<?> arguments = call.args();
            Date value;
            if (arguments.isEmpty()) {
                value = currentValue(name);
            } else if (arguments.size() == 1) {
                String literal = literalOf(arguments.head());
                if (literal == null) {
                    return null;
                }
                value = parse(name, literal);
                if (value == null) {
                    return null;
                }
            } else {
                return null;
            }
            String binding = nextBindingName(name);
            this.bindings.put(binding, value);
            return new Parameter(binding, AnyType.instance(), call.position());
        }

        private String nextBindingName(String name) {
            String binding = SYNTHETIC_PREFIX + name + '.' + this.counter++;
            while (this.parameters.containsKey(binding) ||
                   this.bindings.containsKey(binding)) {
                binding = SYNTHETIC_PREFIX + name + '.' + this.counter++;
            }
            return binding;
        }

        private String literalOf(Object argument) {
            if (argument instanceof StringLiteral stringLiteral) {
                return stringLiteral.value();
            }
            if (argument instanceof Parameter matchedParameter) {
                Object bound = this.parameters.get(matchedParameter.name());
                if (bound instanceof String matchedString) {
                    return matchedString;
                }
            }
            return null;
        }

        private Date currentValue(String name) {
            if (DATE.equals(name)) {
                return Date.from(LocalDate.ofInstant(this.now, ZoneId.systemDefault())
                                         .atStartOfDay(ZoneId.systemDefault())
                                         .toInstant());
            }
            return Date.from(this.now);
        }

        private static Date parse(String name, String literal) {
            // ISO parsing reads the fractional part as a fraction of a
            // second, like Cypher and the server's own date parsing do.
            try {
                if (DATE.equals(name)) {
                    return Date.from(LocalDate.parse(literal)
                                         .atStartOfDay(ZoneId.systemDefault())
                                         .toInstant());
                }
                return Date.from(LocalDateTime.parse(literal)
                                     .atZone(ZoneId.systemDefault())
                                     .toInstant());
            } catch (DateTimeParseException e) {
                return null;
            }
        }

        private Object copyIndexedSeq(scala.collection.IndexedSeq<?> seq) throws Exception {
            List<Object> copies = new ArrayList<>(seq.size());
            boolean changed = false;
            scala.collection.Iterator<?> iterator = seq.iterator();
            while (iterator.hasNext()) {
                Object element = iterator.next();
                Object copied = copy(element);
                changed |= copied != element;
                copies.add(copied);
            }
            if (!changed) {
                return seq;
            }
            return scala.collection.immutable.IndexedSeq$.MODULE$.apply(JavaConverters.asScalaBuffer(copies));
        }

        private Object copySeq(Seq<?> seq) throws Exception {
            List<Object> copies = new ArrayList<>(seq.size());
            boolean changed = false;
            scala.collection.Iterator<?> iterator = seq.iterator();
            while (iterator.hasNext()) {
                Object element = iterator.next();
                Object copied = copy(element);
                changed |= copied != element;
                copies.add(copied);
            }
            if (!changed) {
                return seq;
            }
            return JavaConverters.asScalaBuffer(copies).toList();
        }

        private Object copySet(scala.collection.immutable.Set<?> set) throws Exception {
            List<Object> copies = new ArrayList<>(set.size());
            boolean changed = false;
            scala.collection.Iterator<?> iterator = set.iterator();
            while (iterator.hasNext()) {
                Object element = iterator.next();
                Object copied = copy(element);
                changed |= copied != element;
                copies.add(copied);
            }
            if (!changed) {
                return set;
            }
            return JavaConverters.asScalaBuffer(copies).toSet();
        }

        private Object rebuild(Product product) throws Exception {
            int arity = product.productArity();
            Object[] elements = new Object[arity];
            boolean changed = false;
            for (int i = 0; i < arity; i++) {
                Object element = product.productElement(i);
                Object copied = copy(element);
                changed |= copied != element;
                elements[i] = copied;
            }
            if (!changed) {
                return product;
            }
            Constructor<?> constructor = constructorOf(product.getClass(), arity);
            constructor.setAccessible(true);
            Object[] arguments = new Object[constructor.getParameterCount()];
            System.arraycopy(elements, 0, arguments, 0, arity);
            if (constructor.getParameterCount() == arity + 1) {
                arguments[arity] = product.getClass().getMethod("position").invoke(product);
            }
            return constructor.newInstance(arguments);
        }

        private static Constructor<?> constructorOf(Class<?> clazz, int arity) {
            for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
                if (constructor.getParameterCount() == arity + 1) {
                    Class<?>[] types = constructor.getParameterTypes();
                    if (InputPosition.class.isAssignableFrom(types[arity])) {
                        return constructor;
                    }
                }
            }
            for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
                if (constructor.getParameterCount() == arity) {
                    return constructor;
                }
            }
            throw new IllegalStateException("No matching constructor in " + clazz.getName());
        }
    }
}
