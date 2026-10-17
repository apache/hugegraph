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
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.v9_0.expressions.FunctionInvocation;
import org.opencypher.v9_0.expressions.Parameter;
import org.opencypher.v9_0.expressions.StringLiteral;
import org.opencypher.v9_0.util.InputPosition;
import org.opencypher.v9_0.util.symbols.AnyType;
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

    private static final String[] DATE_FORMATS = {"yyyy-MM-dd"};
    private static final String[] DATETIME_FORMATS = {"yyyy-MM-dd'T'HH:mm:ss",
                                                      "yyyy-MM-dd'T'HH:mm:ss.SSS"};

    // Two leading spaces: parameter names coming from a Cypher query can
    // never collide with this prefix, like the translator's own reserved names.
    private static final String SYNTHETIC_PREFIX = "  cypher.fold.";

    private CypherTemporalFunctions() {
    }

    /**
     * Folds the supported temporal constructors in the parsed statement and
     * returns the effective request parameters for translation: the same
     * instance when nothing was folded, a copy carrying the synthetic
     * bindings otherwise. The parsed statement is only replaced when the
     * rewrite completed without errors, so a fold failure leaves the query
     * exactly as it was parsed.
     */
    public static Map<String, Object> fold(CypherAst ast, Map<String, Object> parameters) {
        TemporalFold fold = new TemporalFold(parameters);
        try {
            Field statementField = CypherAst.class.getDeclaredField("statement");
            Field parametersField = CypherAst.class.getDeclaredField("parameters");
            statementField.setAccessible(true);
            parametersField.setAccessible(true);

            Object folded = fold.copy(ast.statement());
            if (fold.bindings.isEmpty()) {
                return parameters;
            }
            Object merged = mergeParameters(parametersField.get(ast), fold.bindings);
            statementField.set(ast, folded);
            parametersField.set(ast, merged);
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

    private static final class TemporalFold {

        private final Map<String, Object> parameters;
        private final Map<String, Object> bindings = new LinkedHashMap<>();
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
            if (node == null || node instanceof String || node instanceof Number ||
                node instanceof Boolean || node instanceof Character) {
                return node;
            }
            if (node instanceof FunctionInvocation) {
                Object folded = foldCall((FunctionInvocation) node);
                if (folded != null) {
                    return folded;
                }
            }
            if (node instanceof Option) {
                Option<?> option = (Option<?>) node;
                if (!option.isEmpty()) {
                    Object value = option.get();
                    Object copied = copy(value);
                    if (copied != value) {
                        return new Some<>(copied);
                    }
                }
                return node;
            }
            if (node instanceof scala.collection.IndexedSeq) {
                return copyIndexedSeq((scala.collection.IndexedSeq<?>) node);
            }
            if (node instanceof Seq) {
                return copySeq((Seq<?>) node);
            }
            if (node instanceof scala.collection.immutable.Set) {
                return copySet((scala.collection.immutable.Set<?>) node);
            }
            if (node instanceof Tuple2) {
                Tuple2<?, ?> tuple = (Tuple2<?, ?>) node;
                Object first = copy(tuple._1());
                Object second = copy(tuple._2());
                return first == tuple._1() && second == tuple._2() ? node : new Tuple2<>(first, second);
            }
            if (node instanceof Product) {
                return rebuild((Product) node);
            }
            return node;
        }

        private Object foldCall(FunctionInvocation call) {
            String name = call.name();
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
            String binding = SYNTHETIC_PREFIX + name + '.' + counter++;
            this.bindings.put(binding, value);
            return new Parameter(binding, AnyType.instance(), call.position());
        }

        private String literalOf(Object argument) {
            if (argument instanceof StringLiteral) {
                return ((StringLiteral) argument).value();
            }
            if (argument instanceof Parameter) {
                Object bound = this.parameters.get(((Parameter) argument).name());
                if (bound instanceof String) {
                    return (String) bound;
                }
            }
            return null;
        }

        private static Date currentValue(String name) {
            if (DATE.equals(name)) {
                return Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
            }
            return new Date();
        }

        private static Date parse(String name, String literal) {
            String[] patterns = DATE.equals(name) ? DATE_FORMATS : DATETIME_FORMATS;
            for (String pattern : patterns) {
                SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.ROOT);
                format.setLenient(false);
                // Require the literal to match the pattern completely, so a
                // shorter pattern cannot silently drop a trailing part.
                ParsePosition position = new ParsePosition(0);
                Date parsed = format.parse(literal, position);
                if (parsed != null && position.getIndex() == literal.length()) {
                    return parsed;
                }
            }
            return null;
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
