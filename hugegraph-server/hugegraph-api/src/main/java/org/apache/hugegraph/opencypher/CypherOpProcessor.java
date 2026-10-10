/*
 * Copyright (c) 2018-2019 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode.SERVER_ERROR;
import static org.opencypher.gremlin.translation.StatementOption.EXPLAIN;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.ir.TranslationWriter;
import org.opencypher.gremlin.translation.ir.model.GremlinStep;
import org.opencypher.gremlin.translation.translator.Translator;
import org.opencypher.gremlin.translation.traversal.TraversalGremlinBindings;
import org.opencypher.gremlin.translation.traversal.TraversalGremlinSteps;
import org.opencypher.gremlin.traversal.ParameterNormalizer;
import org.opencypher.gremlin.traversal.ProcedureContext;
import org.opencypher.gremlin.traversal.ReturnNormalizer;
import org.opencypher.v9_0.expressions.Parameter;
import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import scala.Product;
import scala.collection.Seq;

/**
 * Description of the modifications:
 * <p>
 * 1) Changed the method signature to adopt the gremlin-server Context API.
 * <pre>
 * public Optional<ThrowingConsumer<Context>> selectOther(RequestMessage requestMessage)
 * -->
 * public Optional<ThrowingConsumer<Context>> selectOther(Context ctx)
 * </pre>
 * </p>
 * <p>
 * 2) Changed the package name.
 * <pre>
 * org.opencypher.gremlin.server.op.cypher
 * -->
 * org.apache.hugegraph.opencypher
 * </pre>
 * </p>
 * <p>
 * 3) Set the logger level from info to trace
 * </p>
 * <p>
 * {@link OpProcessor} implementation for processing Cypher {@link RequestMessage}s:
 * <pre>
 * {
 *   "requestId": "&lt;some UUID&gt;",
 *   "op": "eval",
 *   "processor": "cypher",
 *   "args": { "gremlin": "&lt;CYPHER QUERY&gt;" }
 * }
 * </pre>
 */
public class CypherOpProcessor extends AbstractEvalOpProcessor {

    private static final String DEFAULT_TRANSLATOR_DEFINITION =
            "gremlin+cfog_server_extensions+inline_parameters";

    // translation 1.0.4 uses this string as null and cannot preserve it as bound data.
    private static final String RESERVED_NULL_VALUE = "  cypher.null";

    private static final Logger logger = getLogger(CypherOpProcessor.class);

    public CypherOpProcessor() {
        super(true);
    }

    @Override
    protected Optional<ThrowingConsumer<Context>> validateEvalMessage(RequestMessage message)
            throws OpProcessorException {
        Object query = message.getArgs().get(Tokens.ARGS_GREMLIN);
        String error = null;
        if (!(query instanceof String) || ((String) query).isBlank()) {
            error = "The Cypher query must be a nonblank string";
        } else if (message.getArgs().containsKey(Tokens.ARGS_BINDINGS)) {
            Object supplied = message.getArgs().get(Tokens.ARGS_BINDINGS);
            if (!(supplied instanceof Map)) {
                error = "Cypher bindings must be a map";
            } else {
                Map<?, ?> bindings = (Map<?, ?>) supplied;
                if (bindings.size() > this.maxParameters) {
                    error = "Cypher bindings exceed the maximum parameter count " + this.maxParameters;
                } else {
                    // Cypher parameters do not enter the Gremlin script namespace.
                    for (Object key : bindings.keySet()) {
                        if (!(key instanceof String)) {
                            error = "Cypher binding keys must be nonnull strings";
                            break;
                        }
                    }
                    if (error == null && containsReservedNullValue(bindings)) {
                        error = "Cypher bindings contain a string reserved for null by the translator";
                    }
                }
            }
        }
        if (error != null) {
            throw new OpProcessorException(error, ResponseMessage.build(message)
                    .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                    .statusMessage(error).create());
        }
        return empty();
    }

    private static boolean containsReservedNullValue(Map<?, ?> bindings) {
        Deque<Iterator<?>> pending = new ArrayDeque<>();
        pending.push(bindings.values().iterator());
        while (!pending.isEmpty()) {
            Iterator<?> values = pending.peek();
            if (!values.hasNext()) {
                pending.pop();
                continue;
            }
            Object value = values.next();
            if (RESERVED_NULL_VALUE.equals(value)) {
                return true;
            }
            if (value instanceof Map) {
                pending.push(((Map<?, ?>) value).values().iterator());
            } else if (value instanceof Collection) {
                pending.push(((Collection<?>) value).iterator());
            }
        }
        return false;
    }

    @Override
    public String getName() {
        return "cypher";
    }

    @Override
    public ThrowingConsumer<Context> getEvalOp() {
        return this::evalCypher;
    }

    @Override
    public Optional<ThrowingConsumer<Context>> selectOther(Context ctx) {
        return empty();
    }

    private void evalCypher(Context context) throws OpProcessorException {
        Map<String, Object> args = context.getRequestMessage().getArgs();
        String cypher = (String) args.get(Tokens.ARGS_GREMLIN);
        logger.trace("Cypher: {}", cypher.replaceAll("\n", " "));

        GraphTraversalSource gts = traversal(context);
        DefaultGraphTraversal g = new DefaultGraphTraversal(gts.clone());
        Map<String, Object> parameters = ParameterNormalizer.normalize(getParameters(args));
        ProcedureContext procedureContext = ProcedureContext.global();
        CypherAst ast = CypherAst.parse(cypher, parameters, procedureContext.getSignatures());
        validateParameters(ast, parameters);

        String translatorDefinition = getTranslatorDefinition(context);

        Translator<String, GroovyPredicate> strTranslator = Translator.builder()
                                                                      .gremlinGroovy()
                                                                      .build(translatorDefinition);

        Translator<GraphTraversal, P> traversalTranslator = Translator.builder()
                                                                      .custom(new TraversalGremlinSteps(g),
                                                                              new CypherGremlinPredicates(),
                                                                              new TraversalGremlinBindings())
                                                                      .build(translatorDefinition);

        Seq<GremlinStep> ir = ast.translate(strTranslator.flavor(),
                                            strTranslator.features(), procedureContext);

        String gremlin = TranslationWriter.write(ir, strTranslator, parameters);
        logger.trace("Gremlin: {}", gremlin);

        if (ast.getOptions().contains(EXPLAIN)) {
            explainQuery(context, ast, gremlin);
            return;
        }

        GraphTraversal<?, ?> traversal = TranslationWriter.write(ir, traversalTranslator,
                                                                 parameters);
        ReturnNormalizer returnNormalizer = ReturnNormalizer.create(ast.getReturnTypes());
        Iterator normalizedTraversal = returnNormalizer.normalize(traversal);
        handleTraversal(context, normalizedTraversal, traversal);
    }

    private static void validateParameters(CypherAst ast, Map<String, Object> parameters) {
        // Inspect parsed parameter nodes: dollar signs in literals/comments are not bindings.
        // The upstream translator otherwise silently turns an absent binding into null.
        Deque<Object> pending = new ArrayDeque<>();
        pending.push(ast.statement());
        while (!pending.isEmpty()) {
            Object node = pending.pop();
            if (node instanceof Parameter) {
                String name = ((Parameter) node).name();
                if (!parameters.containsKey(name)) {
                    throw new IllegalArgumentException("Missing Cypher parameter: " + name);
                }
                continue;
            }
            scala.collection.Iterator<?> children;
            if (node instanceof scala.collection.Iterable) {
                children = ((scala.collection.Iterable<?>) node).iterator();
            } else if (node instanceof Product) {
                children = ((Product) node).productIterator();
            } else {
                continue;
            }
            while (children.hasNext()) {
                Object child = children.next();
                if (child != null) {
                    pending.push(child);
                }
            }
        }
    }

    private GraphTraversalSource traversal(Context context) throws OpProcessorException {
        RequestMessage msg = context.getRequestMessage();
        GraphManager graphManager = context.getGraphManager();

        Optional<Map<String, String>> aliasesOptional = msg.optionalArgs(Tokens.ARGS_ALIASES);
        String gAlias = aliasesOptional.map(alias -> alias.get(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS))
                                       .orElse(null);

        if (gAlias == null) {
            return graphManager.getGraphNames().stream()
                               .sorted()
                               .findFirst()
                               .map(graphManager::getGraph)
                               .map(Graph::traversal)
                               .orElseThrow(() -> opProcessorException(msg, "No graphs found on " +
                                                                            "the server"));
        }

        Graph graph = graphManager.getGraph(gAlias);
        if (graph != null) {
            return graph.traversal();
        }

        TraversalSource traversalSource = graphManager.getTraversalSource(gAlias);
        if (traversalSource instanceof GraphTraversalSource) {
            return (GraphTraversalSource) traversalSource;
        }

        throw opProcessorException(msg, "Traversable alias '" + gAlias + "' not found");
    }

    private OpProcessorException opProcessorException(RequestMessage msg, String errorMessage) {
        return new OpProcessorException(errorMessage, ResponseMessage.build(msg)
                                                                     .code(SERVER_ERROR)
                                                                     .statusMessage(errorMessage)
                                                                     .create());
    }

    private void handleTraversal(Context context, Iterator traversal,
                                 GraphTraversal<?, ?> source) {
        RequestMessage msg = context.getRequestMessage();
        final long timeout = msg.getArgs().containsKey(Tokens.ARGS_EVAL_TIMEOUT)
                             ? ((Number) msg.getArgs().get(Tokens.ARGS_EVAL_TIMEOUT)).longValue()
                             : context.getSettings().evaluationTimeout;

        FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            try (GraphTraversal<?, ?> ignored = source) {
                // The parent commits before sending the terminal success response. Both
                // iteration and failure rollback must run on this transaction-owning thread.
                super.handleIterator(context, traversal);
            } catch (Error error) {
                try {
                    attemptRollback(msg, context.getGraphManager(),
                                    context.getSettings().strictTransactionManagement);
                } catch (Exception rollbackFailure) {
                    error.addSuppressed(rollbackFailure);
                }
                // FutureTask retains the error, so send a terminal response here as well.
                // This also covers errors raised after a timeout has cancelled the task.
                logger.error("Fatal error during traversal iteration", error);
                context.writeAndFlush(ResponseMessage.build(msg)
                                                     .code(SERVER_ERROR)
                                                     .statusMessage(error.getMessage())
                                                     .statusAttributeException(error)
                                                     .create());
                throw error;
            } catch (Exception ex) {
                try {
                    attemptRollback(msg, context.getGraphManager(),
                                    context.getSettings().strictTransactionManagement);
                } catch (Exception rollbackFailure) {
                    ex.addSuppressed(rollbackFailure);
                }
                String errorMessage = getErrorMessage(msg, ex);

                logger.error("Error during traversal iteration", ex);
                context.writeAndFlush(ResponseMessage.build(msg)
                                                 .code(SERVER_ERROR)
                                                 .statusMessage(errorMessage)
                                                 .statusAttributeException(ex)
                                                 .create());
            }
            return null;
        }
        );

        context.getGremlinExecutor().getExecutorService().execute(evalFuture);
        if (timeout > 0) {
            context.getScheduledExecutorService().schedule(
                    () -> evalFuture.cancel(true)
                    , timeout, TimeUnit.MILLISECONDS);
        }

    }

    private String getErrorMessage(RequestMessage msg, Exception ex) {
        if (ex instanceof InterruptedException || ex instanceof TraversalInterruptedException) {
            return String.format("A timeout occurred during traversal evaluation of [%s] " +
                                 "- consider increasing the limit given to scriptEvaluationTimeout",
                                 msg);
        } else {
            return ex.getMessage();
        }
    }

    private void explainQuery(Context context, CypherAst ast, String gremlin) {
        Map<String, Object> explanation = new LinkedHashMap<>();
        explanation.put("translation", gremlin);
        explanation.put("options", ast.getOptions().toString());

        ResponseMessage explainMsg = ResponseMessage.build(context.getRequestMessage())
                                                    .code(ResponseStatusCode.SUCCESS)
                                                    .statusMessage("OK")
                                                    .result(singletonList(explanation))
                                                    .create();

        ChannelHandlerContext ctx = context.getChannelHandlerContext();
        ctx.writeAndFlush(explainMsg);
    }

    @Override
    public void close() {
        // do nothing = no resources to release
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getParameters(Map<String, Object> args) {
        if (args.containsKey(Tokens.ARGS_BINDINGS)) {
            return (Map<String, Object>) args.get(Tokens.ARGS_BINDINGS);
        } else {
            return new HashMap<>();
        }
    }

    private String getTranslatorDefinition(Context context) {
        Map<String, Object> config = context.getSettings()
                                            .optionalProcessor(CypherOpProcessor.class)
                                            .map(p -> p.config)
                                            .orElse(emptyMap());

        HashSet<String> properties = new HashSet<>(config.keySet());
        properties.remove("translatorDefinition");
        properties.remove("translatorFeatures");
        if (!properties.isEmpty()) {
            throw new IllegalStateException("Unknown configuration parameters " +
                                            "found for CypherOpProcessor: " + properties);
        }

        return config.getOrDefault("translatorDefinition", DEFAULT_TRANSLATOR_DEFINITION)
               + "+" + config.getOrDefault("translatorFeatures", "");
    }

}
