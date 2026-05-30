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
//import static org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.SERVER_ERROR;
import static org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode.SERVER_ERROR;
import static org.opencypher.gremlin.translation.StatementOption.EXPLAIN;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

//import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.util.Tokens;
//import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
//import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
//import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
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
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.ir.TranslationWriter;
import org.opencypher.gremlin.translation.ir.model.GremlinStep;
import org.opencypher.gremlin.translation.translator.Translator;
import org.opencypher.gremlin.traversal.ParameterNormalizer;
import org.opencypher.gremlin.traversal.ProcedureContext;
import org.opencypher.gremlin.traversal.ReturnNormalizer;
import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import scala.collection.Seq;

/**
 * Description of the modifications:
 * <p>
 * 1) Changed the method signature to adopt the gremlin-server 3.5.1.
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

    private static final Logger logger = getLogger(CypherOpProcessor.class);

    public CypherOpProcessor() {
        super(true);
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

        String translatorDefinition = getTranslatorDefinition(context);

        Translator<String, GroovyPredicate> strTranslator = Translator.builder()
                                                                      .gremlinGroovy()
                                                                      .build(translatorDefinition);

        Translator<GraphTraversal, P> traversalTranslator = Translator.builder()
                                                                      .traversal(g)
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
        inTransaction(gts, () -> handleIterator(context, normalizedTraversal));
    }

    private void inTransaction(GraphTraversalSource gts, Runnable runnable) {
        Graph graph = gts.getGraph();
        boolean supportsTransactions = graph.features().graph().supportsTransactions();
        if (!supportsTransactions) {
            runnable.run();
            return;
        }

        try {
            graph.tx().open();
            runnable.run();
            graph.tx().commit();
        } catch (Exception e) {
            if (graph.tx().isOpen()) {
                graph.tx().rollback();
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

    @Override
    protected void handleIterator(Context context, Iterator traversal) {
        RequestMessage msg = context.getRequestMessage();
        final long timeout = msg.getArgs().containsKey(Tokens.ARGS_EVAL_TIMEOUT)
                             ? ((Number) msg.getArgs().get(Tokens.ARGS_EVAL_TIMEOUT)).longValue()
                             : context.getSettings().evaluationTimeout;

        FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            try {
                super.handleIterator(context, traversal);
            } catch (Exception ex) {
                String errorMessage = getErrorMessage(msg, ex);

                logger.error("Error during traversal iteration", ex);
                ChannelHandlerContext ctx = context.getChannelHandlerContext();
                ctx.writeAndFlush(ResponseMessage.build(msg)
                                                 .code(SERVER_ERROR)
                                                 .statusMessage(errorMessage)
                                                 .statusAttributeException(ex)
                                                 .create());
            }
            return null;
        }
        );

        final Future<?> executionFuture = context.getGremlinExecutor()
                                                 .getExecutorService().submit(evalFuture);
        if (timeout > 0) {
            context.getScheduledExecutorService().schedule(
                    () -> executionFuture.cancel(true)
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
