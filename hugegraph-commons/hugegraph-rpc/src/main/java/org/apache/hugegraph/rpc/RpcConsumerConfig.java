/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.rpc.bootstrap.Bootstraps;
import com.alipay.sofa.rpc.bootstrap.ConsumerBootstrap;
import com.alipay.sofa.rpc.client.AbstractCluster;
import com.alipay.sofa.rpc.client.Cluster;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.RpcErrorType;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.core.response.SofaResponse;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.ext.ExtensionLoaderFactory;

public class RpcConsumerConfig implements RpcServiceConfig4Client {

    private final HugeConfig conf;
    private final String remoteUrls;
    private final Map<String, ConsumerConfig<?>> configs;
    private final List<ConsumerBootstrap<?>> bootstraps;

    static {
        ExtensionLoaderFactory.getExtensionLoader(Cluster.class)
                              .loadExtension(FanoutCluster.class);
    }

    public RpcConsumerConfig(HugeConfig config, String remoteUrls) {
        RpcCommonConfig.initRpcConfigs(config);
        this.conf = config;
        this.remoteUrls = remoteUrls;
        this.configs = new ConcurrentHashMap<>();
        this.bootstraps = new CopyOnWriteArrayList<>();
    }

    @Override
    public <T> T serviceProxy(String interfaceId) {
        return this.serviceProxy(null, interfaceId);
    }

    @Override
    public <T> T serviceProxy(String graph, String interfaceId) {
        String serviceId = graph == null ? interfaceId : interfaceId + ":" + graph;
        AtomicReference<T> proxy = new AtomicReference<>();
        this.configs.compute(serviceId, (key, existing) -> {
            @SuppressWarnings("unchecked")
            ConsumerConfig<T> config = existing == null ? this.consumerConfig(graph, interfaceId) :
                                       (ConsumerConfig<T>) existing;
            ConsumerBootstrap<T> bootstrap = Bootstraps.from(config);
            try {
                proxy.set(bootstrap.refer());
                this.bootstraps.add(bootstrap);
            } catch (RuntimeException | Error failure) {
                // Retain the cleanup owner even when refer never returned a proxy.
                this.bootstraps.add(bootstrap);
                throwFailure(this.cleanup(bootstrap, failure));
            }
            return config;
        });
        return proxy.get();
    }

    @Override
    public boolean removeServiceProxy(String graph, String interfaceId, Object proxy) {
        String serviceId = graph == null ? interfaceId : interfaceId + ":" + graph;
        boolean[] removed = {false};
        this.configs.computeIfPresent(serviceId, (key, config) -> {
            for (ConsumerBootstrap<?> bootstrap : this.bootstraps) {
                if (bootstrap.getConsumerConfig() == config && bootstrap.getProxyIns() == proxy) {
                    Throwable failure = this.cleanup(bootstrap, null);
                    if (failure != null) {
                        throwFailure(failure);
                    }
                    removed[0] = true;
                    break;
                }
            }
            for (ConsumerBootstrap<?> bootstrap : this.bootstraps) {
                if (bootstrap.getConsumerConfig() == config) {
                    return config;
                }
            }
            return null;
        });
        return removed[0];
    }

    @Override
    public void removeAllServiceProxy() {
        Throwable failure = null;
        for (ConsumerBootstrap<?> bootstrap : this.bootstraps) {
            try {
                bootstrap.unRefer();
            } catch (RuntimeException | Error cleanup) {
                failure = addFailure(failure, cleanup);
            }
        }
        if (failure != null) {
            throwFailure(failure);
        }
    }

    public void destroy() {
        Throwable failure = null;
        for (ConsumerBootstrap<?> bootstrap : this.bootstraps) {
            failure = this.cleanup(bootstrap, failure);
        }
        if (failure != null) {
            throwFailure(failure);
        }
    }

    private Throwable cleanup(ConsumerBootstrap<?> bootstrap, Throwable failure) {
        boolean complete = true;
        try {
            bootstrap.unRefer();
        } catch (RuntimeException | Error cleanup) {
            complete = false;
            failure = addFailure(failure, cleanup);
        }
        try {
            Cluster cluster = bootstrap.getCluster();
            if (cluster != null) {
                cluster.destroy();
            }
        } catch (RuntimeException | Error cleanup) {
            complete = false;
            failure = addFailure(failure, cleanup);
        }
        if (complete) {
            this.bootstraps.remove(bootstrap);
        }
        return failure;
    }

    private static Throwable addFailure(Throwable failure, Throwable cleanup) {
        if (failure == null) {
            return cleanup;
        }
        if (failure == cleanup) {
            return failure;
        }
        if (cleanup instanceof Error && !(failure instanceof Error)) {
            cleanup.addSuppressed(failure);
            return cleanup;
        }
        failure.addSuppressed(cleanup);
        return failure;
    }

    private static void throwFailure(Throwable failure) {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw (RuntimeException) failure;
    }

    private <T> ConsumerConfig<T> consumerConfig(String graph,
                                                 String interfaceId) {
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
        } else {
            serviceId = interfaceId;
        }

        ConsumerConfig<T> consumerConfig = new ConsumerConfig<>();

        HugeConfig conf = this.conf;
        String protocol = conf.get(RpcOptions.RPC_PROTOCOL);
        int timeout = conf.get(RpcOptions.RPC_CLIENT_READ_TIMEOUT) * 1000;
        int connectTimeout = conf.get(RpcOptions
                                      .RPC_CLIENT_CONNECT_TIMEOUT) * 1000;
        int reconnectPeriod = conf.get(RpcOptions
                                       .RPC_CLIENT_RECONNECT_PERIOD) * 1000;
        int retries = conf.get(RpcOptions.RPC_CLIENT_RETRIES);
        String loadBalancer = conf.get(RpcOptions.RPC_CLIENT_LOAD_BALANCER);

        if (graph != null) {
            consumerConfig.setId(serviceId).setUniqueId(graph);
            // Default is FailoverCluster, set to FanoutCluster to broadcast
            consumerConfig.setCluster("fanout");
        }
        consumerConfig.setInterfaceId(interfaceId)
                      .setProtocol(protocol)
                      .setDirectUrl(this.remoteUrls)
                      .setTimeout(timeout)
                      .setConnectTimeout(connectTimeout)
                      .setReconnectPeriod(reconnectPeriod)
                      .setRetries(retries)
                      .setSerialization(conf.get(RpcOptions.RPC_SERIALIZATION))
                      .setLoadBalancer(loadBalancer);

        return consumerConfig;
    }

    @Extension("fanout")
    private static class FanoutCluster extends AbstractCluster {

        private static final Logger LOG = Log.logger(FanoutCluster.class);

        FanoutCluster(ConsumerBootstrap<?> consumerBootstrap) {
            super(consumerBootstrap);
        }

        @Override
        protected SofaResponse doInvoke(SofaRequest request)
                                        throws SofaRpcException {
            List<ProviderInfo> providers = this.getRouterChain()
                                               .route(request, null);
            List<SofaResponse> responses = new ArrayList<>(providers.size());
            List<SofaRpcException> excepts = new ArrayList<>(providers.size());

            for (ProviderInfo provider : providers) {
                try {
                    SofaResponse response = this.doInvoke(request, provider);
                    responses.add(response);
                } catch (SofaRpcException e) {
                    excepts.add(e);
                    LOG.warn("{}.(error {})", e.getMessage(), e.getErrorType());
                }
            }

            if (!responses.isEmpty()) {
                /*
                 * Just choose the first one as result to return, ignore others
                 * TODO: maybe more strategies should be provided
                 */
                return responses.get(0);
            } else if (!excepts.isEmpty()) {
                throw excepts.get(0);
            } else {
                assert providers.isEmpty();
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_ROUTER,
                                           "No service provider for " + method);
            }
        }

        private SofaResponse doInvoke(SofaRequest request,
                                      ProviderInfo providerInfo) {
            try {
                SofaResponse response = this.filterChain(providerInfo, request);
                if (response != null) {
                    return response;
                }
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_UNDECLARED_ERROR,
                          "Failed to call " + method + " on remote server " +
                          providerInfo + ", return null response");
            } catch (Exception e) {
                int error = RpcErrorType.CLIENT_UNDECLARED_ERROR;
                if (e instanceof SofaRpcException) {
                    error = ((SofaRpcException) e).getErrorType();
                }
                String method = methodName(request);
                throw new SofaRpcException(error,
                          "Failed to call " + method + " on remote server " +
                          providerInfo + ", caused by exception: " + e);
            }
        }

        private static String methodName(SofaRequest request) {
            return request.getInterfaceName() + "." +
                   request.getMethodName() + "()";
        }
    }
}
