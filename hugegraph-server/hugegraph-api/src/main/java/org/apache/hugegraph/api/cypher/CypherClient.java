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

package org.apache.hugegraph.api.cypher;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.configuration2.Configuration;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
//import org.apache.tinkerpop.gremlin.driver.Tokens;
//import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.slf4j.Logger;

@ThreadSafe
public final class CypherClient {

    private static final Logger LOG = Log.logger(CypherClient.class);
    private final Supplier<Configuration> configurationSupplier;
    private String userName;
    private String password;
    private String token;

    CypherClient(String userName, String password,
                 Supplier<Configuration> configurationSupplier) {
        this.userName = userName;
        this.password = password;
        this.configurationSupplier = configurationSupplier;
    }

    CypherClient(String token, Supplier<Configuration> configurationSupplier) {
        this.token = token;
        this.configurationSupplier = configurationSupplier;
    }

    public CypherModel submitQuery(String cypherQuery, @Nullable Map<String, String> aliases) {
        E.checkArgument(cypherQuery != null && !cypherQuery.isEmpty(),
                        "The cypher-query parameter can't be null or empty");

        Cluster cluster = Cluster.open(getConfig());
        Client client = cluster.connect();

        if (aliases != null && !aliases.isEmpty()) {
            client = client.alias(aliases);
        }

        RequestMessage request = createRequest(cypherQuery);
        CypherModel res;

        try {
            List<Object> list = this.doQueryList(client, request);
            res = CypherModel.dataOf(request.getRequestId().toString(), list);
        } catch (Exception e) {
            LOG.error(String.format("Failed to submit cypher-query: [ %s ], caused by:",
                                    cypherQuery), e);
            res = CypherModel.failOf(request.getRequestId().toString(), e.getMessage());
        } finally {
            client.close();
            cluster.close();
        }

        return res;
    }

    private RequestMessage createRequest(String cypherQuery) {
        return RequestMessage.build(Tokens.OPS_EVAL)
                             .processor("cypher")
                             .add(Tokens.ARGS_GREMLIN, cypherQuery)
                             .create();
    }

    private List<Object> doQueryList(Client client, RequestMessage request)
            throws ExecutionException, InterruptedException {
        ResultSet results = client.submitAsync(request).get();

        Iterator<Result> iter = results.iterator();
        List<Object> list = new LinkedList<>();

        while (iter.hasNext()) {
            Result data = iter.next();
            list.add(data.getObject());
        }

        return list;
    }

    /**
     * As Sasl does not support a token, which is a coded string to indicate a legal user,
     * we had to use a trick to fix it. When the token is set, the password will be set to
     * an empty string, which is an uncommon value under normal conditions.
     * The token will then be transferred through the userName-property.
     * To see org.apache.hugegraph.auth.StandardAuthenticator.TokenSaslAuthenticator
     */
    private Configuration getConfig() {
        Configuration conf = this.configurationSupplier.get();
        conf.addProperty("username", this.token == null ? this.userName : this.token);
        conf.addProperty("password", this.token == null ? this.password : "");

        return conf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CypherClient that = (CypherClient) o;

        return Objects.equals(userName, that.userName) &&
               Objects.equals(password, that.password) &&
               Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, password, token);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("CypherClient{");
        builder.append("userName='").append(userName).append('\'')
               .append(", token='").append(token).append('\'').append('}');

        return builder.toString();
    }
}
