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

package org.apache.hugegraph.k8s;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hugegraph.HugeException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

/**
 * Read k8s configurations
 *
 * @author Scorpiour
 */
public class K8sRegister {

    private static final String CA_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
    private static final String KUBE_TOKEN_FILE =
            "/var/run/secrets/kubernetes.io/serviceaccount/token";
    private static final String NAMESPACE_FILE =
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    private static final String APP_NAME = System.getenv("APP_NAME");
    private static final String SERVICE_HOST = System.getenv("KUBERNETES_SERVICE_HOST");
    private static final String CERT_TYPE = "X.509";
    private static final String KEY_STORE_TYPE = "JKS";
    private static final String CERT_ALIAS = "ANY_CERTIFICATE_ALIAS";
    private static final String SSL_PROTO = "TLS";
    private HttpClient httpClient = null;

    private K8sRegister() {

    }

    public static K8sRegister instance() {
        return SingletonHolder.instance;
    }

    private String getKubeToken() throws Exception {
        File file = new File(KUBE_TOKEN_FILE);
        if (file.canRead()) {
            FileReader reader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(reader);
            try {
                String token = bufferedReader.readLine();
                token = token.trim();
                return token;
            } finally {
                bufferedReader.close();
            }
        }
        throw new HugeException("Kubernetes token file doesn't exist");

    }

    private String getKubeNamespace() throws Exception {
        File file = new File(NAMESPACE_FILE);
        if (file.canRead()) {
            FileReader reader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(reader);
            try {
                String namespace = bufferedReader.readLine();
                namespace = namespace.trim();
                return namespace;
            } finally {
                bufferedReader.close();
            }
        }
        throw new HugeException("Kubernetes namespace file doesn't exist");
    }

    public synchronized void initHttpClient() throws Exception {
        if (null != httpClient) {
            return;
        }

        CertificateFactory factory = CertificateFactory.getInstance(CERT_TYPE);
        Certificate cert = factory.generateCertificate(new FileInputStream(CA_FILE));

        KeyStore keyStore = KeyStore.getInstance(KEY_STORE_TYPE);
        keyStore.load(null, null);
        keyStore.setCertificateEntry(CERT_ALIAS, cert);

        TrustManagerFactory managerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        managerFactory.init(keyStore);

        SSLContext context = SSLContext.getInstance(SSL_PROTO);
        context.init(null, managerFactory.getTrustManagers(), null);

        HttpClient client = HttpClients.custom().setSSLContext(context).build();
        this.httpClient = client;
    }

    public String loadConfigStr() throws Exception {

        String token = this.getKubeToken();
        String namespace = this.getKubeNamespace();

        String url = String.format(
                "https://%s/api/v1/namespaces/%s/services/%s",
                SERVICE_HOST,
                namespace,
                APP_NAME);
        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        get.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

        HttpResponse response = httpClient.execute(get);
        String configMap = EntityUtils.toString(response.getEntity());

        return configMap;
    }

    private static class SingletonHolder {

        public final static K8sRegister instance = new K8sRegister();
    }
}
