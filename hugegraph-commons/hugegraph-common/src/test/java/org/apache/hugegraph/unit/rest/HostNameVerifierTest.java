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

package org.apache.hugegraph.unit.rest;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.hugegraph.rest.AbstractRestClient;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Checks the certificate identity rules of {@link AbstractRestClient.HostNameVerifier}
 * with in-memory certificates, no TLS handshake involved.
 */
public class HostNameVerifierTest {

    private static final int SAN_DNS = 2;
    private static final int SAN_IP = 7;

    @Test
    public void testConfiguredUrlDoesNotGrantTrust() throws Exception {
        SSLSession session = sessionWithSans(san(SAN_DNS, "other.example"));
        // The URL host equals the requested host, the certificate still has to match
        Assert.assertFalse(verifier("https://graph.example:8080").verify("graph.example", session));
        Assert.assertFalse(verifier("graph.example").verify("graph.example", session));
        // A URL host ending with the requested host isn't a match either
        Assert.assertFalse(verifier("https://evil-graph.example").verify("graph.example", session));
        Assert.assertTrue(verifier("https://graph.example").verify("other.example", session));
    }

    @Test
    public void testDnsSubjectAlternativeNames() throws Exception {
        SSLSession session = sessionWithSans(san(SAN_DNS, "graph.example"),
                                             san(SAN_DNS, "*.cluster.example"));
        AbstractRestClient.HostNameVerifier verifier = verifier("https://graph.example");
        Assert.assertTrue(verifier.verify("graph.example", session));
        Assert.assertTrue(verifier.verify("GRAPH.Example", session));
        Assert.assertTrue(verifier.verify("node1.cluster.example", session));

        Assert.assertFalse(verifier.verify("sub.graph.example", session));
        Assert.assertFalse(verifier.verify("raph.example", session));
        // A wildcard only covers exactly one left-most label
        Assert.assertFalse(verifier.verify("cluster.example", session));
        Assert.assertFalse(verifier.verify("a.node1.cluster.example", session));
    }

    @Test
    public void testIpSubjectAlternativeNames() throws Exception {
        SSLSession session = sessionWithSans(san(SAN_IP, "127.0.0.1"),
                                             san(SAN_DNS, "localhost"));
        AbstractRestClient.HostNameVerifier verifier = verifier("https://127.0.0.1");
        Assert.assertTrue(verifier.verify("127.0.0.1", session));
        Assert.assertTrue(verifier.verify("localhost", session));
        Assert.assertFalse(verifier.verify("127.0.0.2", session));

        // An IP host never matches a DNS entry with the same text
        SSLSession dnsOnly = sessionWithSans(san(SAN_DNS, "127.0.0.1"));
        Assert.assertFalse(verifier.verify("127.0.0.1", dnsOnly));
        // A DNS host never matches an IP entry
        SSLSession ipOnly = sessionWithSans(san(SAN_IP, "127.0.0.1"));
        Assert.assertFalse(verifier.verify("localhost", ipOnly));
    }

    @Test
    public void testCertificateWithoutSubjectAlternativeNames() throws Exception {
        X509Certificate cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(null);
        SSLSession session = sessionWith(cert);
        // The common name is not used as an identity fallback
        Assert.assertFalse(verifier("https://localhost").verify("localhost", session));
    }

    @Test
    public void testUnverifiedPeerOrInvalidHost() throws Exception {
        SSLSession unverified = Mockito.mock(SSLSession.class);
        Mockito.when(unverified.getPeerCertificates())
               .thenThrow(new SSLPeerUnverifiedException("no peer certificate"));
        Assert.assertFalse(verifier("https://localhost").verify("localhost", unverified));

        SSLSession session = sessionWithSans(san(SAN_DNS, "localhost"));
        Assert.assertFalse(verifier("https://localhost").verify("", session));
        Assert.assertFalse(verifier("https://localhost").verify("l\u00f3calhost", session));
    }

    private static AbstractRestClient.HostNameVerifier verifier(String url) {
        return new AbstractRestClient.HostNameVerifier(url);
    }

    private static List<?> san(int type, String value) {
        return Arrays.asList(type, value);
    }

    private static SSLSession sessionWithSans(List<?>... sans) throws Exception {
        X509Certificate cert = Mockito.mock(X509Certificate.class);
        List<List<?>> names = new ArrayList<>(Arrays.asList(sans));
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(names);
        return sessionWith(cert);
    }

    private static SSLSession sessionWith(X509Certificate cert) throws Exception {
        SSLSession session = Mockito.mock(SSLSession.class);
        Mockito.when(session.getPeerCertificates()).thenReturn(new Certificate[]{cert});
        return session;
    }
}
