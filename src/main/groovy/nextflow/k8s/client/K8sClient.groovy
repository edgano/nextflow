/*
 * Copyright (c) 2013-2017, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2017, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.k8s.client
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSession
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate

import groovy.json.JsonOutput
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
/**
 * Kubernetes API client
 *
 * Tip: use the following command to find out your kubernetes master node
 *   kubectl cluster-info
 *
 * See
 *   https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#-strong-api-overview-strong-
 *
 * Useful cheatsheet
 *   https://kubernetes.io/docs/reference/kubectl/cheatsheet/
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class K8sClient {

    protected ClientConfig config

    private TrustManager[] trustManagers

    private HostnameVerifier hostnameVerifier

    K8sClient() {
        this(new ClientConfig())
    }

    ClientConfig getConfig() { config }

    /**
     * Creates a kubernetes client using the configuration setting provided by the specified
     * {@link ConfigDiscovery} instance
     *
     * @param config
     */
    K8sClient(ClientConfig config) {
        this.config = config
        setupSslCert()
    }


    protected setupSslCert() {

        if( !config.verifySsl ) {
            // -- no SSL is required - use fake trust manager
            final trustAll = new X509TrustManager() {
                @Override X509Certificate[] getAcceptedIssuers() { return null }
                @Override void checkClientTrusted(X509Certificate[] certs, String authType) { }
                @Override void checkServerTrusted(X509Certificate[] certs, String authType) { }
            }

            trustManagers = [trustAll] as TrustManager[]
            hostnameVerifier = new HostnameVerifier() {
                @Override boolean verify(String hostname, SSLSession session) { return true }
            }

        }
        else if ( config.sslCert != null) {
            char[] password = null
            final factory = CertificateFactory.getInstance("X.509");
            final authority = new ByteArrayInputStream(config.sslCert)
            final certificates = factory.generateCertificates(authority)
            if (certificates.isEmpty()) {
                throw new IllegalArgumentException("Trusted certificates set cannot be empty");
            }

            final keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, password);
            certificates.eachWithIndex{ cert, index ->
                String alias = "ca$index"
                keyStore.setCertificateEntry(alias, cert);
            }

            final trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            trustManagers = trustManagerFactory.getTrustManagers();
        }
    }

    /**
     * Create a pod
     *
     * See
     *  https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#create-55
     *  https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#pod-v1-core
     *
     * @param spec
     * @return
     */
    K8sResponseJson podCreate(String req) {
        assert req
        final action = "/api/v1/namespaces/$config.namespace/pods"
        final resp = post(action, req)
        trace('POST', action, resp.text)
        return new K8sResponseJson(resp.text)
    }

    K8sResponseJson podCreate(Map req) {
        podCreate(JsonOutput.toJson(req))
    }

    /**
     * Delete a pod
     *
     * See
     *   https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#delete-58
     *
     * @param name
     * @return
     */
    K8sResponseJson podDelete(String name) {
        assert name
        final action = "/api/v1/namespaces/$config.namespace/pods/$name"
        final resp = delete(action)
        trace('DELETE', action, resp.text)
        new K8sResponseJson(resp.text)
    }

    /*
     * https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#list-62
     * https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#list-all-namespaces-63
     */
    K8sResponseJson podList(boolean allNamespaces=false) {
        final String action = allNamespaces ? "pods" : "namespaces/$config.namespace/pods"
        final resp = get("/api/v1/$action")
        trace('GET', action, resp.text)
        new K8sResponseJson(resp.text)
    }

    /*
     * https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#read-status-69
     */
    K8sResponseJson podStatus(String name) {
        assert name
        final action = "/api/v1/namespaces/$config.namespace/pods/$name/status"
        final resp = get(action)
        trace('GET', action, resp.text)
        new K8sResponseJson(resp.text)
    }


    /**
     * Get pod current state object
     *
     * @param podName The pod name
     * @return
     *  A {@link Map} representing the pod state at shown below
     *  <code>
     *       {
     *                "terminated": {
     *                    "exitCode": 127,
     *                    "reason": "ContainerCannotRun",
     *                    "message": "OCI runtime create failed: container_linux.go:296: starting container process caused \"exec: \\\"bash\\\": executable file not found in $PATH\": unknown",
     *                    "startedAt": "2018-01-12T22:04:25Z",
     *                    "finishedAt": "2018-01-12T22:04:25Z",
     *                    "containerID": "docker://730ef2e05be72ffc354f2682b4e8300610812137b9037b726c21e5c4e41b6dda"
     *                }
     *  </code>
     */
    @CompileDynamic
    Map podState( String podName ) {
        assert podName

        final resp = podStatus(podName)

        if( resp.status?.containerStatuses instanceof List && resp.status.containerStatuses.size()>0 ) {
            final container = resp.status.containerStatuses.get(0)
            if( container.name != podName )
                throw new K8sResponseException("Invalid pod status -- name does not match", resp)

            if( !container.state )
                throw new K8sResponseException("Invalid pod status -- missing state object", resp)

            return container.state
        }
        else
            throw new K8sResponseException("Invalid pod status -- missing container statuses", resp)
    }
    /*
     * https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#read-log
     */
    InputStream podLog(String name) {
        podLog( Collections.emptyMap(), name )
    }

    InputStream podLog(Map params, String name) {
        assert name
        // -- compose the request action uri
        def action = "/api/v1/namespaces/$config.namespace/pods/$name/log"
        int count=0
        for( String key : (params.keySet()) ) {
            action += "${count++==0 ? '?' : '&'}${key}=${params.get(key)}"
        }
        // -- submit request
        def resp = get(action)
        resp.stream
    }

    protected K8sResponseApi post(String path, String spec) {
        makeRequest('POST', path, spec)
    }

    protected K8sResponseApi delete(String path, String body=null) {
        makeRequest('DELETE', path, body)
    }

    protected HttpURLConnection createConnection0(String url) {
        new URL(url).openConnection() as HttpURLConnection
    }

    protected void setupHttpsConn( HttpsURLConnection conn ) {
        if (config.keyManagers != null || trustManagers != null) {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(config.keyManagers, trustManagers, new SecureRandom());
            conn.setSSLSocketFactory(sslContext.getSocketFactory())
        }
        else {
            conn.setSSLSocketFactory(null)
        }
        if( hostnameVerifier )
            conn.setHostnameVerifier(hostnameVerifier)
    }

    /**
     * Makes a HTTP(S) request the kubernetes master
     *
     * @param method The HTTP verb to use eg. {@code GET}, {@code POST}, etc
     * @param path The API action path
     * @param body The request payload
     * @return
     *      A two elements list in which the first entry is an integer representing the HTTP response code,
     *      the second element is the text (json) response
     */
    protected K8sResponseApi makeRequest(String method, String path, String body=null) {
        assert config.server, 'Missing Kubernetes server name'
        assert config.token, 'Missing Kubernetes auth token'
        assert path.startsWith('/'), 'Kubernetes API request path must starts with a `/` character'

        final prefix = config.server.contains("://") ? config.server : "https://$config.server"
        final conn = createConnection0(prefix + path)
        conn.setRequestProperty("Authorization", "Bearer $config.token")
        conn.setRequestProperty("Content-Type", "application/json")

        if( conn instanceof HttpsURLConnection ) {
            setupHttpsConn(conn)
        }

        if( !method ) method = body ? 'POST' : 'GET'
        conn.setRequestMethod(method)
        log.trace "[K8s] API request $method $path ${body ? '\n'+prettyPrint(body).indent() : ''}"

        if( body ) {
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.getOutputStream() << body
            conn.getOutputStream().flush()
        }

        final code = conn.getResponseCode()
        final isError = code >= 400
        final stream = isError ? conn.getErrorStream() : conn.getInputStream()
        if( isError )
            throw new K8sResponseException("Request $method $path returned an error code=$code", stream)
        return new K8sResponseApi(code, stream)
    }

    static private void trace(String method, String path, String text) {
        log.trace "[K8s] API response $method $path \n${prettyPrint(text).indent()}"
    }

    protected K8sResponseApi get(String path) {
        makeRequest('GET',path)
    }

    static protected String prettyPrint(String json) {
        try {
            JsonOutput.prettyPrint(json)
        }
        catch( Exception e ) {
            return json
        }
    }

    K8sResponseJson configCreate(String name, Map data) {

        final spec = [
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: [ name: name, namespace: config.namespace ],
                data: data
        ]

        configCreate0(spec)
    }

    protected K8sResponseJson configCreate0(Map spec) {
        final action = "/api/v1/namespaces/${config.namespace}/configmaps"
        final body = JsonOutput.toJson(spec)
        def resp = post(action, body)
        trace('POST', action, resp.text)
        return new K8sResponseJson(resp.text)
    }


    K8sResponseJson configDelete(String name) {
        final action = "/api/v1/namespaces/${config.namespace}/configmaps/$name"
        def resp = delete(action)
        trace('DELETE', action, resp.text)
        return new K8sResponseJson(resp.text)
    }

    K8sResponseJson configDeleteAll() {
        final action = "/api/v1/namespaces/${config.namespace}/configmaps"
        def resp = delete(action)
        trace('DELETE', action, resp.text)
        return new K8sResponseJson(resp.text)
    }


    @CompileDynamic
    static void main(String[] args) {


        def config = new ConfigDiscovery().discover()
        def client = new K8sClient(config)

        try {
            def result = client.configCreate('data1', [:])
        }
        catch ( K8sResponseException e ) {
            assert e.response.reason == 'AlreadyExists'
        }
        //println result//.status.containerStatuses[0].state.toConfigObject().prettyPrint()
    }


}

