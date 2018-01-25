/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
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
import javax.net.ssl.KeyManager
import java.nio.file.Files

import spock.lang.Specification
import test.TestHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ConfigDiscoveryTest extends Specification {


    def 'should read config from file' () {

        given:
        final CERT_DATA = "d29ybGQgaGVsbG8="
        final CLIENT_CERT = "aGVsbG8gd29ybGQ="
        final CLIENT_KEY = "Y2lhbyBtaWFv"

        def CONFIG = TestHelper.createInMemTempFile('config')
        CONFIG.text = """
            apiVersion: v1
            clusters:
            - cluster:
                insecure-skip-tls-verify: true
                server: https://localhost:6443
                certificate-authority-data: $CERT_DATA
              name: docker-for-desktop-cluster
            contexts:
            - context:
                cluster: docker-for-desktop-cluster
                user: docker-for-desktop
              name: docker-for-desktop
            current-context: docker-for-desktop
            kind: Config
            preferences: {}
            users:
            - name: docker-for-desktop
              user:
                client-certificate-data: $CLIENT_CERT
                client-key-data: $CLIENT_KEY
            """
            .stripIndent()

        def discovery = Spy(ConfigDiscovery)
        def KEY_MANAGERS = [] as KeyManager[]

        when:
        def config = discovery.fromConfig(CONFIG)
        then:
        1 * discovery.discoverAuthToken() >> 'secret-token'
        1 * discovery.createKeyManagers(CLIENT_CERT.decodeBase64(), CLIENT_KEY.decodeBase64()) >>  KEY_MANAGERS
        config.server == 'https://localhost:6443'
        config.token == 'secret-token'
        config.namespace == 'default'
        config.clientCert == CLIENT_CERT.decodeBase64()
        config.clientKey == CLIENT_KEY.decodeBase64()
        config.sslCert == CERT_DATA.decodeBase64()
        config.keyManagers.is( KEY_MANAGERS )
        !config.verifySsl
        !config.isFromCluster

    }

    def 'should read config from file with cert files' () {

        given:
        def folder = Files.createTempDirectory(null)
        def CA_FILE = folder.resolve('ca'); CA_FILE.text = 'ca-content'
        def CLIENT_CERT_FILE = folder.resolve('client-cert'); CLIENT_CERT_FILE.text = 'client-cert-content'
        def CLIENT_KEY_FILE = folder.resolve('client-key'); CLIENT_KEY_FILE.text = 'client-key-content'
        def CONFIG = folder.resolve('config')
        def KEY_MANAGERS = [] as KeyManager[]

        CONFIG.text = """
            apiVersion: v1
            clusters:
            - cluster:
                insecure-skip-tls-verify: true
                server: https://localhost:6443
                certificate-authority: $CA_FILE
              name: docker-for-desktop-cluster
            contexts:
            - context:
                cluster: docker-for-desktop-cluster
                user: docker-for-desktop
              name: docker-for-desktop
            current-context: docker-for-desktop
            kind: Config
            preferences: {}
            users:
            - name: docker-for-desktop
              user:
                client-certificate: $CLIENT_CERT_FILE
                client-key: $CLIENT_KEY_FILE
            """
                .stripIndent()

        def discovery = Spy(ConfigDiscovery)

        when:
        def config = discovery.fromConfig(CONFIG)
        then:
        1 * discovery.discoverAuthToken() >> 'secret-token'
        1 * discovery.createKeyManagers( CLIENT_CERT_FILE.bytes, CLIENT_KEY_FILE.bytes ) >> KEY_MANAGERS
        config.server == 'https://localhost:6443'
        config.token == 'secret-token'
        config.namespace == 'default'
        config.clientCert == CLIENT_CERT_FILE.bytes
        config.clientKey == CLIENT_KEY_FILE.bytes
        config.sslCert == CA_FILE.bytes
        config.keyManagers.is( KEY_MANAGERS )
        !config.verifySsl
        !config.isFromCluster

        cleanup:
        folder?.deleteDir()
    }


    def 'should load from cluster env' () {
        given:
        def CERT_FILE = TestHelper.createInMemTempFile('ca'); CERT_FILE.text = 'ca-content'
        def TOKEN_FILE = TestHelper.createInMemTempFile('token'); TOKEN_FILE.text = 'my-token'
        def NAMESPACE_FILE = TestHelper.createInMemTempFile('namespace'); NAMESPACE_FILE.text = 'foo-namespace'

        def discovery = Spy(ConfigDiscovery)

        when:
        def config = discovery.fromCluster([ KUBERNETES_SERVICE_HOST: 'foo.com', KUBERNETES_SERVICE_PORT: '4343' ])
        then:
        1 * discovery.path('/var/run/secrets/kubernetes.io/serviceaccount/ca.crt') >> CERT_FILE
        1 * discovery.path('/var/run/secrets/kubernetes.io/serviceaccount/token') >> TOKEN_FILE
        1 * discovery.path('/var/run/secrets/kubernetes.io/serviceaccount/namespace') >> NAMESPACE_FILE
        0 * discovery.createKeyManagers(_,_) >> null
        config.server == 'foo.com:4343'
        config.namespace == 'foo-namespace'
        config.token == 'my-token'
        config.sslCert == CERT_FILE.text.bytes
        config.isFromCluster

        when:
        config = discovery.fromCluster([ KUBERNETES_SERVICE_HOST: 'https://host.com' ])
        then:
        1 * discovery.path('/var/run/secrets/kubernetes.io/serviceaccount/ca.crt') >> CERT_FILE
        1 * discovery.path('/var/run/secrets/kubernetes.io/serviceaccount/token') >> TOKEN_FILE
        1 * discovery.path('/var/run/secrets/kubernetes.io/serviceaccount/namespace') >> NAMESPACE_FILE
        config.server == 'https://host.com'
    }

    def 'should create  key managers' () {
        given:
        final CERT = 'LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNTRENDQVRDZ0F3SUJBZ0lJRlFsM1l2Y2k1TWN3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB4T0RBeE1UTXlNREEyTlRaYUZ3MHhPVEF4TVRNeU1EQTJOVFphTURNeApGREFTQmdOVkJBb1RDMFJ2WTJ0bGNpQkpibU11TVJzd0dRWURWUVFERXhKa2IyTnJaWEl0Wm05eUxXUmxjMnQwCmIzQXdnWjh3RFFZSktvWklodmNOQVFFQkJRQURnWTBBTUlHSkFvR0JBUEtYT0ZsV2t2THIzb29ETGNFOElyME0KTzNBMHZqQlVvUzZ0bUdBbFRYYTd0QWQwM3BTMXNJNit0WVRwVlU2YXR6ZU9vU0VrOWhmaWxBdVNYdG1hSHZCUAp1czFEcG1LZEZRMWI3OFRkSnQ4OGV3c3BRajFxYUwvQldHeitMUzUrRHUrNUJuUGtmZlhDS1UxQTdUc2tZamJyClhxeDhlN2FWZURWTmFjZXc0Z0RqQWdNQkFBR2pBakFBTUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFBMXVtVlAKR29EZTVCRXJrb21qWXdITXhiTTd4UStibTYrUDE1T0pINUo0UGNQeU11d25ocC9ORVp1NnpsTTZSUUo3SUNKQgpHWTRBMnFKVmJsWUkwQkJzRkF1TXMreTAyazdVVVVoK0NRYVd0SXhBcFNmbkQ4dUVXQ0g5VE1ZNGdLbTZjTDhVCk1OVVl1RnpUQ2hmTS96RjdUMXVaZWxJYXNrYXFaWSt3a3hxa3YyRUQxQ2F5MDUxSXRWRXZVbDIvSVZyVHdrT20KZ25nL3Q4L2RkeDhpOUkzTFJrMTlTaERKdXlQZ1NrTTZRSWlSd09mRHk4V0ZFaURpd0hBS0ErSEZhTGhOOFJTMwpieDUvdEhEN01id0FpdnorNTU4YUFEQjNEd1ZpekthM2d5Wm4yUzRjUGFqZnNwODFqRkNIQS9QekdQdTU2MzJwCkxRN0gyRW1aYmJuUHFYTFgKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo='.decodeBase64()
        final KEY = 'LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlDWGdJQkFBS0JnUUR5bHpoWlZwTHk2OTZLQXkzQlBDSzlERHR3Tkw0d1ZLRXVyWmhnSlUxMnU3UUhkTjZVCnRiQ092cldFNlZWT21yYzNqcUVoSlBZWDRwUUxrbDdabWg3d1Q3ck5RNlppblJVTlcrL0UzU2JmUEhzTEtVSTkKYW1pL3dWaHMvaTB1Zmc3dnVRWno1SDMxd2lsTlFPMDdKR0kyNjE2c2ZIdTJsWGcxVFduSHNPSUE0d0lEQVFBQgpBb0dBYWRUOCtVU2lvU1d6bFVRanZ1eHNQMHRKMXY2N2hqdzFnVGFzaGkxZjZRK2tUNmgxdml5eGxPU3dMZ2JaCmQ0eFpwL3dxWVZwTm5rZnp6RVNUNnB5cEo5WTEwdHY1cFpSWG9HbG1NT2tIZSswUW45N0c5ZDRzL2JCV3lmYXYKRzhRTC9tZFN6Vy85YUdrSkpiNWU0VDlsSURvRDNFVDgwYUFWbzl2V0NPVUxsdWtDUVFEK0hINU5ucVBuSTdnTApWOUJKZzlRRVBwUTVYa2traW8rejZ2YkRHQU5rR1VPV1dmRURKUHE2Q2JBb1dqeWh1Qy9KS1dYRWs4Rkt0M1Y2CkhVNllYeVpGQWtFQTlHVE9XOFM4KzVNNHE5R3lNeURxN1ZkVHA3M2daeSsvNjVQam5hNlpDUnhTZklxL2xKUVoKY2F6MkhGYVRzRFdLbkdhWGNxTmdBVXNEODNyWTlzM3hCd0pCQUt5Vjc1YUtPMm0rRWI3cWVsV2p5bmpEZytwZQp4akNpUnkxOFZQSjJPYjlmaFU3MWNVS2dlQVdvbE5NalRuREw1dkNxUkNzNTZ4cnk5VC9sN2I2QlNUMENRUURnCjRoV2xDZTdnQzhOZEQzTkxhdUhpRGJZenB4dmp0Mk9Ca2E4ai9ISmptTVVxUnI0dEtPNFUxUlFPVlhoRzc2MmgKWnlHNjRpeklZOCs1N3ZQUWZ3Wm5Ba0VBdW9RWW1lUi90UWhIakhRNFlhZGRHbkNBQ2hZZ29ObEFzSGhGTElxVQo1ZTZaMXN2Q3VKU285TDVVRCtrclFUYWlGU01pRHZwZlJyVE1ZKzZ5Q0tTajd3PT0KLS0tLS1FTkQgUlNBIFBSSVZBVEUgS0VZLS0tLS0K'.decodeBase64()
        final discovery = new ConfigDiscovery()
        when:
        def managers = discovery.createKeyManagers(CERT, KEY)
        then:
        managers.size()==1
    }
}
