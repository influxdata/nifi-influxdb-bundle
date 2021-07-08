/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.influxdata.nifi.services;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.security.GeneralSecurityException;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;
import okhttp3.OkHttpClient;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;

/**
 * @author Jakub Bednar (bednar@github) (11/06/2019 09:50)
 */
abstract class AbstractInfluxDatabaseService extends AbstractControllerService {

    /**
     * The {@link OkHttpClient.Builder} requires the {@link X509TrustManager} to use the SSL connection for that
     * we have to build own {@link SSLContext}.
     *
     * @see org.apache.nifi.security.util.SslContextFactory#createSslContext
     */
	void configureSSL(@NonNull final OkHttpClient.Builder okHttpClient,
					  @NonNull final ClientAuth clientAuth,
					  @NonNull final SSLContextService sslService) throws GeneralSecurityException
	{

		Objects.requireNonNull(okHttpClient, "OkHttpClient.Builder is required");
		Objects.requireNonNull(clientAuth, "ClientAuth is required");
		Objects.requireNonNull(sslService, "SSLContextService is required");

		final SSLContext sslContext = sslService.createContext();
		if (ClientAuth.REQUIRED == clientAuth) {
			sslContext.getDefaultSSLParameters().setNeedClientAuth(true);
		} else if (ClientAuth.WANT == clientAuth) {
			sslContext.getDefaultSSLParameters().setWantClientAuth(true);
		} else {
			sslContext.getDefaultSSLParameters().setWantClientAuth(false);
		}
		final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
		final TlsConfiguration tlsConfiguration = sslService.createTlsConfiguration();
		final X509TrustManager trustManager = SslContextFactory.getX509TrustManager(tlsConfiguration);
		okHttpClient.sslSocketFactory(socketFactory, trustManager);
	}
}