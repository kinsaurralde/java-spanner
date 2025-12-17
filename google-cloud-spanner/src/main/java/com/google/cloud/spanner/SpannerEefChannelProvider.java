/*
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spanner;

import com.google.api.gax.rpc.TransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.grpc.fallback.GcpFallbackChannel;
import com.google.cloud.grpc.fallback.GcpFallbackChannelOptions;
import com.google.cloud.grpc.fallback.GcpFallbackOpenTelemetry;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.google.api.gax.grpc.GrpcTransportChannel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import io.grpc.Grpc;
import io.grpc.alts.AltsChannelCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.CallCredentials;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import io.grpc.auth.MoreCallCredentials;
import java.time.Duration;

public final class SpannerEefChannelProvider implements TransportChannelProvider {

    private final boolean isDirectPathEnabled;
    private final String endpoint;
    private final Map<String, String> headers;
    private final Credentials credentials;
    private final GcpFallbackOpenTelemetry fallbackTelemetry;
    // A full implementation would also store and apply the executor, pool size, etc.

    private SpannerEefChannelProvider(
            boolean isDirectPathEnabled,
            String endpoint,
            Map<String, String> headers,
            Credentials credentials,
            GcpFallbackOpenTelemetry fallbackTelemetry) {
        this.isDirectPathEnabled = isDirectPathEnabled;
        this.endpoint = endpoint;
        this.headers = headers;
        this.credentials = credentials;
        this.fallbackTelemetry = fallbackTelemetry;
    }

    public static SpannerEefChannelProvider create() {
        return new SpannerEefChannelProvider(false, null, ImmutableMap.of(), null, null);
    }
    
    @Override
    public String getTransportName() {
        return "eef-grpc";
    }

    private static CallCredentials createHardBoundTokensCallCredentials(
	ComputeEngineCredentials credentials,
        ComputeEngineCredentials.GoogleAuthTransport googleAuthTransport,
        ComputeEngineCredentials.BindingEnforcement bindingEnforcement) {
      ComputeEngineCredentials.Builder credsBuilder =
          ((ComputeEngineCredentials) credentials).toBuilder();
      // We only set scopes and HTTP transport factory from the original credentials because
      // only those are used in gRPC CallCredentials to fetch request metadata. We create a new
      // credential
      // via {@code newBuilder} as opposed to {@code toBuilder} because we don't want a reference to
      // the
      // access token held by {@code credentials}; we want this new credential to fetch a new access
      // token
      // from MDS using the {@param googleAuthTransport} and {@param bindingEnforcement}.
      return MoreCallCredentials.from(
          ComputeEngineCredentials.newBuilder()
              .setScopes(credsBuilder.getScopes())
              .setHttpTransportFactory(credsBuilder.getHttpTransportFactory())
              .setGoogleAuthTransport(googleAuthTransport)
              .setBindingEnforcement(bindingEnforcement)
              .build());
    }

    @Override
    public TransportChannel getTransportChannel() throws IOException {
        if (needsEndpoint()) {
            throw new IllegalStateException("The endpoint must be set before getting a channel.");
        }
        if (!isDirectPathEnabled) {
            // If EEF is disabled, we must build a standard channel ourselves.
            ManagedChannel singleChannel = ManagedChannelBuilder.forTarget(this.endpoint).build();
            return GrpcTransportChannel.create(singleChannel);
        }

      // Create builders for both paths.
      String targetEndpoint = "spanner.googleapis.com:443";
      String dpTargetEndpoint = "google-c2p:///spanner.googleapis.com";

      ChannelCredentials credentials = AltsChannelCredentials.create();


      CallCredentials altsCallCredentials =
            createHardBoundTokensCallCredentials(
                ComputeEngineCredentials.create(),
		ComputeEngineCredentials.GoogleAuthTransport.ALTS, null);
      System.out.println("altsCallCredentials: " + altsCallCredentials);

      ChannelCredentials channelCreds =
          GoogleDefaultChannelCredentials.newBuilder()
              .altsCallCredentials(altsCallCredentials)
              .build();
        
        GcpFallbackChannelOptions.Builder fallbackOptionsBuilder =
                GcpFallbackChannelOptions.newBuilder()
                        .setPrimaryChannelName("directpath")
                        .setFallbackChannelName("cloudpath")
                        .setPeriod(Duration.ofMillis(1000))
                        .setMinFailedCalls(1)
                        .setErrorRateThreshold(0.1f);
        if (fallbackTelemetry != null) {
            fallbackOptionsBuilder.setGcpFallbackOpenTelemetry(fallbackTelemetry);
        }
        GcpFallbackChannelOptions fallbackOptions = fallbackOptionsBuilder.build();

        ManagedChannelBuilder<?> dpBuilder = Grpc.newChannelBuilder(dpTargetEndpoint, channelCreds);
        ManagedChannelBuilder<?> cpBuilder = ManagedChannelBuilder.forTarget(targetEndpoint);
        
        ManagedChannel eefManagedChannel = new GcpFallbackChannel(fallbackOptions, dpBuilder, cpBuilder);

        return GrpcTransportChannel.create(eefManagedChannel);
    }

    @Override
    public boolean needsHeaders() {
        return this.headers == null;
    }

    @Override
    public boolean needsEndpoint() {
        return Strings.isNullOrEmpty(this.endpoint);
    }

    @Override
    public boolean acceptsPoolSize() {
        return false;
    }
    
    @Override
    public boolean needsCredentials() {
        return this.credentials == null;
    }

    @Override
    public boolean needsExecutor() {
        return false; // Deprecated method
    }
    
    @Override
    public boolean shouldAutoClose() {
        return true;
    }

    @Override
    public TransportChannelProvider withHeaders(Map<String, String> headers) {
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials, fallbackTelemetry);
    }

    @Override
    public TransportChannelProvider withEndpoint(String endpoint) {
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials, fallbackTelemetry);
    }
    
    @Override
    public TransportChannelProvider withExecutor(Executor executor) {
        // A full implementation would store and use the executor.
        return this;
    }

    @Deprecated
    @Override
    public TransportChannelProvider withExecutor(ScheduledExecutorService executor) {
        return withExecutor((Executor) executor);
    }

    @Override
    public TransportChannelProvider withCredentials(Credentials credentials) {
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials, fallbackTelemetry);
    }
    
    @Override
    public TransportChannelProvider withPoolSize(int poolSize) {
        // A full implementation would store and use the pool size.
        return this;
    }

    public SpannerEefChannelProvider withDirectPathEnabled(boolean enabled) {
        return new SpannerEefChannelProvider(enabled, endpoint, headers, credentials, fallbackTelemetry);
    }

    public SpannerEefChannelProvider withGcpFallbackOpenTelemetry(GcpFallbackOpenTelemetry fallbackTelemetry) {
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials, fallbackTelemetry);
    }
}
