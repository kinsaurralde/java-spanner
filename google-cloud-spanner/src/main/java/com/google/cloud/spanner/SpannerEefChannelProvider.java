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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.google.api.gax.grpc.GrpcTransportChannel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public final class SpannerEefChannelProvider implements TransportChannelProvider {

    private final boolean isDirectPathEnabled;
    private final String endpoint;
    private final Map<String, String> headers;
    private final Credentials credentials;
    // A full implementation would also store and apply the executor, pool size, etc.

    private SpannerEefChannelProvider(
            boolean isDirectPathEnabled,
            String endpoint,
            Map<String, String> headers,
            Credentials credentials) {
        this.isDirectPathEnabled = isDirectPathEnabled;
        this.endpoint = endpoint;
        this.headers = headers;
        this.credentials = credentials;
    }

    public static SpannerEefChannelProvider create() {
        return new SpannerEefChannelProvider(false, null, ImmutableMap.of(), null);
    }
    
    @Override
    public String getTransportName() {
        return "eef-grpc";
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

        // Create a builder for the fallback (Cloudpath) channel.
        ManagedChannelBuilder<?> fallbackChannelBuilder = ManagedChannelBuilder.forTarget(this.endpoint);
        // In a complete implementation, you would apply headers, executor, credentials etc. here

        // Create a new builder for the primary (DirectPath) channel.
        ManagedChannelBuilder<?> primaryChannelBuilder = ManagedChannelBuilder.forTarget(this.endpoint); // Replace with actual DP endpoint logic
        
        GcpFallbackChannelOptions fallbackOptions =
                GcpFallbackChannelOptions.newBuilder()
                        .setPrimaryChannelName("directpath")
                        .setFallbackChannelName("cloudpath")
                        .build();
        
        ManagedChannel eefManagedChannel = new GcpFallbackChannel(fallbackOptions, primaryChannelBuilder, fallbackChannelBuilder);

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
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials);
    }

    @Override
    public TransportChannelProvider withEndpoint(String endpoint) {
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials);
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
        return new SpannerEefChannelProvider(isDirectPathEnabled, endpoint, headers, credentials);
    }
    
    @Override
    public TransportChannelProvider withPoolSize(int poolSize) {
        // A full implementation would store and use the pool size.
        return this;
    }

    public SpannerEefChannelProvider withDirectPathEnabled(boolean enabled) {
        return new SpannerEefChannelProvider(enabled, endpoint, headers, credentials);
    }
}
