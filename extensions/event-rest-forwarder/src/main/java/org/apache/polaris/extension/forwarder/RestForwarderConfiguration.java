/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.extension.forwarder;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for the generic REST event forwarder. The forwarder POSTs raw Polaris catalog
 * events to a single user-defined REST endpoint; backend-specific translation (DataHub URN/aspect
 * building, Kafka topic routing, Atlas type mapping, …) is the receiver's responsibility.
 */
@ConfigMapping(prefix = "polaris.event-listener.rest-forwarder")
@ApplicationScoped
public interface RestForwarderConfiguration {

  /**
   * Absolute URL the listener POSTs each event envelope to (e.g. {@code
   * http://receiver:8080/polaris/events}). Optional so SmallRye Config does not fail at startup
   * when the listener type is {@code no-op} and this property is absent. When the listener type is
   * {@code rest-forwarder} and this is absent, each emit logs an error and skips — catalog
   * operations are never blocked.
   */
  @WithName("endpoint-url")
  Optional<String> endpointUrl();

  /**
   * Bearer credential sent in the {@code Authorization} header. Optional — omitted when the
   * receiver is unauthenticated (local quickstart) or uses a different auth scheme handled by an
   * upstream proxy.
   */
  @WithName("token")
  Optional<String> token();

  /**
   * If true, each emit blocks the Polaris request thread until the receiver responds. If false
   * (default), emits are fire-and-forget; receiver failures are logged and never block the catalog
   * operation.
   *
   * <p>Even in synchronous mode, {@link #requestTimeout()} caps the maximum wait, so a hung
   * receiver cannot block a Polaris request indefinitely. Production deployments should keep this
   * {@code false} — synchronous mode is intended for tests, debugging, and the rare workload that
   * requires strict per-event ordering at the receiver (async dispatch can interleave events for
   * the same catalog/table due to HTTP/2 multiplexing and connection pooling).
   */
  @WithName("synchronous-mode")
  @WithDefault("false")
  boolean synchronousMode();

  /**
   * TCP connect timeout for HTTP transport. Caps how long a single emit can wait for the receiver
   * to accept a connection (e.g., when it is down or unreachable). Without this cap, the JDK
   * {@code HttpClient} default is OS-level (often tens of seconds), which can stall requests in
   * synchronous mode and let pending futures pile up in async mode.
   */
  @WithName("connect-timeout")
  @WithDefault("5s")
  Duration connectTimeout();

  /**
   * End-to-end timeout for a single emit (connect + send + response). Caps how long a hung or slow
   * receiver can keep an in-flight future alive. Combined with {@link #maxInflightEmits()}, this
   * bounds the worst-case resource usage when the receiver is unavailable.
   */
  @WithName("request-timeout")
  @WithDefault("10s")
  Duration requestTimeout();

  /**
   * Maximum number of concurrently-pending emits. Acts as backpressure when the receiver is slow
   * or down: new emits beyond this limit are dropped (logged at WARN) rather than queued, so a
   * sustained outage cannot grow memory, sockets, or threads unboundedly. Polaris catalog
   * operations are never blocked — drops only affect downstream sync, not catalog correctness.
   */
  @WithName("max-inflight-emits")
  @WithDefault("1000")
  int maxInflightEmits();

  /**
   * Circuit breaker: number of consecutive emit failures (network error or non-2xx) that trigger
   * the circuit to open. While open, new emits are skipped immediately (no network attempt) until
   * {@link #circuitBreakerOpenDuration()} elapses, after which the next emit acts as a probe — if
   * it succeeds the circuit closes, otherwise it reopens. Designed to ride out 10-minute-class
   * receiver outages without thrashing.
   */
  @WithName("circuit-breaker-failure-threshold")
  @WithDefault("5")
  int circuitBreakerFailureThreshold();

  /**
   * Cooldown duration for the circuit breaker after it opens. See {@link
   * #circuitBreakerFailureThreshold()}.
   */
  @WithName("circuit-breaker-open-duration")
  @WithDefault("30s")
  Duration circuitBreakerOpenDuration();

  /**
   * In-memory replay buffer capacity for emits that failed (or were skipped due to circuit /
   * inflight cap). A background daemon thread drains the buffer when the receiver recovers. Set to
   * {@code 0} to disable. Drop-oldest semantics on overflow keep recent events at the cost of
   * dropping the oldest — bounded memory footprint regardless of outage length.
   */
  @WithName("replay-buffer-capacity")
  @WithDefault("1000")
  int replayBufferCapacity();
}
