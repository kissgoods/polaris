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
package org.apache.polaris.extension.datahub;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Configuration for the DataHub event listener. */
@ConfigMapping(prefix = "polaris.event-listener.datahub")
@ApplicationScoped
public interface DataHubConfiguration {

  /**
   * DataHub GMS base URL, e.g. {@code http://localhost:8080}. Optional so that SmallRye Config does
   * not fail at startup when the listener type is {@code no-op} and this property is absent.
   *
   * <p>When absent and the listener is {@code datahub-http}, each emit logs an error and returns
   * without throwing, so catalog operations are never blocked.
   */
  @WithName("gms-url")
  Optional<String> gmsUrl();

  /** Personal Access Token used as Bearer credential. Optional for unsecured local quickstart. */
  @WithName("token")
  Optional<String> token();

  /** Logical platform instance name; used in DataHub container hierarchy. */
  @WithName("platform-instance")
  @WithDefault("polaris")
  String platformInstance();

  /** Fabric / environment label attached to dataset URNs (PROD, DEV, STAGE, ...). */
  @WithName("env")
  @WithDefault("PROD")
  String env();

  /**
   * If true, every emit blocks the Polaris request until DataHub responds. If false (default),
   * emits are fire-and-forget; failures are logged and never block the catalog operation.
   *
   * <p>Even in synchronous mode, the {@link #requestTimeout()} caps the maximum wait, so a hung
   * DataHub cannot block a Polaris request indefinitely. Production deployments should keep this
   * {@code false} — synchronous mode is intended for tests and debugging.
   *
   * <p><b>Strict per-URN ordering</b>: in async mode, two emits for the same URN can interleave
   * at DataHub (HTTP/2 multiplexing or connection pooling), and the replay buffer can land a
   * stale entry after a newer direct emit has already been accepted — once {@code sendAsync}
   * has dispatched the request, the JDK {@link java.net.http.HttpClient} does not honor
   * cancellation, so we cannot stop an in-flight stale replay from reaching DataHub. Setting
   * this knob to {@code true} serializes each emit through the Polaris catalog-request thread
   * that triggered it (no concurrent emits issue from the same caller, no buffered replay
   * races with a newer direct emit on the same URN). If your workload requires DataHub to
   * mirror Polaris order exactly, enable this and accept the latency hit on each catalog op.
   */
  @WithName("synchronous-mode")
  @WithDefault("false")
  boolean synchronousMode();

  /**
   * TCP connect timeout for HTTP transport. Caps how long a single emit can wait for DataHub to
   * accept a connection (e.g., when DataHub is down or unreachable). Without this cap, the JDK
   * {@code HttpClient} default is OS-level (often tens of seconds), which can stall requests in
   * synchronous mode and let pending futures pile up in async mode.
   */
  @WithName("connect-timeout")
  @WithDefault("5s")
  Duration connectTimeout();

  /**
   * End-to-end timeout for a single emit (connect + send + response). Caps how long a hung or slow
   * DataHub can keep an in-flight future alive. Combined with {@link #maxInflightEmits()}, this
   * bounds the worst-case resource usage when DataHub is unavailable.
   */
  @WithName("request-timeout")
  @WithDefault("10s")
  Duration requestTimeout();

  /**
   * Maximum number of concurrently-pending DataHub emits. Acts as backpressure when DataHub is slow
   * or down: new emits beyond this limit are dropped (logged at WARN) rather than queued, so a
   * sustained outage cannot grow memory, sockets, or threads unboundedly. Polaris catalog
   * operations are never blocked — drops only affect DataHub sync, not catalog correctness.
   */
  @WithName("max-inflight-emits")
  @WithDefault("1000")
  int maxInflightEmits();

  /**
   * Circuit breaker: number of consecutive emit failures (network error or non-2xx) that trigger
   * the circuit to open. While open, new emits are skipped immediately (no network attempt) until
   * {@link #circuitBreakerOpenDuration()} elapses, after which the next emit acts as a probe — if
   * it succeeds the circuit closes, otherwise it reopens. Designed to ride out 10-minute-class
   * DataHub outages without thrashing.
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
   * In-memory replay buffer capacity for emits that failed (or were skipped due to circuit/inflight
   * cap). A background daemon thread drains the buffer when DataHub recovers. Set to {@code 0} to
   * disable. Drop-oldest semantics on overflow keeps recent events at the cost of dropping the
   * oldest — bounded memory footprint regardless of outage length.
   */
  @WithName("replay-buffer-capacity")
  @WithDefault("1000")
  int replayBufferCapacity();

  /**
   * Catalog-name → DataHub Domain URN mapping. When a dataset emit is for a catalog whose name is
   * present in this map, the listener attaches a {@code domains} aspect with the configured URN.
   * Catalogs absent from this map emit no {@code domains} aspect (no fallback). Catalog containers
   * and namespace containers are never assigned a domain — only datasets.
   *
   * <p>Property format: {@code polaris.event-listener.datahub.domain-mapping.<catalog>=urn:li:domain:<id>}.
   * Values must be full URNs ({@code urn:li:domain:...}); the listener passes them through verbatim.
   *
   * <p><b>Deployment</b>: because the DataHub listener is added as a {@code runtimeOnly} extension,
   * its {@code @ConfigMapping} keys are rejected by SmallRye's {@code validate-unknown} guard when
   * sourced from {@code application.properties} (SRCFG00050). Inject these keys via environment
   * variables instead, e.g. {@code POLARIS_EVENT_LISTENER_DATAHUB_DOMAIN_MAPPING_<CATALOG>=urn:li:domain:<id>}.
   * Catalog names should be alphanumeric (plus {@code -}); names containing {@code .} conflict with
   * SmallRye's key-segment separator and are not supported.
   */
  @WithName("domain-mapping")
  Map<String, String> domainMapping();
}
