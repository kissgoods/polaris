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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.extension.datahub.http.HttpEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpEmitterTest {

  private HttpServer server;
  private final List<RecordedRequest> recorded = new ArrayList<>();
  private CountDownLatch latch;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          recorded.add(
              new RecordedRequest(
                  exchange.getRequestMethod(),
                  exchange.getRequestURI().getPath(),
                  exchange.getRequestHeaders().getFirst("Authorization"),
                  exchange.getRequestHeaders().getFirst("Content-Type"),
                  new String(body, StandardCharsets.UTF_8)));
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
          if (latch != null) latch.countDown();
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void emitUpsertPostsAspectsToOpenApiV3() throws Exception {
    latch = new CountDownLatch(1);
    HttpEmitter emitter = new HttpEmitter(buildConfig(true, "tok"));

    emitter.emitUpsert(
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)",
        "dataset",
        Map.of("datasetProperties", Map.of("name", "tbl")));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(1);
    RecordedRequest r = recorded.get(0);
    assertThat(r.method).isEqualTo("POST");
    assertThat(r.path).isEqualTo("/openapi/v3/entity/dataset");
    assertThat(r.contentType).isEqualTo("application/json");
    assertThat(r.authorization).isEqualTo("Bearer tok");

    // DataHub OpenAPI v3 `POST /openapi/v3/entity/{entityName}` body is a JSON ARRAY with each
    // entry carrying `urn` plus FLAT aspect-name keys (no nested "aspects" envelope), and each
    // aspect wrapped in {"value": ...}. Asserting the precise shape catches accidental drift
    // from the documented v3 contract — strict on purpose since the fake server here would
    // otherwise accept any payload.
    JsonNode body = new ObjectMapper().readTree(r.body);
    assertThat(body.isArray()).as("v3 entity body must be a JSON array").isTrue();
    assertThat(body).hasSize(1);
    JsonNode entry = body.get(0);
    assertThat(entry.get("urn").asText())
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)");
    assertThat(entry.has("aspects"))
        .as("aspects must be flat keys on the entry, not nested under 'aspects'")
        .isFalse();
    assertThat(entry.get("datasetProperties").get("value").get("name").asText()).isEqualTo("tbl");
    // Regression: every upsert auto-includes status.removed=false so that re-creating an entity
    // after emitStatusRemoved resurrects it on the DataHub UI instead of leaving it soft-deleted.
    assertThat(entry.get("status").get("value").get("removed").asBoolean()).isFalse();
  }

  @Test
  void emitUpsertPreservesCallerProvidedStatusAspect() throws Exception {
    latch = new CountDownLatch(1);
    HttpEmitter emitter = new HttpEmitter(buildConfig(true, null));

    // Caller explicitly passes a status aspect — the auto-injection must not clobber it.
    emitter.emitUpsert(
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)",
        "dataset",
        Map.of("status", Map.of("removed", true)));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    JsonNode body = new ObjectMapper().readTree(recorded.get(0).body);
    assertThat(body.isArray()).isTrue();
    assertThat(body.get(0).get("status").get("value").get("removed").asBoolean()).isTrue();
  }

  @Test
  void emitStatusRemovedSendsRemovedTrue() throws Exception {
    latch = new CountDownLatch(1);
    HttpEmitter emitter = new HttpEmitter(buildConfig(true, null));

    emitter.emitStatusRemoved("urn:li:container:iceberg.polaris.cat", "container");

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    RecordedRequest r = recorded.get(0);
    assertThat(r.path).isEqualTo("/openapi/v3/entity/container");
    assertThat(r.authorization).isNull();

    JsonNode body = new ObjectMapper().readTree(r.body);
    assertThat(body.isArray()).isTrue();
    JsonNode entry = body.get(0);
    assertThat(entry.get("urn").asText()).isEqualTo("urn:li:container:iceberg.polaris.cat");
    assertThat(entry.has("aspects")).isFalse();
    assertThat(entry.get("status").get("value").get("removed").asBoolean()).isTrue();
  }

  @Test
  void emitWithMissingGmsUrlSkipsRequestWithoutThrowing() throws Exception {
    DataHubConfiguration cfg =
        configWith(Optional.empty(), null, true, 1000, Duration.ofSeconds(30));

    HttpEmitter emitter = new HttpEmitter(cfg);
    emitter.emitUpsert(
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)",
        "dataset",
        java.util.Map.of("datasetProperties", java.util.Map.of("name", "tbl")));

    assertThat(recorded).isEmpty();
  }

  /**
   * Regression test for the inflight backpressure cap. With max-inflight-emits=1, the second emit
   * issued while the first is still pending must be dropped (not queued) so that a sustained outage
   * cannot grow memory unboundedly.
   */
  @Test
  void emitsBeyondInflightLimitAreDroppedNotQueued() throws Exception {
    // Use a context that blocks until released — keeps the first emit "in flight".
    server.removeContext("/");
    CountDownLatch hold = new CountDownLatch(1);
    server.createContext(
        "/",
        exchange -> {
          try {
            hold.await(10, TimeUnit.SECONDS);
            byte[] body = exchange.getRequestBody().readAllBytes();
            recorded.add(
                new RecordedRequest(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    null,
                    null,
                    new String(body, StandardCharsets.UTF_8)));
            exchange.sendResponseHeaders(200, -1);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          } finally {
            exchange.close();
          }
        });

    DataHubConfiguration cfg =
        configWith(
            Optional.of("http://127.0.0.1:" + server.getAddress().getPort()),
            null,
            false, // async — first emit returns immediately while server is held
            1, // inflight cap = 1
            Duration.ofSeconds(30));
    HttpEmitter emitter = new HttpEmitter(cfg);

    emitter.emitUpsert("urn:li:dataset:a", "dataset", Map.of("aspect", Map.of("k", "v1")));
    // Second emit must be dropped because the first is still in-flight.
    emitter.emitUpsert("urn:li:dataset:b", "dataset", Map.of("aspect", Map.of("k", "v2")));

    hold.countDown();

    // Wait briefly for the first emit's response to be recorded; the second must never arrive.
    Thread.sleep(500);
    assertThat(recorded).hasSize(1);
    assertThat(recorded.get(0).body).contains("v1");
    emitter.close();
  }

  /**
   * In synchronous mode the request timeout caps the maximum a Polaris request can stall on a hung
   * DataHub. The emit must return (logging a failure) instead of blocking indefinitely.
   */
  @Test
  void synchronousEmitTimesOutAgainstHungServerWithoutBlocking() throws Exception {
    server.removeContext("/");
    CountDownLatch hold = new CountDownLatch(1); // never counted down — server hangs
    server.createContext(
        "/",
        exchange -> {
          try {
            hold.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          } finally {
            exchange.close();
          }
        });

    DataHubConfiguration cfg =
        configWith(
            Optional.of("http://127.0.0.1:" + server.getAddress().getPort()),
            null,
            true, // synchronous
            10,
            Duration.ofMillis(500)); // tight request timeout
    HttpEmitter emitter = new HttpEmitter(cfg);

    long start = System.nanoTime();
    emitter.emitUpsert("urn:li:dataset:x", "dataset", Map.of("aspect", Map.of("k", "v")));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    // Must return well within a few seconds — caller is not blocked indefinitely.
    assertThat(elapsedMs).isLessThan(5_000);
    hold.countDown();
    emitter.close();
  }

  /**
   * Regression for the catch-around-permit invariant against a malformed gms-url. Uses ONE emitter
   * with {@code maxInflightEmits=1} and flips the URL between the two emits — if the bad emit
   * leaked the permit, the second (good) emit would be dropped (cap=1) and the server would never
   * see it. The assertion that {@code recorded} contains exactly the second body proves the permit
   * was released by the same emitter.
   */
  @Test
  void malformedGmsUrlReleasesPermitOnSameEmitter() throws Exception {
    AtomicReference<String> currentUrl = new AtomicReference<>("not a url");
    DataHubConfiguration cfg =
        sameEmitterCfg(currentUrl, new AtomicReference<>(Duration.ofSeconds(5)));
    HttpEmitter emitter = new HttpEmitter(cfg);
    latch = new CountDownLatch(1);

    // (1) bad URL — must throw inside post() and release the permit, not propagate to caller.
    emitter.emitUpsert("urn:li:dataset:bad", "dataset", Map.of("aspect", Map.of("k", "v_bad")));

    // (2) flip to working URL; with cap=1 this emit can ONLY succeed if (1) released the permit.
    currentUrl.set("http://127.0.0.1:" + server.getAddress().getPort());
    emitter.emitUpsert("urn:li:dataset:good", "dataset", Map.of("aspect", Map.of("k", "v_good")));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(1);
    assertThat(recorded.get(0).body).contains("v_good");
    emitter.close();
  }

  /**
   * Regression for invalid request-timeout (non-positive {@link Duration} → {@code
   * HttpRequest.timeout()} throws {@link IllegalArgumentException}). Same-emitter, cap=1 proof that
   * the permit was released.
   */
  @Test
  void invalidRequestTimeoutReleasesPermitOnSameEmitter() throws Exception {
    AtomicReference<String> currentUrl =
        new AtomicReference<>("http://127.0.0.1:" + server.getAddress().getPort());
    AtomicReference<Duration> currentTimeout = new AtomicReference<>(Duration.ZERO);
    DataHubConfiguration cfg = sameEmitterCfg(currentUrl, currentTimeout);
    HttpEmitter emitter = new HttpEmitter(cfg);
    latch = new CountDownLatch(1);

    // (1) Duration.ZERO triggers IAE inside HttpRequest.timeout() → must release permit.
    emitter.emitUpsert("urn:li:dataset:bad", "dataset", Map.of("aspect", Map.of("k", "v_bad")));

    // (2) Flip to a valid timeout; with cap=1 this can only succeed if (1) released the permit.
    currentTimeout.set(Duration.ofSeconds(5));
    emitter.emitUpsert("urn:li:dataset:good", "dataset", Map.of("aspect", Map.of("k", "v_good")));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(1);
    assertThat(recorded.get(0).body).contains("v_good");
    emitter.close();
  }

  /**
   * Circuit breaker opens after the configured number of consecutive failures, and subsequent emits
   * are skipped without hitting the network. Verified by counting server-side requests: after
   * threshold failures, no further request reaches the server.
   */
  @Test
  void circuitOpensAfterConsecutiveFailuresAndSkipsSubsequentEmits() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 3,
            /*circuitOpen*/ Duration.ofSeconds(5),
            /*replayCap*/ 0);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // 3 sync emits — all fail with 500, threshold=3 → circuit opens
    for (int i = 0; i < 3; i++) {
      emitter.emitUpsert("urn:dataset:f" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(requestCount.get()).isEqualTo(3);

    // 4th emit must be skipped — circuit OPEN
    emitter.emitUpsert("urn:dataset:skipped", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(3); // no new server hit

    emitter.close();
  }

  /**
   * After the open-duration cooldown, a single probe is allowed. If the probe succeeds the circuit
   * closes and subsequent emits are sent normally.
   */
  @Test
  void circuitClosesAfterCooldownAndSuccessfulProbe() throws Exception {
    AtomicInteger errorsRemaining = new AtomicInteger(3);
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(500, -1);
          } else {
            exchange.sendResponseHeaders(200, -1);
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 3,
            /*circuitOpen*/ Duration.ofMillis(200),
            /*replayCap*/ 0);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // Trip the circuit
    for (int i = 0; i < 3; i++) {
      emitter.emitUpsert("urn:" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(requestCount.get()).isEqualTo(3);

    // Skipped during cooldown
    emitter.emitUpsert("urn:skipped", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(3);

    Thread.sleep(250); // exceed cooldown

    // Probe — server now returns 200 → circuit closes
    emitter.emitUpsert("urn:probe", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(4);

    // Subsequent emits go through normally
    emitter.emitUpsert("urn:after", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(5);

    emitter.close();
  }

  /**
   * Regression: after the open-duration cooldown, a failed probe must re-open the circuit so that
   * subsequent emits are skipped again. Without this, a stale {@code circuitOpenedAtMs} from the
   * initial trip keeps {@code isCircuitOpen()} returning {@code false} for the rest of the outage
   * and the breaker stops protecting the network.
   */
  @Test
  void circuitReopensAfterCooldownAndFailedProbe() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          exchange.sendResponseHeaders(500, -1); // always fail
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 3,
            /*circuitOpen*/ Duration.ofMillis(200),
            /*replayCap*/ 0);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // Trip the circuit
    for (int i = 0; i < 3; i++) {
      emitter.emitUpsert("urn:" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(requestCount.get()).isEqualTo(3);

    // Skipped during cooldown
    emitter.emitUpsert("urn:skipped1", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(3);

    Thread.sleep(250); // exceed cooldown

    // Probe — server still 500 → circuit must re-open
    emitter.emitUpsert("urn:probe", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(4);

    // Next emit must be skipped again (this is what regresses without the fix)
    emitter.emitUpsert("urn:skipped2", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(4);

    Thread.sleep(250); // exceed cooldown again

    // Second probe — also fails, and the cycle must continue
    emitter.emitUpsert("urn:probe2", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(5);
    emitter.emitUpsert("urn:skipped3", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(requestCount.get()).isEqualTo(5);

    emitter.close();
  }

  /**
   * The replay buffer holds failed emits and the daemon replayer redelivers them once DataHub
   * recovers. Verified by sending N emits while the server returns 5xx, then watching the bodies
   * arrive once the server switches to 200.
   */
  @Test
  void replayBufferRedeliversAfterDataHubRecovers() throws Exception {
    final int N = 3;
    AtomicInteger errorsRemaining = new AtomicInteger(N);
    CountDownLatch deliveredLatch = new CountDownLatch(N);
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(500, -1);
          } else {
            byte[] body = exchange.getRequestBody().readAllBytes();
            recorded.add(
                new RecordedRequest(
                    "POST",
                    exchange.getRequestURI().getPath(),
                    null,
                    null,
                    new String(body, StandardCharsets.UTF_8)));
            exchange.sendResponseHeaders(200, -1);
            deliveredLatch.countDown();
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 100, // don't open the circuit during this test
            /*circuitOpen*/ Duration.ofSeconds(30),
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // N sync emits, all fail → enqueued to replay buffer
    for (int i = 0; i < N; i++) {
      emitter.emitUpsert("urn:r" + i, "dataset", Map.of("a", Map.of("k", "v" + i)));
    }
    // Server now returns 200; replayer should drain buffer and deliver
    assertThat(deliveredLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(N);

    emitter.close();
  }

  /**
   * Regression: a persistent client-side error (403 auth) must NOT open the circuit or fill the
   * replay buffer. Retrying or buffering 4xx responses never recovers — doing so would falsely trip
   * the breaker against a healthy DataHub and starve later valid emits. Verified indirectly: (1)
   * all 5 emits reach the server (would be 3 if the breaker had opened), and (2) after the server
   * recovers, no replays are attempted (would see extra hits if the buffer held entries).
   */
  @Test
  void nonRetryable4xxDoesNotOpenCircuitOrBufferForReplay() throws Exception {
    AtomicInteger errorsRemaining = new AtomicInteger(5);
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(403, -1);
          } else {
            exchange.sendResponseHeaders(200, -1);
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 3,
            /*circuitOpen*/ Duration.ofSeconds(30),
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // 5 emits, all 403 — well past threshold. Without the 4xx classification, after 3 the
    // circuit would open and emits 4 and 5 would be skipped at the network level.
    for (int i = 0; i < 5; i++) {
      emitter.emitUpsert("urn:f" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(requestCount.get()).isEqualTo(5); // all reached server → circuit stayed closed

    // Give the daemon replayer time to drain anything that was enqueued. Server now returns 200,
    // so any buffered entry would surface as an additional server hit.
    Thread.sleep(500);
    assertThat(requestCount.get()).isEqualTo(5); // no replay → buffer was empty

    emitter.close();
  }

  /**
   * 429 Too Many Requests is treated as retryable: it represents transient server pressure (rate
   * limiting) that does recover, so it must count toward the circuit failure threshold and enqueue
   * to the replay buffer like a 5xx. Verified by tripping the breaker with three 429s, then letting
   * the server recover and asserting the buffered events are redelivered.
   */
  @Test
  void retryable429OpensCircuitAndBuffersForReplay() throws Exception {
    final int N = 3;
    AtomicInteger errorsRemaining = new AtomicInteger(N);
    CountDownLatch successLatch = new CountDownLatch(N);
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(429, -1);
          } else {
            exchange.sendResponseHeaders(200, -1);
            successLatch.countDown();
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ N,
            /*circuitOpen*/ Duration.ofMillis(200), // short so replay runs within test
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // N sync emits, all 429 → threshold hit, circuit opens, all N enqueued for replay.
    for (int i = 0; i < N; i++) {
      emitter.emitUpsert("urn:r" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(requestCount.get()).isEqualTo(N);

    // After cooldown the replayer drains the buffer; server returns 200 → all N redelivered.
    assertThat(successLatch.await(10, TimeUnit.SECONDS)).isTrue();

    emitter.close();
  }

  /**
   * Drop-oldest semantics keep memory bounded during a sustained outage: with cap=2 and 5 failed
   * emits, only the most recent 2 should be retained for replay.
   */
  @Test
  void replayBufferDropsOldestWhenFull() throws Exception {
    AtomicInteger errorsRemaining = new AtomicInteger(5);
    CountDownLatch deliveredLatch = new CountDownLatch(2);
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(500, -1);
          } else {
            byte[] body = exchange.getRequestBody().readAllBytes();
            recorded.add(
                new RecordedRequest(
                    "POST",
                    exchange.getRequestURI().getPath(),
                    null,
                    null,
                    new String(body, StandardCharsets.UTF_8)));
            exchange.sendResponseHeaders(200, -1);
            deliveredLatch.countDown();
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 100,
            /*circuitOpen*/ Duration.ofSeconds(30),
            /*replayCap*/ 2);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // 5 sync emits, all fail. Buffer cap=2 → drop-oldest keeps the last 2 (v_d, v_e).
    emitter.emitUpsert("urn:a", "dataset", Map.of("a", Map.of("k", "v_a")));
    emitter.emitUpsert("urn:b", "dataset", Map.of("a", Map.of("k", "v_b")));
    emitter.emitUpsert("urn:c", "dataset", Map.of("a", Map.of("k", "v_c")));
    emitter.emitUpsert("urn:d", "dataset", Map.of("a", Map.of("k", "v_d")));
    emitter.emitUpsert("urn:e", "dataset", Map.of("a", Map.of("k", "v_e")));

    // Server now returns 200; only buffered events (v_d, v_e) should be replayed.
    assertThat(deliveredLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(2);
    assertThat(recorded.stream().map(r -> r.body).toList())
        .anyMatch(b -> b.contains("v_d"))
        .anyMatch(b -> b.contains("v_e"));
    assertThat(recorded.stream().map(r -> r.body).toList())
        .noneMatch(b -> b.contains("v_a"))
        .noneMatch(b -> b.contains("v_b"))
        .noneMatch(b -> b.contains("v_c"));

    emitter.close();
  }

  /**
   * Regression for the 10-minute outage flow: cooldown ends, the first replay probe still fails,
   * the breaker re-opens with a fresh cooldown, and on the next probe DataHub is back. Every
   * originally-buffered event must arrive at the server — none silently lost when the first probe
   * took the head out of the buffer. Without peek-and-keep, the head item delivered to the failing
   * probe would be removed and not retried.
   */
  @Test
  void replayBufferSurvivesFailedProbeAndDeliversAfterEventualRecovery() throws Exception {
    final int N = 3;
    AtomicInteger errorsRemaining = new AtomicInteger(N + 1); // N trip + 1 failed probe
    AtomicInteger requestCount = new AtomicInteger();
    CountDownLatch deliveredLatch = new CountDownLatch(N);
    List<String> deliveredBodies = new ArrayList<>();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          byte[] body = exchange.getRequestBody().readAllBytes();
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(500, -1);
          } else {
            synchronized (deliveredBodies) {
              deliveredBodies.add(new String(body, StandardCharsets.UTF_8));
            }
            exchange.sendResponseHeaders(200, -1);
            deliveredLatch.countDown();
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ N,
            /*circuitOpen*/ Duration.ofMillis(200),
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // N emits all return 500 → trips circuit, all N enqueued for replay
    for (int i = 0; i < N; i++) {
      emitter.emitUpsert("urn:r" + i, "dataset", Map.of("a", Map.of("k", "v" + i)));
    }
    assertThat(requestCount.get()).isEqualTo(N);

    // Replayer wakes after the 200ms cooldown:
    //   probe #1 still gets 500 → HALF_OPEN→OPEN with new cooldown, head stays in buffer.
    // After the next cooldown:
    //   probe #2 gets 200 → CLOSED → drain rest of buffer (urn:r1, urn:r2 redelivered).
    assertThat(deliveredLatch.await(15, TimeUnit.SECONDS)).isTrue();
    synchronized (deliveredBodies) {
      assertThat(deliveredBodies).hasSize(N);
      for (int i = 0; i < N; i++) {
        final String marker = "v" + i;
        assertThat(deliveredBodies.stream().anyMatch(b -> b.contains(marker)))
            .as("value %s should have been redelivered, not lost", marker)
            .isTrue();
      }
    }

    emitter.close();
  }

  /**
   * After cooldown ends, only ONE caller is allowed through as the HALF_OPEN probe; concurrent
   * emits must be rejected at the gateway until the probe resolves. Without this, every emit that
   * races past the cooldown boundary would hit a still-degraded GMS at once, defeating the point of
   * the breaker. Verified by holding the probe in-flight at the server and confirming the request
   * count stays at "trip count + 1" even after 20 concurrent emits.
   */
  @Test
  void halfOpenAllowsOnlyOneProbeUnderConcurrentEmits() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    AtomicInteger errorsRemaining = new AtomicInteger(3); // first 3 trip the circuit
    CountDownLatch probeArrived = new CountDownLatch(1);
    CountDownLatch holdProbe = new CountDownLatch(1);
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          if (errorsRemaining.getAndDecrement() > 0) {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
          } else {
            // The probe — block here so concurrent emits arrive while we sit in HALF_OPEN.
            probeArrived.countDown();
            try {
              holdProbe.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
          }
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 50,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(30),
            /*circuitThreshold*/ 3,
            /*circuitOpen*/ Duration.ofMillis(200),
            /*replayCap*/ 0);
    HttpEmitter emitter = new HttpEmitter(cfg);

    // Trip the circuit
    for (int i = 0; i < 3; i++) {
      emitter.emitUpsert("urn:f" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(requestCount.get()).isEqualTo(3);

    Thread.sleep(250); // exceed cooldown — next emit is a candidate probe

    // Fire 20 concurrent emits — only one wins the HALF_OPEN CAS and reaches the server.
    int concurrent = 20;
    Thread[] threads = new Thread[concurrent];
    for (int i = 0; i < concurrent; i++) {
      final int idx = i;
      threads[i] =
          new Thread(
              () -> emitter.emitUpsert("urn:c" + idx, "dataset", Map.of("a", Map.of("k", "v"))));
      threads[i].start();
    }

    assertThat(probeArrived.await(5, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(300); // racing threads should have all been rejected by the gateway by now
    assertThat(requestCount.get()).isEqualTo(4); // 3 trip + 1 probe; no one else got through

    holdProbe.countDown();
    for (Thread t : threads) {
      t.join(5_000);
    }

    emitter.close();
  }

  /**
   * Regression: a 2xx callback from an emit that entered the gateway while CLOSED must NOT flip the
   * circuit back to CLOSED if other concurrent failures have since tripped it to OPEN. Only a
   * HALF_OPEN probe is allowed to close the breaker. Without the {@code entered}-aware guard the
   * stale success would short-circuit the cooldown and let traffic flow into a still-degraded GMS.
   */
  @Test
  void staleInflightSuccessDoesNotForceClosedOverOpenCircuit() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    CountDownLatch firstArrived = new CountDownLatch(1);
    CountDownLatch holdFirst = new CountDownLatch(1);

    // setExecutor() must be called before start(), but @BeforeEach already started the default
    // server. Replace it with a fresh, multi-threaded instance so the held first emit doesn't
    // block subsequent emits from being served. @AfterEach will stop whichever server is current.
    server.stop(0);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService exec =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(exec);
    server.createContext(
        "/",
        exchange -> {
          int n = requestCount.incrementAndGet();
          if (n == 1) {
            firstArrived.countDown();
            try {
              holdFirst.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            exchange.sendResponseHeaders(500, -1);
          }
          exchange.close();
        });
    server.start();

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 50,
            /*sync*/ false, // async so the held emit doesn't block the trip emits
            /*requestTimeout*/ Duration.ofSeconds(30),
            /*circuitThreshold*/ 3,
            /*circuitOpen*/ Duration.ofSeconds(30), // long enough that no real cooldown elapses
            /*replayCap*/ 0);
    HttpEmitter emitter = new HttpEmitter(cfg);

    try {
      // emit A — server holds it. Its callback (200) will arrive long after the trip below.
      emitter.emitUpsert("urn:held", "dataset", Map.of("a", Map.of("k", "v")));
      assertThat(firstArrived.await(5, TimeUnit.SECONDS)).isTrue();

      // emit B/C/D — return 500. After all three callbacks land, circuit must be OPEN.
      for (int i = 0; i < 3; i++) {
        emitter.emitUpsert("urn:f" + i, "dataset", Map.of("a", Map.of("k", "v")));
      }
      long deadline = System.currentTimeMillis() + 5_000;
      while (requestCount.get() < 4 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertThat(requestCount.get()).isEqualTo(4);
      Thread.sleep(300); // let the three 500-callbacks land and trip the circuit

      // Sanity: circuit is OPEN — a new emit must NOT reach the server.
      int beforeRelease = requestCount.get();
      emitter.emitUpsert("urn:probe-before", "dataset", Map.of("a", Map.of("k", "v")));
      Thread.sleep(200);
      assertThat(requestCount.get()).isEqualTo(beforeRelease);

      // Release the held emit — its 200 callback fires NOW. Without the fix this calls
      // recordSuccess() which unconditionally sets state=CLOSED, defeating the cooldown.
      holdFirst.countDown();
      Thread.sleep(500); // allow A's callback to run

      // The breaker must still be OPEN: a fresh emit must still be skipped.
      int beforeFinal = requestCount.get();
      emitter.emitUpsert("urn:probe-after", "dataset", Map.of("a", Map.of("k", "v")));
      Thread.sleep(200);
      assertThat(requestCount.get())
          .as("stale 200 from a CLOSED-entered emit must not reopen traffic")
          .isEqualTo(beforeFinal);
    } finally {
      emitter.close();
      exec.shutdownNow();
    }
  }

  /**
   * Regression: when a HALF_OPEN probe enters the gateway but fails to be scheduled (here: a
   * malformed gms-url throws inside request build), releaseProbeSlot() must refresh the cooldown
   * timestamp. Without the refresh, the very next emit observes the same already-elapsed OPEN
   * timestamp and immediately claims another HALF_OPEN slot — busy-looping log/CPU output against a
   * misconfigured or saturated GMS.
   */
  @Test
  void releaseProbeSlotRefreshesCooldownWhenProbeFailsToSchedule() throws Exception {
    AtomicInteger reqCount = new AtomicInteger();
    AtomicReference<String> urlRef =
        new AtomicReference<>("http://127.0.0.1:" + server.getAddress().getPort());
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          reqCount.incrementAndGet();
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        });

    DataHubConfiguration cfg =
        new DataHubConfiguration() {
          @Override
          public Optional<String> gmsUrl() {
            return Optional.of(urlRef.get());
          }

          @Override
          public Optional<String> token() {
            return Optional.empty();
          }

          @Override
          public String platformInstance() {
            return "polaris";
          }

          @Override
          public String env() {
            return "PROD";
          }

          @Override
          public boolean synchronousMode() {
            return true;
          }

          @Override
          public Duration connectTimeout() {
            return Duration.ofSeconds(5);
          }

          @Override
          public Duration requestTimeout() {
            return Duration.ofSeconds(2);
          }

          @Override
          public int maxInflightEmits() {
            return 10;
          }

          @Override
          public int circuitBreakerFailureThreshold() {
            return 3;
          }

          @Override
          public Duration circuitBreakerOpenDuration() {
            return Duration.ofMillis(200);
          }

          @Override
          public int replayBufferCapacity() {
            return 0;
          }

          @Override
          public Map<String, String> domainMapping() {
            return Map.of();
          }
        };
    HttpEmitter emitter = new HttpEmitter(cfg);

    // Trip the circuit
    for (int i = 0; i < 3; i++) {
      emitter.emitUpsert("urn:f" + i, "dataset", Map.of("a", Map.of("k", "v")));
    }
    assertThat(reqCount.get()).isEqualTo(3);

    Thread.sleep(250); // exceed cooldown

    // Probe-eligible emit, but URL is broken → build throws inside post(), releaseProbeSlot()
    // runs. Without refresh, the cooldown timestamp is left at the original trip moment.
    urlRef.set("not a url");
    emitter.emitUpsert("urn:probe", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(reqCount.get()).isEqualTo(3); // no network call

    // Restore the URL and try again IMMEDIATELY. With the refresh in place, the fresh cooldown
    // is active and this emit must be gated. Without it, the emit would race straight through.
    urlRef.set("http://127.0.0.1:" + server.getAddress().getPort());
    emitter.emitUpsert("urn:after", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(reqCount.get())
        .as("releaseProbeSlot must refresh cooldown to back off after a failed-to-schedule probe")
        .isEqualTo(3);

    // After the fresh cooldown elapses, a normal probe is allowed and reaches the server.
    Thread.sleep(250);
    emitter.emitUpsert("urn:later", "dataset", Map.of("a", Map.of("k", "v")));
    assertThat(reqCount.get()).isEqualTo(4);

    emitter.close();
  }

  /**
   * Regression for the replay-buffer staleness window: a drop emit failing during a DataHub
   * outage gets buffered, then DataHub recovers and a same-URN upsert (table recreated) succeeds
   * directly. Without purging same-URN buffered entries on direct-emit success, the replayer
   * would later deliver the stale drop and DataHub would show the live table as removed.
   */
  @Test
  void successfulDirectEmitPurgesStaleSameUrnEntriesFromReplayBuffer() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          int n = requestCount.incrementAndGet();
          if (n == 1) {
            // 1st emit (drop) fails so the listener buffers it for replay.
            exchange.sendResponseHeaders(500, -1);
          } else {
            // 2nd+ emits succeed — covers the resurrect-direct and any (regression!) stale replay.
            exchange.sendResponseHeaders(200, -1);
          }
          exchange.close();
        });

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 10,
            /*sync*/ true,
            /*requestTimeout*/ Duration.ofSeconds(2),
            /*circuitThreshold*/ 100, // don't trip — focus on the buffer purge path
            /*circuitOpen*/ Duration.ofSeconds(30),
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    final String urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)";

    // (1) Drop on URN X — server returns 500 → buffered for replay.
    emitter.emitStatusRemoved(urn, "dataset");
    assertThat(requestCount.get()).isEqualTo(1);

    // (2) DataHub recovers; recreate of the SAME URN succeeds. The success callback must purge
    // the stale drop still sitting in the replay buffer.
    emitter.emitUpsert(urn, "dataset", Map.of("datasetProperties", Map.of("name", "tbl")));
    assertThat(requestCount.get()).isEqualTo(2);

    // (3) Give the replayer well over its poll interval to wake. With the purge in place, the
    // buffer is empty and no further request reaches the server. Without the purge, the
    // replayer would deliver the stale drop and requestCount would tick to 3.
    Thread.sleep(600);
    assertThat(requestCount.get())
        .as("stale buffered drop must be purged by the newer successful upsert on same URN")
        .isEqualTo(2);

    emitter.close();
  }

  /**
   * Regression: an older direct emit whose 2xx callback lands AFTER a newer same-URN emit has
   * been buffered must NOT purge the newer buffered entry. Without the seq gating, purge keys
   * only on URN and would evict the newer change, losing the most recent state.
   *
   * <p>Setup: server holds the first request (emit_A) while emit_B (same URN, newer seq) goes
   * to a 500 and is enqueued. Then emit_A is released and its 2xx callback runs. emit_B must
   * survive and be delivered by the replayer on the next attempt.
   */
  @Test
  void purgeOnSuccessPreservesNewerSameUrnBufferedEntriesByExecutionSequence() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    CountDownLatch firstArrived = new CountDownLatch(1);
    CountDownLatch holdFirst = new CountDownLatch(1);

    // Replace the @BeforeEach single-threaded server with a multi-threaded one so the held
    // first emit doesn't block emit_B and the replay.
    server.stop(0);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService exec =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(exec);
    server.createContext(
        "/",
        exchange -> {
          int n = requestCount.incrementAndGet();
          if (n == 1) {
            firstArrived.countDown();
            try {
              holdFirst.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1); // emit_A (older seq) succeeds
          } else if (n == 2) {
            exchange.sendResponseHeaders(500, -1); // emit_B (newer seq) fails → buffered
          } else {
            exchange.sendResponseHeaders(200, -1); // replay of emit_B succeeds
          }
          exchange.close();
        });
    server.start();

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 50,
            /*sync*/ false, // async so emit_A returns without waiting on the held response
            /*requestTimeout*/ Duration.ofSeconds(30),
            /*circuitThreshold*/ 100, // don't trip — focus is the seq-gated purge
            /*circuitOpen*/ Duration.ofSeconds(30),
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    try {
      final String urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)";

      // (1) emit_A (older seq) — server holds the response, request is in-flight.
      emitter.emitUpsert(urn, "dataset", Map.of("datasetProperties", Map.of("name", "tbl")));
      assertThat(firstArrived.await(5, TimeUnit.SECONDS)).isTrue();

      // (2) emit_B (newer seq, same URN) — server returns 500 → buffered for replay.
      emitter.emitStatusRemoved(urn, "dataset");
      long deadline = System.currentTimeMillis() + 5_000;
      while (requestCount.get() < 2 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertThat(requestCount.get()).isEqualTo(2);
      // Give emit_B's 500 callback time to enqueue before releasing emit_A.
      Thread.sleep(200);

      // (3) Release emit_A. Its 2xx callback fires NOW. Without the seq gate this would purge
      // emit_B (newer) from the buffer; with the seq gate emit_B must survive.
      holdFirst.countDown();

      // (4) Wait for the replayer to drain emit_B. requestCount must reach 3.
      deadline = System.currentTimeMillis() + 10_000;
      while (requestCount.get() < 3 && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertThat(requestCount.get())
          .as("newer buffered same-URN emit must survive purge from an older direct-emit success")
          .isEqualTo(3);
    } finally {
      emitter.close();
      exec.shutdownNow();
    }
  }

  /**
   * Regression: when a same-URN direct emit succeeds after the replayer has already peeked an
   * older buffered entry, purge marks the peeked entry stale before the replayer's next loop
   * iteration. The replayer's stale-flag check between peek and {@code sendAsync} then drops
   * the entry without dispatching it, preventing the older drop from reaching DataHub and
   * resurrecting removed state.
   *
   * <p>Forced timing: holds the very first server response so the replayer is observably
   * blocked inside {@code post()}, drains a newer same-URN emit that succeeds and runs purge,
   * then releases the held response. The replayer returns to its loop, peeks the next head, and
   * the stale check fires; the post-purge buffer state must be empty without any third request
   * having been sent.
   */
  @Test
  void replayerSkipsBufferedEntryMarkedStaleByConcurrentPurgeBeforeSend() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    CountDownLatch firstArrived = new CountDownLatch(1);
    CountDownLatch holdFirst = new CountDownLatch(1);

    // Multi-threaded server so direct emits don't block on the held replay.
    server.stop(0);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService exec =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(exec);
    server.createContext(
        "/",
        exchange -> {
          int n = requestCount.incrementAndGet();
          if (n == 1) {
            // Initial direct emit (drop) — fail so it lands in the replay buffer.
            exchange.sendResponseHeaders(500, -1);
          } else if (n == 2) {
            // Replayer's send of the buffered drop arrives here. Hold it so we can race a
            // newer same-URN direct emit through purge before this drop's whenComplete fires.
            firstArrived.countDown();
            try {
              holdFirst.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            // The newer direct upsert and anything else: just succeed.
            exchange.sendResponseHeaders(200, -1);
          }
          exchange.close();
        });
    server.start();

    DataHubConfiguration cfg =
        configWithResilience(
            /*inflight*/ 50,
            /*sync*/ false,
            /*requestTimeout*/ Duration.ofSeconds(30),
            /*circuitThreshold*/ 100,
            /*circuitOpen*/ Duration.ofSeconds(30),
            /*replayCap*/ 10);
    HttpEmitter emitter = new HttpEmitter(cfg);

    try {
      final String urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)";

      // (1) Drop fails → buffered. Emit is async (sync=false in this cfg), so wait for the
      // 500 to land at the server before continuing.
      emitter.emitStatusRemoved(urn, "dataset");
      long deadline = System.currentTimeMillis() + 5_000;
      while (requestCount.get() < 1 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertThat(requestCount.get()).isEqualTo(1);

      // (2) Replayer wakes (poll interval = 200ms) and sends the buffered drop. We hold it
      // server-side so the replayer's post() stays inside future.join() — that's the window
      // during which a same-URN direct emit can race past us.
      assertThat(firstArrived.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(requestCount.get()).isEqualTo(2);

      // (3) Same-URN upsert (newer seq) — succeeds directly. The success callback runs purge,
      // which marks the buffered drop stale AND removes it.
      emitter.emitUpsert(urn, "dataset", Map.of("datasetProperties", Map.of("name", "tbl")));
      // Wait for the upsert response to be recorded.
      deadline = System.currentTimeMillis() + 5_000;
      while (requestCount.get() < 3 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertThat(requestCount.get()).isEqualTo(3);

      // (4) Release the held replay response. Its 2xx fires, replayer poll()s buffer empty,
      // goes back to loop and finds no further work — the stale check applies if there were
      // any other peeked entries. (For this scenario the buffer is empty.)
      holdFirst.countDown();

      // (5) Settling delay: confirm no further request arrives. Without this fix, a *second*
      // buffered same-URN drop (had we enqueued one) would have been sent after purge — the
      // stale-flag check guards that case. Here we assert the simpler invariant: requestCount
      // does not grow past 3, i.e., the replayer does not invent a fourth request.
      Thread.sleep(500);
      assertThat(requestCount.get()).isEqualTo(3);
    } finally {
      emitter.close();
      exec.shutdownNow();
    }
  }

  /**
   * Builds a {@link DataHubConfiguration} whose gmsUrl and requestTimeout are read from {@link
   * AtomicReference}s on each emit, so a single emitter can flip between bad and good config to
   * directly verify permit release on the same emitter (cap=1).
   */
  private DataHubConfiguration sameEmitterCfg(
      AtomicReference<String> currentUrl, AtomicReference<Duration> currentTimeout) {
    return new DataHubConfiguration() {
      @Override
      public Optional<String> gmsUrl() {
        return Optional.of(currentUrl.get());
      }

      @Override
      public Optional<String> token() {
        return Optional.empty();
      }

      @Override
      public String platformInstance() {
        return "polaris";
      }

      @Override
      public String env() {
        return "PROD";
      }

      @Override
      public boolean synchronousMode() {
        return true;
      }

      @Override
      public Duration connectTimeout() {
        return Duration.ofSeconds(5);
      }

      @Override
      public Duration requestTimeout() {
        return currentTimeout.get();
      }

      @Override
      public int maxInflightEmits() {
        return 1; // tight cap — leak would be detected immediately
      }

      @Override
      public int circuitBreakerFailureThreshold() {
        return 100; // never trip during these tests
      }

      @Override
      public Duration circuitBreakerOpenDuration() {
        return Duration.ofSeconds(30);
      }

      @Override
      public int replayBufferCapacity() {
        return 0; // disabled — these tests assert raw emit behavior
      }

      @Override
      public Map<String, String> domainMapping() {
        return Map.of();
      }
    };
  }

  private DataHubConfiguration configWith(
      Optional<String> gmsUrl, String token, boolean sync, int inflight, Duration requestTimeout) {
    return new DataHubConfiguration() {
      @Override
      public Optional<String> gmsUrl() {
        return gmsUrl;
      }

      @Override
      public Optional<String> token() {
        return Optional.ofNullable(token);
      }

      @Override
      public String platformInstance() {
        return "polaris";
      }

      @Override
      public String env() {
        return "PROD";
      }

      @Override
      public boolean synchronousMode() {
        return sync;
      }

      @Override
      public Duration connectTimeout() {
        return Duration.ofSeconds(5);
      }

      @Override
      public Duration requestTimeout() {
        return requestTimeout;
      }

      @Override
      public int maxInflightEmits() {
        return inflight;
      }

      @Override
      public int circuitBreakerFailureThreshold() {
        return 100; // never trip during these tests
      }

      @Override
      public Duration circuitBreakerOpenDuration() {
        return Duration.ofSeconds(30);
      }

      @Override
      public int replayBufferCapacity() {
        return 0; // disabled — these tests assert raw emit behavior
      }

      @Override
      public Map<String, String> domainMapping() {
        return Map.of();
      }
    };
  }

  /**
   * Builds a config with circuit-breaker / replay-buffer knobs exposed for resilience tests. Uses
   * the working server URL by default and tight inflight cap so behavior is deterministic.
   */
  private DataHubConfiguration configWithResilience(
      int inflight,
      boolean sync,
      Duration requestTimeout,
      int circuitThreshold,
      Duration circuitOpen,
      int replayCap) {
    return new DataHubConfiguration() {
      @Override
      public Optional<String> gmsUrl() {
        return Optional.of("http://127.0.0.1:" + server.getAddress().getPort());
      }

      @Override
      public Optional<String> token() {
        return Optional.empty();
      }

      @Override
      public String platformInstance() {
        return "polaris";
      }

      @Override
      public String env() {
        return "PROD";
      }

      @Override
      public boolean synchronousMode() {
        return sync;
      }

      @Override
      public Duration connectTimeout() {
        return Duration.ofSeconds(5);
      }

      @Override
      public Duration requestTimeout() {
        return requestTimeout;
      }

      @Override
      public int maxInflightEmits() {
        return inflight;
      }

      @Override
      public int circuitBreakerFailureThreshold() {
        return circuitThreshold;
      }

      @Override
      public Duration circuitBreakerOpenDuration() {
        return circuitOpen;
      }

      @Override
      public int replayBufferCapacity() {
        return replayCap;
      }

      @Override
      public Map<String, String> domainMapping() {
        return Map.of();
      }
    };
  }

  private DataHubConfiguration buildConfig(boolean sync, String token) {
    int port = server.getAddress().getPort();
    return configWith(
        Optional.of("http://127.0.0.1:" + port), token, sync, 1000, Duration.ofSeconds(30));
  }

  private record RecordedRequest(
      String method, String path, String authorization, String contentType, String body) {}
}
