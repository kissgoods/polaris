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
package org.apache.polaris.extension.forwarder.http;

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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.extension.forwarder.EventEnvelope;
import org.apache.polaris.extension.forwarder.RestForwarderConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Wire tests for {@link HttpEventPoster}. A local {@link HttpServer} stands in for the receiver;
 * the tests assert the request shape (path, headers, body) and the five isolation guarantees
 * (timeouts, inflight cap, circuit breaker, replay buffer, non-retryable 4xx classification).
 */
class HttpEventPosterTest {

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

  // ============= Wire shape =============

  @Test
  void postSerializesEnvelopeAsSingleJsonObjectWithBearerToken() throws Exception {
    latch = new CountDownLatch(1);
    HttpEventPoster poster = new HttpEventPoster(buildConfig(true, "tok"));

    poster.post(envelope("AfterCreateTable", "cat", "tbl"));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(1);
    RecordedRequest r = recorded.get(0);
    assertThat(r.method).isEqualTo("POST");
    assertThat(r.path).as("path is the receiver's endpoint root — no implicit suffix").isEqualTo("/");
    assertThat(r.contentType).isEqualTo("application/json");
    assertThat(r.authorization).isEqualTo("Bearer tok");

    // Body is a single envelope JSON object (NOT an array — receiver decides batching itself).
    JsonNode body = new ObjectMapper().readTree(r.body);
    assertThat(body.isObject()).isTrue();
    assertThat(body.get("eventType").asText()).isEqualTo("AfterCreateTable");
    assertThat(body.get("catalog").asText()).isEqualTo("cat");
    assertThat(body.get("table").asText()).isEqualTo("tbl");
  }

  @Test
  void postWithoutTokenOmitsAuthorizationHeader() throws Exception {
    latch = new CountDownLatch(1);
    HttpEventPoster poster = new HttpEventPoster(buildConfig(true, null));

    poster.post(envelope("AfterDropTable", "cat", "tbl"));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded.get(0).authorization).isNull();
  }

  @Test
  void postWithMissingEndpointUrlSkipsRequestWithoutThrowing() {
    RestForwarderConfiguration cfg =
        configWith(Optional.empty(), null, true, 1000, Duration.ofSeconds(30));
    HttpEventPoster poster = new HttpEventPoster(cfg);

    poster.post(envelope("AfterCreateTable", "cat", "tbl"));

    assertThat(recorded).isEmpty();
  }

  // ============= Inflight backpressure =============

  @Test
  void emitsBeyondInflightLimitAreDroppedNotQueued() throws Exception {
    // Hold the server so the first emit stays in-flight while the second arrives.
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

    RestForwarderConfiguration cfg =
        configWith(
            Optional.of("http://127.0.0.1:" + server.getAddress().getPort()),
            null,
            false, // async
            1, // inflight cap = 1
            Duration.ofSeconds(30));
    HttpEventPoster poster = new HttpEventPoster(cfg);

    // Mark the in-flight envelope so only it can be recorded.
    poster.post(envelopeWithActor("AfterCreateTable", "alpha"));
    // Dropped — first is still inflight.
    poster.post(envelopeWithActor("AfterCreateTable", "beta"));
    hold.countDown();

    Thread.sleep(500);
    assertThat(recorded).hasSize(1);
    assertThat(recorded.get(0).body).contains("alpha").doesNotContain("beta");
    poster.close();
  }

  // ============= Sync timeout =============

  @Test
  void synchronousPostTimesOutAgainstHungReceiverWithoutBlocking() throws Exception {
    server.removeContext("/");
    CountDownLatch hold = new CountDownLatch(1); // never released — server hangs
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

    RestForwarderConfiguration cfg =
        configWith(
            Optional.of("http://127.0.0.1:" + server.getAddress().getPort()),
            null,
            true, // sync
            10,
            Duration.ofMillis(500)); // tight request timeout
    HttpEventPoster poster = new HttpEventPoster(cfg);

    long start = System.nanoTime();
    poster.post(envelope("AfterCreateTable", "cat", "tbl"));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertThat(elapsedMs).isLessThan(5_000);
    hold.countDown();
    poster.close();
  }

  // ============= Permit release on config errors =============

  @Test
  void malformedEndpointUrlReleasesPermitOnSamePoster() throws Exception {
    AtomicReference<String> currentUrl = new AtomicReference<>("not a url");
    AtomicReference<Duration> currentTimeout = new AtomicReference<>(Duration.ofSeconds(5));
    RestForwarderConfiguration cfg = sameEmitterCfg(currentUrl, currentTimeout);
    HttpEventPoster poster = new HttpEventPoster(cfg);
    latch = new CountDownLatch(1);

    // (1) bad URL — must throw inside doPost and release the permit, not propagate.
    poster.post(envelopeWithActor("AfterCreateTable", "v_bad"));

    // (2) valid URL; cap=1 so this can only land if (1) released the permit.
    currentUrl.set("http://127.0.0.1:" + server.getAddress().getPort());
    poster.post(envelopeWithActor("AfterCreateTable", "v_good"));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(1);
    assertThat(recorded.get(0).body).contains("v_good");
    poster.close();
  }

  @Test
  void invalidRequestTimeoutReleasesPermitOnSamePoster() throws Exception {
    AtomicReference<String> currentUrl =
        new AtomicReference<>("http://127.0.0.1:" + server.getAddress().getPort());
    AtomicReference<Duration> currentTimeout = new AtomicReference<>(Duration.ZERO);
    RestForwarderConfiguration cfg = sameEmitterCfg(currentUrl, currentTimeout);
    HttpEventPoster poster = new HttpEventPoster(cfg);
    latch = new CountDownLatch(1);

    // (1) Duration.ZERO triggers IAE inside HttpRequest.timeout() → must release permit.
    poster.post(envelopeWithActor("AfterCreateTable", "v_bad"));

    // (2) valid timeout; cap=1 so this can only land if (1) released the permit.
    currentTimeout.set(Duration.ofSeconds(5));
    poster.post(envelopeWithActor("AfterCreateTable", "v_good"));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(1);
    assertThat(recorded.get(0).body).contains("v_good");
    poster.close();
  }

  // ============= Circuit breaker =============

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

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(
                /*inflight*/ 10,
                /*sync*/ true,
                /*requestTimeout*/ Duration.ofSeconds(2),
                /*circuitThreshold*/ 3,
                /*circuitOpen*/ Duration.ofSeconds(5),
                /*replayCap*/ 0));

    for (int i = 0; i < 3; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    assertThat(requestCount.get()).isEqualTo(3);

    // Next emit skipped — circuit OPEN
    poster.post(envelopeWithActor("AfterCreateTable", "skipped"));
    assertThat(requestCount.get()).isEqualTo(3);

    poster.close();
  }

  @Test
  void circuitClosesAfterCooldownAndSuccessfulProbe() throws Exception {
    AtomicInteger errorsRemaining = new AtomicInteger(3);
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          if (errorsRemaining.getAndDecrement() > 0) exchange.sendResponseHeaders(500, -1);
          else exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(
                10, true, Duration.ofSeconds(2), 3, Duration.ofMillis(200), 0));

    for (int i = 0; i < 3; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    poster.post(envelopeWithActor("AfterCreateTable", "skipped"));
    assertThat(requestCount.get()).isEqualTo(3);

    Thread.sleep(250); // cooldown

    poster.post(envelopeWithActor("AfterCreateTable", "probe")); // 200 → closes
    poster.post(envelopeWithActor("AfterCreateTable", "after"));
    assertThat(requestCount.get()).isEqualTo(5);

    poster.close();
  }

  @Test
  void circuitReopensAfterCooldownAndFailedProbe() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(
                10, true, Duration.ofSeconds(2), 3, Duration.ofMillis(200), 0));

    for (int i = 0; i < 3; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    poster.post(envelopeWithActor("AfterCreateTable", "skipped1"));
    assertThat(requestCount.get()).isEqualTo(3);

    Thread.sleep(250); // cooldown
    poster.post(envelopeWithActor("AfterCreateTable", "probe1")); // 500 → re-OPEN
    assertThat(requestCount.get()).isEqualTo(4);
    // Without the fix the breaker would stay CLOSED and this would reach the server.
    poster.post(envelopeWithActor("AfterCreateTable", "skipped2"));
    assertThat(requestCount.get()).isEqualTo(4);

    Thread.sleep(250); // cooldown again
    poster.post(envelopeWithActor("AfterCreateTable", "probe2"));
    assertThat(requestCount.get()).isEqualTo(5);

    poster.close();
  }

  // ============= 4xx classification =============

  @Test
  void nonRetryable4xxDoesNotOpenCircuitOrBufferForReplay() throws Exception {
    AtomicInteger errorsRemaining = new AtomicInteger(5);
    AtomicInteger requestCount = new AtomicInteger();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          requestCount.incrementAndGet();
          if (errorsRemaining.getAndDecrement() > 0) exchange.sendResponseHeaders(403, -1);
          else exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), 3, Duration.ofSeconds(30), 10));

    // 5 emits past the threshold; 403 must not trip the breaker or buffer for replay.
    for (int i = 0; i < 5; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    assertThat(requestCount.get()).isEqualTo(5);

    // Server now returns 200; any buffered replay would surface as an additional hit.
    Thread.sleep(500);
    assertThat(requestCount.get()).isEqualTo(5);

    poster.close();
  }

  @Test
  void retryable429OpensCircuitAndBuffersForReplay() throws Exception {
    final int n = 3;
    AtomicInteger errorsRemaining = new AtomicInteger(n);
    CountDownLatch successLatch = new CountDownLatch(n);
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

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), n, Duration.ofMillis(200), 10));

    for (int i = 0; i < n; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    assertThat(requestCount.get()).isEqualTo(n);

    // Cooldown elapses, replayer drains the buffer, server returns 200 → all n redelivered.
    assertThat(successLatch.await(10, TimeUnit.SECONDS)).isTrue();
    poster.close();
  }

  // ============= Replay buffer =============

  @Test
  void replayBufferRedeliversAfterReceiverRecovers() throws Exception {
    final int n = 3;
    AtomicInteger errorsRemaining = new AtomicInteger(n);
    CountDownLatch deliveredLatch = new CountDownLatch(n);
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

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), 100, Duration.ofSeconds(30), 10));

    for (int i = 0; i < n; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    assertThat(deliveredLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(n);

    poster.close();
  }

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
                    "POST", "/", null, null, new String(body, StandardCharsets.UTF_8)));
            exchange.sendResponseHeaders(200, -1);
            deliveredLatch.countDown();
          }
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), 100, Duration.ofSeconds(30), 2));

    poster.post(envelopeWithActor("AfterCreateTable", "v_a"));
    poster.post(envelopeWithActor("AfterCreateTable", "v_b"));
    poster.post(envelopeWithActor("AfterCreateTable", "v_c"));
    poster.post(envelopeWithActor("AfterCreateTable", "v_d"));
    poster.post(envelopeWithActor("AfterCreateTable", "v_e"));

    assertThat(deliveredLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(recorded).hasSize(2);
    List<String> bodies = recorded.stream().map(r -> r.body).toList();
    assertThat(bodies).anyMatch(b -> b.contains("v_d")).anyMatch(b -> b.contains("v_e"));
    assertThat(bodies)
        .noneMatch(b -> b.contains("v_a"))
        .noneMatch(b -> b.contains("v_b"))
        .noneMatch(b -> b.contains("v_c"));

    poster.close();
  }

  @Test
  void replayBufferSurvivesFailedProbeAndDeliversAfterEventualRecovery() throws Exception {
    final int n = 3;
    AtomicInteger errorsRemaining = new AtomicInteger(n + 1); // n trip + 1 failed probe
    AtomicInteger requestCount = new AtomicInteger();
    CountDownLatch deliveredLatch = new CountDownLatch(n);
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

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), n, Duration.ofMillis(200), 10));

    for (int i = 0; i < n; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    assertThat(requestCount.get()).isEqualTo(n);

    // probe #1 still 500 → HALF_OPEN→OPEN with new cooldown, head stays in buffer.
    // probe #2 gets 200 → CLOSED → drain rest of buffer.
    assertThat(deliveredLatch.await(15, TimeUnit.SECONDS)).isTrue();
    synchronized (deliveredBodies) {
      assertThat(deliveredBodies).hasSize(n);
      for (int i = 0; i < n; i++) {
        final String marker = "v" + i;
        assertThat(deliveredBodies.stream().anyMatch(b -> b.contains(marker)))
            .as("value %s should have been redelivered, not lost", marker)
            .isTrue();
      }
    }
    poster.close();
  }

  @Test
  void halfOpenAllowsOnlyOneProbeUnderConcurrentEmits() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    AtomicInteger errorsRemaining = new AtomicInteger(3);
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

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(50, true, Duration.ofSeconds(30), 3, Duration.ofMillis(200), 0));

    for (int i = 0; i < 3; i++) poster.post(envelopeWithActor("AfterCreateTable", "v" + i));
    assertThat(requestCount.get()).isEqualTo(3);

    Thread.sleep(250); // exceed cooldown

    int concurrent = 20;
    Thread[] threads = new Thread[concurrent];
    for (int i = 0; i < concurrent; i++) {
      final int idx = i;
      threads[i] =
          new Thread(() -> poster.post(envelopeWithActor("AfterCreateTable", "c" + idx)));
      threads[i].start();
    }
    assertThat(probeArrived.await(10, TimeUnit.SECONDS)).isTrue();
    // Only the single probe must have reached the server while the others were rejected by the
    // HALF_OPEN gateway. requestCount = 3 (trip) + 1 (probe).
    Thread.sleep(200); // give losers time to call into the gateway and bounce off
    assertThat(requestCount.get()).isEqualTo(4);

    holdProbe.countDown();
    for (Thread t : threads) t.join(10_000);
    poster.close();
  }

  // ============= Same-resource purge (Finding #2 regression) =============

  /**
   * Drop-then-recreate race regression. Real-world sequence:
   * <ol>
   *   <li>{@code AfterDropTable cat/ns/t} fails during a receiver outage → buffered.
   *   <li>Receiver recovers; user recreates the same table → {@code AfterCreateTable cat/ns/t}
   *       posts successfully directly.
   *   <li>Without purge, the still-buffered DROP would be replayed onto the freshly created
   *       table, deleting it on the receiver.
   * </ol>
   * With purge, the buffered DROP is dropped before the replayer can deliver it.
   */
  @Test
  void successfulDirectEmitPurgesOlderBufferedEntryForSameResource() throws Exception {
    // Server logic is body-keyed (not request-counted) so the test is race-free even if the
    // daemon replayer fires between the two posts: anything containing "v_drop" is always 500
    // (would land in buffer), "v_create" is always 200 (the direct-emit success that triggers
    // purge). With sync=true, the third assertion runs strictly after the purge has completed.
    final List<String> deliveredBodies = new ArrayList<>();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          String s = new String(body, StandardCharsets.UTF_8);
          if (s.contains("v_create")) {
            synchronized (deliveredBodies) {
              deliveredBodies.add(s);
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            exchange.sendResponseHeaders(500, -1);
          }
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(
                /*inflight*/ 10,
                /*sync*/ true,
                /*requestTimeout*/ Duration.ofSeconds(2),
                /*circuitThreshold*/ 100, // never trip — keep replayer free to attempt
                /*circuitOpen*/ Duration.ofSeconds(30),
                /*replayCap*/ 10));

    long dropSeq = 100L;
    long createSeq = 200L; // strictly greater than dropSeq — purge gates on `<`
    poster.post(envelopeForResource("AfterDropTable", "cat", "tbl", dropSeq, "v_drop"));
    poster.post(envelopeForResource("AfterCreateTable", "cat", "tbl", createSeq, "v_create"));

    // sync=true ensures the purge has completed by the time the second post returns. The
    // buffered drop should now be evicted, so the buffer is empty.
    assertThat(poster.replayBufferSize())
        .as("v_drop (seq=100 < 200) must be purged by the successful create at seq=200")
        .isEqualTo(0);

    // Wait briefly to confirm the daemon replayer does NOT deliver a stale drop — the server
    // would never accept it anyway, but a 4th request reaching the server would surface as
    // a body containing "v_drop" (it wouldn't be — server returns 500), so we just confirm
    // deliveredBodies only contains the one create.
    Thread.sleep(400);
    synchronized (deliveredBodies) {
      assertThat(deliveredBodies).hasSize(1);
      assertThat(deliveredBodies.get(0)).contains("v_create");
    }

    poster.close();
  }

  @Test
  void successfulEmitDoesNotPurgeNewerBufferedSameResourceEntries() throws Exception {
    // The opposite guard: a direct emit must NOT evict buffered same-resource entries with
    // larger seq, because those reflect even more recent state and still need delivery.
    //
    // Setup: server accepts v_middle always, accepts v_newest only after acceptNewest flips
    // (so the replayer can drain v_newest cleanly after purge). Anything else (v_oldest) gets
    // 500 forever — the replayer's repeated attempts on v_oldest can never fool the test into
    // "v_oldest was delivered". Buffer state is asserted right after the sync direct emit
    // returns, before the daemon replayer can affect what's left in the queue.
    final java.util.concurrent.atomic.AtomicBoolean acceptNewest =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    final List<String> deliveredBodies = new ArrayList<>();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          String s = new String(body, StandardCharsets.UTF_8);
          boolean accept = s.contains("v_middle") || (acceptNewest.get() && s.contains("v_newest"));
          if (accept) {
            synchronized (deliveredBodies) {
              deliveredBodies.add(s);
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            exchange.sendResponseHeaders(500, -1);
          }
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(
                /*inflight*/ 10,
                /*sync*/ true,
                /*requestTimeout*/ Duration.ofSeconds(2),
                /*circuitThreshold*/ 100,
                /*circuitOpen*/ Duration.ofSeconds(30),
                /*replayCap*/ 10));

    poster.post(envelopeForResource("AfterCreateTable", "cat", "tbl", 50L, "v_oldest"));
    poster.post(envelopeForResource("AfterUpdateTable", "cat", "tbl", 300L, "v_newest"));
    // sync=true: purge has completed by the time the third post returns.
    poster.post(envelopeForResource("AfterCreateTable", "cat", "tbl", 200L, "v_middle"));

    assertThat(poster.replayBufferSize())
        .as("v_oldest (seq=50 < 200) purged; v_newest (seq=300 > 200) survives")
        .isEqualTo(1);

    // Now let the replayer drain the survivor.
    acceptNewest.set(true);
    long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      synchronized (deliveredBodies) {
        if (deliveredBodies.stream().anyMatch(b -> b.contains("v_newest"))) break;
      }
      Thread.sleep(100);
    }

    synchronized (deliveredBodies) {
      assertThat(deliveredBodies).anyMatch(b -> b.contains("v_middle"));
      assertThat(deliveredBodies)
          .as("v_newest had seq=300 > 200 (the successful emit) so it must survive purge")
          .anyMatch(b -> b.contains("v_newest"));
      assertThat(deliveredBodies)
          .as("v_oldest had seq=50 < 200 so it must be purged before replay")
          .noneMatch(b -> b.contains("v_oldest"));
    }

    poster.close();
  }

  @Test
  void successfulEmitDoesNotPurgeBufferedEntriesForDifferentResource() throws Exception {
    // Purge keys on the (catalog, namespace, table) tuple. A successful emit for tableA must
    // not evict buffered emits for tableB on the same catalog, and vice versa. Server logic is
    // body-keyed: v_B_create always 200 (the direct success that triggers purge), v_A_drop is
    // accepted only after acceptDrop flips (so we can confirm the survivor by letting the
    // replayer drain it after the buffer-state assertion).
    final java.util.concurrent.atomic.AtomicBoolean acceptDrop =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    final List<String> deliveredBodies = new ArrayList<>();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          String s = new String(body, StandardCharsets.UTF_8);
          boolean accept = s.contains("v_B_create") || (acceptDrop.get() && s.contains("v_A_drop"));
          if (accept) {
            synchronized (deliveredBodies) {
              deliveredBodies.add(s);
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            exchange.sendResponseHeaders(500, -1);
          }
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), 100, Duration.ofSeconds(30), 10));

    poster.post(envelopeForResource("AfterDropTable", "cat", "tableA", 100L, "v_A_drop"));
    // Different resource, even though catalog + seq might tempt a naive matcher.
    poster.post(envelopeForResource("AfterCreateTable", "cat", "tableB", 200L, "v_B_create"));

    // sync=true: purge has completed by the time the second post returns. Since tableA and
    // tableB have different resourceKeys, the buffered tableA drop must NOT be evicted.
    assertThat(poster.replayBufferSize())
        .as("different resource — buffered tableA drop must still be present in the queue")
        .isEqualTo(1);

    // Confirm survivor identity by letting the replayer deliver it.
    acceptDrop.set(true);
    long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      synchronized (deliveredBodies) {
        if (deliveredBodies.stream().anyMatch(b -> b.contains("v_A_drop"))) break;
      }
      Thread.sleep(100);
    }

    synchronized (deliveredBodies) {
      assertThat(deliveredBodies).anyMatch(b -> b.contains("v_B_create"));
      assertThat(deliveredBodies)
          .as("different resource — buffered tableA drop must still be replayed")
          .anyMatch(b -> b.contains("v_A_drop"));
    }

    poster.close();
  }

  @Test
  void successfulCommitOnRenameDestinationPurgesBufferedRename() throws Exception {
    // Adversarial review #2 regression. Without the dual-key purge, the buffered rename's
    // resourceKey would only reference the SOURCE (tableA), so a successful direct commit on the
    // DESTINATION (tableB) would not evict it. After cooldown the replayer would replay the old
    // rename onto tableB, overwriting the post-commit state on the receiver. With the dual-key
    // fix, resourceKeys() of the rename also includes tableB, so the purge does match.
    final java.util.concurrent.atomic.AtomicBoolean acceptCommit =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    final List<String> deliveredBodies = new ArrayList<>();
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          String s = new String(body, StandardCharsets.UTF_8);
          // Reject the rename always (so it lands in the buffer). Accept the commit only after
          // we flip the gate, so the test can observe the buffer-state assertion before the
          // poster's success path runs.
          boolean accept = acceptCommit.get() && s.contains("v_commit");
          if (accept) {
            synchronized (deliveredBodies) {
              deliveredBodies.add(s);
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            exchange.sendResponseHeaders(500, -1);
          }
          exchange.close();
        });

    HttpEventPoster poster =
        new HttpEventPoster(
            configWithResilience(10, true, Duration.ofSeconds(2), 100, Duration.ofSeconds(30), 10));

    // Buffer a rename tableA -> tableB at seq=100.
    poster.post(renameEnvelope("cat", "tableA", "tableB", 100L, "v_rename"));
    assertThat(poster.replayBufferSize()).isEqualTo(1);

    // Direct commit on tableB at seq=200 — newer seq and overlapping resource (destination key
    // of the rename == source key of the commit). Purge should evict the buffered rename.
    acceptCommit.set(true);
    poster.post(envelopeForResource("AfterCommitTable", "cat", "tableB", 200L, "v_commit"));

    assertThat(poster.replayBufferSize())
        .as(
            "buffered rename old->new must be purged by a newer successful commit landing on new"
                + " — the rename's resourceKeys() must surface both source AND destination")
        .isEqualTo(0);

    synchronized (deliveredBodies) {
      assertThat(deliveredBodies).hasSize(1).first().asString().contains("v_commit");
      assertThat(deliveredBodies)
          .as("rename must never be delivered — it was purged before replay")
          .noneMatch(b -> b.contains("v_rename"));
    }

    poster.close();
  }

  // ============= helpers =============

  /** Monotonic counter so each helper-built envelope gets a unique sequenceNumber. */
  private static final java.util.concurrent.atomic.AtomicLong TEST_SEQ =
      new java.util.concurrent.atomic.AtomicLong();

  private static EventEnvelope envelope(String type, String catalog, String table) {
    return new EventEnvelope(
        "evt-" + TEST_SEQ.get(),
        TEST_SEQ.incrementAndGet(),
        type,
        1L,
        "test-actor",
        null, // realm
        catalog,
        null,
        null,
        table,
        null,
        null,
        null,
        null,
        null);
  }

  /** Like {@link #envelope} but lets the test recognize each envelope by its actor field. */
  private static EventEnvelope envelopeWithActor(String type, String actorMarker) {
    return new EventEnvelope(
        "evt-" + TEST_SEQ.get(),
        TEST_SEQ.incrementAndGet(),
        type,
        1L,
        actorMarker,
        null, // realm
        "cat",
        null,
        null,
        "tbl",
        null,
        null,
        null,
        null,
        null);
  }

  /**
   * Envelope variant where the test controls the resource tuple AND the sequence number — used
   * by the same-resource purge regressions where the relative ordering of seqs is load-bearing.
   */
  private static EventEnvelope envelopeForResource(
      String type, String catalog, String table, long seq, String actorMarker) {
    return new EventEnvelope(
        "evt-" + seq,
        seq,
        type,
        1L,
        actorMarker,
        null, // realm
        catalog,
        null,
        null,
        table,
        null,
        null,
        null,
        null,
        null);
  }

  /**
   * Rename-specific helper — populates both the source (namespace/table) and the destination
   * (renameTo). Exercises the dual-key purge path that the single-resource helpers can't reach.
   */
  private static EventEnvelope renameEnvelope(
      String catalog, String fromTable, String toTable, long seq, String actorMarker) {
    return new EventEnvelope(
        "evt-" + seq,
        seq,
        "AfterRenameTable",
        1L,
        actorMarker,
        null, // realm
        catalog,
        null,
        null,
        fromTable,
        null,
        null,
        new EventEnvelope.RenameTarget(null, toTable),
        null,
        null);
  }

  private RestForwarderConfiguration buildConfig(boolean sync, String token) {
    int port = server.getAddress().getPort();
    return configWith(
        Optional.of("http://127.0.0.1:" + port), token, sync, 1000, Duration.ofSeconds(30));
  }

  private RestForwarderConfiguration configWith(
      Optional<String> endpointUrl,
      String token,
      boolean sync,
      int inflight,
      Duration requestTimeout) {
    return new RestForwarderConfiguration() {
      @Override
      public Optional<String> endpointUrl() {
        return endpointUrl;
      }

      @Override
      public Optional<String> token() {
        return Optional.ofNullable(token);
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
        return 100;
      }

      @Override
      public Duration circuitBreakerOpenDuration() {
        return Duration.ofSeconds(30);
      }

      @Override
      public int replayBufferCapacity() {
        return 0;
      }
    };
  }

  private RestForwarderConfiguration configWithResilience(
      int inflight,
      boolean sync,
      Duration requestTimeout,
      int circuitThreshold,
      Duration circuitOpen,
      int replayCap) {
    return new RestForwarderConfiguration() {
      @Override
      public Optional<String> endpointUrl() {
        return Optional.of("http://127.0.0.1:" + server.getAddress().getPort());
      }

      @Override
      public Optional<String> token() {
        return Optional.empty();
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
    };
  }

  /**
   * Single config instance that lets the test flip endpoint URL / request timeout between two
   * emits so the permit-release invariant can be observed on the SAME poster. The poster reads
   * {@code config.endpointUrl()} / {@code config.requestTimeout()} fresh per call, so swapping
   * the values via {@link AtomicReference} is sufficient to change behavior between calls.
   */
  private RestForwarderConfiguration sameEmitterCfg(
      AtomicReference<String> url, AtomicReference<Duration> requestTimeout) {
    return new RestForwarderConfiguration() {
      @Override
      public Optional<String> endpointUrl() {
        return Optional.of(url.get());
      }

      @Override
      public Optional<String> token() {
        return Optional.empty();
      }

      @Override
      public boolean synchronousMode() {
        return false;
      }

      @Override
      public Duration connectTimeout() {
        return Duration.ofSeconds(5);
      }

      @Override
      public Duration requestTimeout() {
        return requestTimeout.get();
      }

      @Override
      public int maxInflightEmits() {
        return 1; // load-bearing: the permit-release assertion depends on cap=1
      }

      @Override
      public int circuitBreakerFailureThreshold() {
        return 100;
      }

      @Override
      public Duration circuitBreakerOpenDuration() {
        return Duration.ofSeconds(30);
      }

      @Override
      public int replayBufferCapacity() {
        return 0;
      }
    };
  }

  private record RecordedRequest(
      String method, String path, String authorization, String contentType, String body) {}
}
