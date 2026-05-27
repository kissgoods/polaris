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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.extension.forwarder.EventEnvelope;
import org.apache.polaris.extension.forwarder.RestForwarderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends {@link EventEnvelope} payloads via plain HTTP POST to the configured receiver. Adds zero
 * new runtime dependencies beyond Jackson (already on the Polaris classpath) and the JDK 11+
 * {@link HttpClient}.
 *
 * <p>Isolation guarantees against a slow or unavailable receiver:
 *
 * <ul>
 *   <li><b>Connect timeout</b> on the {@link HttpClient} — caps how long a TCP handshake can
 *       block.
 *   <li><b>Request timeout</b> on each {@link HttpRequest} — caps the end-to-end wait for a
 *       single emit; in synchronous mode this is the maximum a Polaris request can stall.
 *   <li><b>Inflight semaphore</b> — bounds the number of concurrently-pending emits so a
 *       sustained outage cannot grow memory, sockets, or threads unboundedly. Excess emits are
 *       dropped (WARN logged with rate-limited counter) rather than queued.
 *   <li><b>Circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED|OPEN)</b> — after {@code
 *       circuitBreakerFailureThreshold} consecutive emit failures, the circuit transitions to
 *       OPEN and emits are skipped for {@code circuitBreakerOpenDuration}. Once the cooldown
 *       elapses, a single CAS-claimed probe enters HALF_OPEN; concurrent emits are skipped while
 *       the probe is in flight. The probe's outcome moves the circuit back to CLOSED (resume) or
 *       OPEN with a fresh cooldown (still down). Avoids a thundering-herd at the cooldown edge
 *       against a still-degraded receiver.
 *   <li><b>Replay buffer</b> — failed/skipped emits are enqueued to a bounded in-memory buffer;
 *       a background daemon thread re-attempts the head when the receiver becomes reachable
 *       again, using peek-and-keep so a failed probe leaves the item in place instead of
 *       silently losing it. Drop-oldest on overflow.
 * </ul>
 *
 * <p>Polaris catalog operations are never blocked or failed by receiver-side issues. The poster
 * also runs a <b>same-resource older-sequence purge</b>: when a direct emit succeeds, any
 * buffered emit for the same (catalog, namespace, table) tuple with a smaller {@link
 * EventEnvelope#sequenceNumber} is dropped (and flagged stale to catch the in-flight
 * peek-before-send race). This prevents a classic drop-then-recreate race where the buffered
 * delete would otherwise be replayed onto a freshly recreated resource. Receivers should still
 * dedup on {@link EventEnvelope#eventId} to absorb timeout-after-commit doubles that the
 * listener can't suppress (the receiver already committed before the listener saw the timeout).
 */
public class HttpEventPoster implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HttpEventPoster.class);

  /** Log dropped emits at most once per this many drops to avoid log flooding during outages. */
  private static final long DROP_LOG_INTERVAL = 100L;

  /** Replayer poll interval when the buffer is empty or the circuit is open. */
  private static final long REPLAY_POLL_INTERVAL_MS = 200L;

  /** Maximum time {@link #close()} waits for the replayer thread to exit. */
  private static final long SHUTDOWN_GRACE_MS = 1_000L;

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final RestForwarderConfiguration config;
  private final Semaphore inflight;
  private final AtomicLong droppedCount = new AtomicLong();

  // Circuit breaker — explicit CLOSED → OPEN → HALF_OPEN(probe) → CLOSED|OPEN state machine.
  // HALF_OPEN is claimed by a single CAS-winning caller after cooldown; this prevents the
  // cooldown-edge burst where N concurrent emits/replays would otherwise all see "elapsed >
  // cooldown" and rush the receiver together.
  //
  // The state and the cooldown timestamp are bundled into an immutable snapshot stored in a
  // single AtomicReference. Every transition (open, probe-claim, probe-release, close) is a CAS
  // on the whole snapshot, so a state change and its accompanying timestamp are observed
  // atomically. Without this, a brief window between "state=OPEN" and "timestamp=now" lets a
  // concurrent emit read a stale timestamp, decide the cooldown has elapsed, and immediately
  // grab a probe slot — defeating the breaker under high traffic.
  private enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private record CircuitSnapshot(CircuitState state, long openedAtMs) {}

  private static final CircuitSnapshot INITIAL_CIRCUIT =
      new CircuitSnapshot(CircuitState.CLOSED, 0L);

  private final AtomicReference<CircuitSnapshot> circuit = new AtomicReference<>(INITIAL_CIRCUIT);
  private final AtomicInteger consecutiveFailures = new AtomicInteger();
  private final AtomicBoolean openLogged = new AtomicBoolean(false);

  // Replay buffer (null when capacity == 0, i.e. disabled)
  private final BlockingQueue<EmitTask> replayBuffer;
  private final Thread replayer;
  private final AtomicLong bufferDropCount = new AtomicLong();
  private final Object bufferLock = new Object();
  private volatile boolean shuttingDown = false;

  public HttpEventPoster(RestForwarderConfiguration config) {
    this(
        config,
        // HTTP/1.1 강제: JDK 21 의 HttpClient 기본 동작은 매 새 연결마다 HTTP/2 cleartext
        // (h2c) upgrade 를 먼저 시도한 뒤 실패하면 HTTP/1.1 로 fallback 한다. 받는 server
        // (uvicorn, Caddy 의 기본 설정, 일부 reverse proxy) 가 h2c 를 지원하지 않으면
        // 첫 시도가 body 를 잃은 채 서버에 도달해 400 으로 응답되거나 'Unsupported upgrade
        // request' 경고를 찍는다. 그 후 JDK 가 1.1 로 재시도해서 결과적으로 envelope 가
        // 전달되지만, 매 emit 마다 두 번 요청이 일어나고 receiver access log 도 noise 가
        // 끼인다. receiver 가 HTTP/1.1 만 받는 게 가장 흔한 운영 환경이므로 처음부터
        // HTTP/1.1 로 고정한다. (HTTPS 엔드포인트라면 ALPN 으로 협상되므로 이 강제가
        // 큰 손해가 아니다.)
        // followRedirects(NORMAL): 3xx (301/302/307/308) 을 자동 follow 한다. JDK
        // HttpClient 의 기본은 NEVER 라 redirect 응답이 그대로 caller 에 전달되는데,
        // isRetryableFailure 가 3xx 를 retryable 4xx 가 아닌 일반 4xx 로 분류해 envelope
        // 를 "delivered" 로 마킹하고 replay buffer 에서 purge 한다 — 결과적으로 receiver
        // 의 Location target 에 envelope 가 도달하지 않는 silent loss. ingress 의 trailing-
        // slash 정규화, HTTPS redirect, path rewrite 같이 흔한 운영 환경에서 발생하므로
        // 처음부터 자동 follow 한다. NORMAL 은 HTTPS→HTTP downgrade 만 차단해서 보안상
        // 안전한 default 다.
        HttpClient.newBuilder()
            .connectTimeout(config.connectTimeout())
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build(),
        new ObjectMapper());
  }

  HttpEventPoster(
      RestForwarderConfiguration config, HttpClient httpClient, ObjectMapper jsonMapper) {
    this.config = config;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.inflight = new Semaphore(Math.max(1, config.maxInflightEmits()));
    int cap = config.replayBufferCapacity();
    if (cap > 0) {
      this.replayBuffer = new LinkedBlockingQueue<>(cap);
      this.replayer = startReplayer();
    } else {
      this.replayBuffer = null;
      this.replayer = null;
    }
  }

  /**
   * Submit one envelope for delivery. Returns {@code true} only in sync paths (config's
   * synchronousMode flag) when the receiver actually accepted the payload OR permanently rejected
   * it with a non-retryable 4xx; in both cases the caller should consider the item handled.
   * Returns {@code false} on retryable failures, gateway skips, or in async paths where the
   * outcome isn't known by return time.
   */
  public boolean post(EventEnvelope envelope) {
    return doPost(envelope, true, false);
  }

  @Override
  public void close() {
    shuttingDown = true;
    if (replayer != null) {
      replayer.interrupt();
      try {
        replayer.join(SHUTDOWN_GRACE_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    httpClient.close();
  }

  /**
   * Whether the circuit breaker is currently in a non-CLOSED state. Visible for tests /
   * monitoring. A {@code true} return only means "not CLOSED right now"; the cooldown / probe
   * handoff is done in {@link #tryEnterCircuit}.
   */
  boolean isCircuitOpen() {
    return circuit.get().state != CircuitState.CLOSED;
  }

  /** Current replay buffer depth. Visible for tests / monitoring. */
  int replayBufferSize() {
    return replayBuffer == null ? 0 : replayBuffer.size();
  }

  private boolean doPost(
      final EventEnvelope envelope, final boolean enqueueOnFailure, final boolean forceSync) {
    final String json;
    try {
      json = jsonMapper.writeValueAsString(envelope);
    } catch (Exception e) {
      LOG.error("Skipping rest-forwarder emit: failed to serialize envelope", e);
      return false;
    }

    final String endpointUrl = config.endpointUrl().orElse(null);
    if (endpointUrl == null) {
      LOG.error(
          "Skipping rest-forwarder emit: polaris.event-listener.rest-forwarder.endpoint-url is not"
              + " configured");
      return false;
    }

    final CircuitState entered = tryEnterCircuit();
    if (entered == null) {
      if (openLogged.compareAndSet(false, true)) {
        LOG.warn(
            "rest-forwarder circuit OPEN after {} consecutive failures — skipping emits for ~{}s",
            config.circuitBreakerFailureThreshold(),
            config.circuitBreakerOpenDuration().toSeconds());
      }
      if (enqueueOnFailure) offerToReplayBuffer(envelope);
      return false;
    }
    final boolean isProbe = (entered == CircuitState.HALF_OPEN);

    if (!inflight.tryAcquire()) {
      if (isProbe) releaseProbeSlot();
      long n = droppedCount.incrementAndGet();
      if (n == 1L || n % DROP_LOG_INTERVAL == 0L) {
        LOG.warn(
            "Dropping rest-forwarder emit for {}: inflight limit ({}) reached — receiver may be"
                + " slow or unavailable. total dropped so far: {}",
            envelope.eventType(),
            config.maxInflightEmits(),
            n);
      }
      if (enqueueOnFailure) offerToReplayBuffer(envelope);
      return false;
    }

    // Once the permit is held, ANY exception path on the way to scheduling sendAsync must release
    // the permit and swallow the failure so receiver config errors (malformed endpoint-url,
    // invalid request-timeout, non-http scheme, etc.) cannot propagate into Polaris.
    boolean scheduled = false;
    CompletableFuture<HttpResponse<String>> future = null;
    final AtomicBoolean handled = new AtomicBoolean(false);
    try {
      HttpRequest.Builder reqBuilder =
          HttpRequest.newBuilder(URI.create(endpointUrl))
              .timeout(config.requestTimeout())
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
      config.token().ifPresent(t -> reqBuilder.header("Authorization", "Bearer " + t));
      HttpRequest req = reqBuilder.build();

      future =
          httpClient
              .sendAsync(req, HttpResponse.BodyHandlers.ofString())
              .whenComplete(
                  (resp, err) -> {
                    try {
                      int statusCode = (err == null) ? resp.statusCode() : 0;
                      boolean success = err == null && statusCode / 100 == 2;
                      if (success) {
                        recordSuccess(entered);
                        handled.set(true);
                        // A direct emit that the receiver accepted (2xx) supersedes pending
                        // same-resource entries in the replay buffer whose sequence is OLDER
                        // than this one. Without this, a real-world sequence such as
                        // {drop t1 (fails → buffered), recreate t1 (succeeds), buffered drop
                        // replayed after cooldown} would delete the freshly recreated table on
                        // the receiver. Gating on seq < this.seq keeps newer same-resource
                        // buffered entries alive — they reflect even more recent state and
                        // must still be delivered.
                        purgeBufferedOlderSameResource(envelope);
                      } else if (isRetryableFailure(err, statusCode)) {
                        if (err != null) {
                          // WARN (not ERROR): transient outages are expected; circuit breaker
                          // + replay buffer handle recovery without operator action.
                          LOG.warn(
                              "rest-forwarder HTTP emit failed for {}",
                              envelope.eventType(),
                              err);
                        } else {
                          LOG.warn(
                              "rest-forwarder HTTP emit retryable non-2xx for {}: {} {}",
                              envelope.eventType(),
                              statusCode,
                              resp.body());
                        }
                        recordFailure(entered);
                        if (enqueueOnFailure) offerToReplayBuffer(envelope);
                      } else {
                        // Non-retryable 4xx (e.g. 400 schema, 401/403 auth, 404 not found):
                        // retrying or buffering would never recover. Log and drop — this must NOT
                        // count toward circuit failures or fill the replay buffer, otherwise a
                        // persistent client-side misconfig would falsely open the breaker and
                        // starve healthy emits. Treated as "handled" so the replayer removes it.
                        LOG.warn(
                            "rest-forwarder HTTP emit dropped (non-retryable {}) for {}: {}",
                            statusCode,
                            envelope.eventType(),
                            resp.body());
                        // A non-retryable response from a HALF_OPEN probe still indicates the
                        // receiver is reachable, so close the circuit just like a 2xx would.
                        recordSuccess(entered);
                        handled.set(true);
                      }
                    } finally {
                      inflight.release();
                    }
                  });
      scheduled = true;
    } catch (Exception e) {
      LOG.error(
          "Skipping rest-forwarder emit for {}: failed to build or schedule request"
              + " (check endpoint-url, request-timeout, and other listener config)",
          envelope.eventType(),
          e);
    } finally {
      if (!scheduled) {
        if (isProbe) releaseProbeSlot();
        inflight.release();
      }
    }

    if (scheduled && (config.synchronousMode() || forceSync)) {
      try {
        future.join();
        return handled.get();
      } catch (Exception e) {
        LOG.warn("rest-forwarder HTTP emit failed (sync) for {}", envelope.eventType(), e);
        return false;
      }
    }
    return false;
  }

  /**
   * Gateway for an outgoing emit. Returns the state the caller is sending under, or {@code null}
   * if the caller must skip.
   *
   * <ul>
   *   <li>{@link CircuitState#CLOSED} → normal traffic; no state change.
   *   <li>{@link CircuitState#HALF_OPEN} → this caller is the probe (won the CAS). On any failure
   *       to actually schedule the request, the caller must release the probe slot via {@link
   *       #releaseProbeSlot()} so other callers aren't permanently locked out.
   *   <li>{@code null} → blocked (OPEN within cooldown, HALF_OPEN with a probe already in flight,
   *       or lost the CAS race for the probe slot).
   * </ul>
   */
  private CircuitState tryEnterCircuit() {
    while (true) {
      CircuitSnapshot snap = circuit.get();
      if (snap.state == CircuitState.CLOSED) return CircuitState.CLOSED;
      if (snap.state == CircuitState.HALF_OPEN) return null;
      // OPEN — cooldown check against the timestamp carried in this exact snapshot.
      long elapsed = System.currentTimeMillis() - snap.openedAtMs;
      if (elapsed < config.circuitBreakerOpenDuration().toMillis()) return null;
      CircuitSnapshot probe = new CircuitSnapshot(CircuitState.HALF_OPEN, snap.openedAtMs);
      if (circuit.compareAndSet(snap, probe)) return CircuitState.HALF_OPEN;
      // CAS lost — re-read and re-decide.
    }
  }

  /**
   * Revert HALF_OPEN → OPEN when we couldn't actually schedule the probe request. The cooldown
   * timestamp is refreshed to {@code now} so a probe that never reached the network (inflight cap
   * full, malformed URL / invalid request-timeout, etc.) doesn't immediately let the next emit
   * grab another HALF_OPEN slot.
   */
  private void releaseProbeSlot() {
    while (true) {
      CircuitSnapshot snap = circuit.get();
      if (snap.state != CircuitState.HALF_OPEN) return;
      CircuitSnapshot reverted =
          new CircuitSnapshot(CircuitState.OPEN, System.currentTimeMillis());
      if (circuit.compareAndSet(snap, reverted)) return;
    }
  }

  /**
   * Record a successful emit. The state under which the request entered the gateway ({@code
   * entered}) decides whether to flip the circuit to CLOSED — only an explicit HALF_OPEN probe is
   * allowed to do that. A success arriving from an emit that entered while CLOSED (i.e. a stale
   * in-flight request that started before a concurrent trip moved the circuit to OPEN) must NOT
   * override the OPEN state and short-circuit the cooldown.
   */
  private void recordSuccess(CircuitState entered) {
    consecutiveFailures.set(0);
    if (entered == CircuitState.HALF_OPEN) {
      while (true) {
        CircuitSnapshot snap = circuit.get();
        if (snap.state != CircuitState.HALF_OPEN) {
          // A concurrent recordFailure for this same probe already moved us elsewhere (most
          // likely back to OPEN with a fresh cooldown). Honor that verdict.
          return;
        }
        if (circuit.compareAndSet(snap, new CircuitSnapshot(CircuitState.CLOSED, 0L))) {
          openLogged.set(false);
          LOG.info("rest-forwarder circuit CLOSED — emits resuming");
          return;
        }
      }
    }
    // entered == CLOSED: success from a normal emit; state is left alone.
  }

  /**
   * Record a failed emit. As with {@link #recordSuccess}, the verdict is driven by the state the
   * request entered under — a failure from a stale CLOSED-entered emit must not be misread as a
   * failed HALF_OPEN probe and reset the cooldown.
   */
  private void recordFailure(CircuitState entered) {
    int n = consecutiveFailures.incrementAndGet();
    if (entered == CircuitState.HALF_OPEN) {
      // Probe failed — re-OPEN with a FRESH cooldown timestamp, published atomically with the
      // state change so a concurrent emit can never read (OPEN, old timestamp).
      while (true) {
        CircuitSnapshot snap = circuit.get();
        if (snap.state != CircuitState.HALF_OPEN) return;
        CircuitSnapshot reopened =
            new CircuitSnapshot(CircuitState.OPEN, System.currentTimeMillis());
        if (circuit.compareAndSet(snap, reopened)) {
          openLogged.set(false);
          return;
        }
      }
    } else if (entered == CircuitState.CLOSED
        && n >= config.circuitBreakerFailureThreshold()) {
      // First trip from CLOSED.
      while (true) {
        CircuitSnapshot snap = circuit.get();
        if (snap.state != CircuitState.CLOSED) return;
        CircuitSnapshot opened =
            new CircuitSnapshot(CircuitState.OPEN, System.currentTimeMillis());
        if (circuit.compareAndSet(snap, opened)) {
          openLogged.set(false);
          return;
        }
      }
    }
    // entered == CLOSED but the circuit has since moved on: the CAS-guarded loops above bail
    // out, leaving the cooldown / probe slot owned by whoever set them.
  }

  /**
   * Classifies an emit failure as retryable. Transport errors (connect/read timeouts, IO errors)
   * and transient server signals (5xx, 408 Request Timeout, 429 Too Many Requests) are
   * retryable. Other 4xx (400 schema, 401/403 auth, 404 not found, etc.) will never recover by
   * retrying and are dropped at the call site.
   */
  private static boolean isRetryableFailure(Throwable err, int statusCode) {
    if (err != null) return true;
    if (statusCode >= 500 && statusCode < 600) return true;
    return statusCode == 408 || statusCode == 429;
  }

  private void offerToReplayBuffer(EventEnvelope envelope) {
    if (replayBuffer == null) return;
    EmitTask task = new EmitTask(envelope, envelope.resourceKeys(), new AtomicBoolean(false));
    synchronized (bufferLock) {
      while (!replayBuffer.offer(task)) {
        // Buffer full — drop oldest to make room. Bounded memory at the cost of older events.
        if (replayBuffer.poll() == null) break; // race; queue drained — nothing to drop
        long n = bufferDropCount.incrementAndGet();
        if (n == 1L || n % DROP_LOG_INTERVAL == 0L) {
          LOG.warn(
              "rest-forwarder replay buffer full ({} cap) — dropped oldest event. total dropped"
                  + " from buffer: {}",
              config.replayBufferCapacity(),
              n);
        }
      }
    }
  }

  /**
   * Evict buffered emits whose set of touched resources overlaps this just-succeeded emit AND
   * whose sequence number is strictly smaller. Mark them stale FIRST so a replayer that already
   * {@link java.util.concurrent.BlockingQueue#peek peek-ed} an entry can skip the actual network
   * call between peek and {@code sendAsync} — removing from the queue alone wouldn't stop an
   * already-peeked send. (Once {@code sendAsync} has dispatched the request, JDK HttpClient does
   * not honor cancellation; the same {@code synchronous-mode=true} escape hatch documented for
   * out-of-order arrivals applies here.)
   *
   * <p>Overlap (not equality) is the right test because a rename touches both its source and
   * destination identities: a buffered {@code AfterRenameTable old -> new} should be evicted by
   * a newer successful emit landing on EITHER side, otherwise the stale rename would resurface
   * and overwrite the destination state at the receiver. {@link EventEnvelope#resourceKeys()}
   * surfaces the source and destination keys; {@link EmitTask#resourceKeys()} snapshots them at
   * buffer-time so we don't have to re-derive them per purge.
   */
  private void purgeBufferedOlderSameResource(EventEnvelope envelope) {
    if (replayBuffer == null) return;
    java.util.Set<String> succeededKeys = envelope.resourceKeys();
    if (succeededKeys.isEmpty()) return;
    long seq = envelope.sequenceNumber();
    synchronized (bufferLock) {
      replayBuffer.removeIf(
          t -> {
            if (t.envelope().sequenceNumber() >= seq) return false;
            for (String k : t.resourceKeys()) {
              if (succeededKeys.contains(k)) {
                t.stale().set(true);
                return true;
              }
            }
            return false;
          });
    }
  }

  private Thread startReplayer() {
    Thread t = new Thread(this::replayLoop, "rest-forwarder-replayer");
    t.setDaemon(true);
    t.setPriority(Thread.MIN_PRIORITY);
    t.start();
    return t;
  }

  private void replayLoop() {
    while (!shuttingDown) {
      try {
        EmitTask head = replayBuffer.peek();
        if (head == null) {
          Thread.sleep(REPLAY_POLL_INTERVAL_MS);
          continue;
        }
        // Stale check between peek and send. A concurrent direct emit for the same resource with
        // a newer seq may have purged this entry (and marked it stale) after we peeked but
        // before we hit sendAsync — typical cause is GC or scheduler pauses making the gap
        // non-trivial. Skipping a stale entry avoids resurrecting older state at the receiver
        // when the newer emit has already been accepted.
        if (head.stale().get()) {
          synchronized (bufferLock) {
            if (replayBuffer.peek() == head) {
              replayBuffer.poll();
            }
          }
          continue;
        }
        // Peek-and-keep: the buffered item stays in the queue until we know the send succeeded
        // (or was permanently rejected). doPost runs synchronously via forceSync so we can
        // observe the outcome, and with enqueueOnFailure=false so failures don't append a
        // duplicate at the tail. If doPost returns false the head remains and will be retried
        // after the next cooldown — no silent loss when the receiver is still down at probe
        // time. The circuit gateway also ensures only one HALF_OPEN probe runs at a time.
        boolean handled = doPost(head.envelope(), false, true);
        if (handled) {
          synchronized (bufferLock) {
            // Only remove if the head is still our task; drop-oldest from a concurrent enqueue
            // may have already evicted it.
            if (replayBuffer.peek() == head) {
              replayBuffer.poll();
            }
          }
        } else {
          // Failed or skipped — circuit is (re-)OPEN. Sleep so we don't busy-loop while the
          // gateway rejects every attempt during cooldown.
          Thread.sleep(REPLAY_POLL_INTERVAL_MS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        LOG.warn("rest-forwarder replayer encountered unexpected error", e);
        try {
          Thread.sleep(REPLAY_POLL_INTERVAL_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  /**
   * Buffered emit awaiting replay. {@code resourceKeys} is denormalized from the envelope at
   * enqueue time to keep the per-buffer-entry overlap check cheap and to snapshot the values
   * before the envelope record (which is immutable but referenced by other code paths) could be
   * confused with a future variant. {@code stale} is flipped by {@link
   * #purgeBufferedOlderSameResource} when a newer overlapping direct emit succeeds; the replayer
   * rereads it between peek and send.
   */
  private record EmitTask(
      EventEnvelope envelope, java.util.Set<String> resourceKeys, AtomicBoolean stale) {}
}
