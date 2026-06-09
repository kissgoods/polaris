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
package org.apache.polaris.extension.datahub.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.extension.datahub.DataHubConfiguration;
import org.apache.polaris.extension.datahub.DataHubEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends DataHub aspect upserts via plain HTTP POST to the GMS OpenAPI v3 endpoints. Adds zero new
 * runtime dependencies beyond Jackson (already on the Polaris classpath) and the JDK 11+ {@link
 * HttpClient}.
 *
 * <p>Isolation guarantees against a slow or unavailable DataHub:
 *
 * <ul>
 *   <li><b>Connect timeout</b> on the {@link HttpClient} — caps how long a TCP handshake can block.
 *   <li><b>Request timeout</b> on each {@link HttpRequest} — caps the end-to-end wait for a single
 *       emit; in synchronous mode this is the maximum a Polaris request can stall.
 *   <li><b>Inflight semaphore</b> — bounds the number of concurrently-pending emits so a sustained
 *       outage cannot grow memory, sockets, or threads unboundedly. Excess emits are dropped (WARN
 *       logged with rate-limited counter) rather than queued.
 *   <li><b>Circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED|OPEN)</b> — after {@code
 *       circuitBreakerFailureThreshold} consecutive emit failures, the circuit transitions to OPEN
 *       and emits are skipped for {@code circuitBreakerOpenDuration}. Once the cooldown elapses, a
 *       single CAS-claimed probe enters HALF_OPEN; concurrent emits are skipped while the probe is
 *       in flight. The probe's outcome moves the circuit back to CLOSED (resume) or OPEN with a
 *       fresh cooldown (still down). This avoids a thundering-herd at the cooldown edge against a
 *       still-degraded GMS.
 *   <li><b>Replay buffer</b> — failed/skipped emits are enqueued to a bounded in-memory buffer; a
 *       background daemon thread re-attempts the head when DataHub becomes reachable again, using
 *       peek-and-keep so a failed probe leaves the item in place instead of silently losing it.
 *       Drop-oldest on overflow.
 * </ul>
 *
 * <p>Polaris catalog operations are never blocked or failed by DataHub-side issues.
 */
public class HttpEmitter implements DataHubEmitter {

  private static final Logger LOG = LoggerFactory.getLogger(HttpEmitter.class);

  /** Log dropped emits at most once per this many drops to avoid log flooding during outages. */
  private static final long DROP_LOG_INTERVAL = 100L;

  /** Replayer poll interval when the buffer is empty or the circuit is open. */
  private static final long REPLAY_POLL_INTERVAL_MS = 200L;

  /** Maximum time {@link #close()} waits for the replayer thread to exit. */
  private static final long SHUTDOWN_GRACE_MS = 1_000L;

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final DataHubConfiguration config;
  private final Semaphore inflight;
  private final AtomicLong droppedCount = new AtomicLong();

  // Circuit breaker — explicit CLOSED → OPEN → HALF_OPEN(probe) → CLOSED|OPEN state machine.
  // HALF_OPEN is claimed by a single CAS-winning caller after cooldown; this prevents the
  // cooldown-edge burst where N concurrent emits/replays would otherwise all see "elapsed >
  // cooldown" and rush GMS together.
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

  // Monotonically-increasing sequence assigned at emit time. Used by purgeBufferedOlderSameUrn
  // to keep newer buffered same-URN entries safe from being evicted by a slower, older direct
  // emit whose 2xx happened to land after them — a same-URN race that emit_A (older) + emit_B
  // (newer, buffered) would otherwise resolve incorrectly.
  private final AtomicLong emitSeq = new AtomicLong();

  public HttpEmitter(DataHubConfiguration config) {
    this(
        config,
        HttpClient.newBuilder().connectTimeout(config.connectTimeout()).build(),
        new ObjectMapper());
  }

  HttpEmitter(DataHubConfiguration config, HttpClient httpClient, ObjectMapper jsonMapper) {
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

  @Override
  public void emitUpsert(String urn, String entityType, Map<String, Object> aspects) {
    // DataHub OpenAPI v3 `POST /openapi/v3/entity/{entityName}` expects a JSON ARRAY of entity
    // entries. Each entry has `urn` plus aspect names as FLAT top-level keys (NOT nested under
    // an "aspects" envelope). Each aspect value is wrapped in {"value": <payload>} per the
    // DataHub generic-entities batch ingest contract.
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("urn", urn);
    aspects.forEach((aspectName, value) -> entry.put(aspectName, Map.of("value", value)));
    // Resurrect any previously soft-deleted entity on every upsert. Without this, an
    // emitStatusRemoved → emitUpsert sequence (drop → recreate of a catalog/namespace/table at
    // the same URN) leaves the entity stuck in the "removed" state on the DataHub UI.
    entry.putIfAbsent("status", Map.of("value", Map.of("removed", false)));

    post("/openapi/v3/entity/" + entityType, List.of(entry), true, false,
        emitSeq.incrementAndGet());
  }

  @Override
  public void emitStatusRemoved(String urn, String entityType) {
    // Same shape as emitUpsert — single-entry JSON array with flat aspect keys.
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("urn", urn);
    entry.put("status", Map.of("value", Map.of("removed", true)));

    post("/openapi/v3/entity/" + entityType, List.of(entry), true, false,
        emitSeq.incrementAndGet());
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
   * Whether the circuit breaker is currently in a non-CLOSED state. Visible for tests / monitoring.
   * A {@code true} return only means "not CLOSED right now"; the cooldown / probe handoff is done
   * in {@link #tryEnterCircuit}.
   */
  boolean isCircuitOpen() {
    return circuit.get().state != CircuitState.CLOSED;
  }

  /**
   * Gateway for an outgoing emit. Returns the state the caller is sending under, or {@code null} if
   * the caller must skip.
   *
   * <ul>
   *   <li>{@link CircuitState#CLOSED} → normal traffic; no state change.
   *   <li>{@link CircuitState#HALF_OPEN} → this caller is the probe (won the CAS). On any failure
   *       to actually schedule the request, the caller must release the probe slot via {@link
   *       #releaseProbeSlot()} so other callers aren't permanently locked out.
   *   <li>{@code null} → blocked (OPEN within cooldown, HALF_OPEN with a probe already in flight,
   *       or lost the CAS race for the probe slot).
   * </ul>
   *
   * <p>The cooldown check and the OPEN→HALF_OPEN CAS read the same {@link CircuitSnapshot}, so a
   * concurrent recordFailure that has just published a new (OPEN, now) snapshot cannot be confused
   * with a stale (OPEN, old) one.
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
      // CAS lost — someone else moved the state (claimed the probe, re-opened with a new
      // cooldown, etc). Re-read and re-decide instead of acting on a stale snapshot.
    }
  }

  /**
   * Revert HALF_OPEN → OPEN when we couldn't actually schedule the probe request. The cooldown
   * timestamp is refreshed to {@code now} so a probe that never reached the network (inflight cap
   * full, malformed URL / invalid request-timeout, etc.) doesn't immediately let the next emit grab
   * another HALF_OPEN slot. Backing off the same way a real probe failure does caps log/CPU churn
   * under misconfiguration or saturation.
   */
  private void releaseProbeSlot() {
    while (true) {
      CircuitSnapshot snap = circuit.get();
      if (snap.state != CircuitState.HALF_OPEN) return;
      CircuitSnapshot reverted = new CircuitSnapshot(CircuitState.OPEN, System.currentTimeMillis());
      if (circuit.compareAndSet(snap, reverted)) return;
    }
  }

  /** Current replay buffer depth. Visible for tests / monitoring. */
  int replayBufferSize() {
    return replayBuffer == null ? 0 : replayBuffer.size();
  }

  /**
   * Sends one emit. Returns {@code true} only in sync paths (config.synchronousMode or {@code
   * forceSync}) when the server actually accepted the payload OR permanently rejected it with a
   * non-retryable 4xx; in both cases the caller (e.g. replayer) should consider the item handled
   * and stop trying. Returns {@code false} on retryable failures, gateway skips, or in async paths
   * where the outcome isn't known by return time.
   */
  private boolean post(
      final String path,
      final Object body,
      final boolean enqueueOnFailure,
      final boolean forceSync,
      final long seq) {
    final String json;
    try {
      json = jsonMapper.writeValueAsString(body);
    } catch (Exception e) {
      LOG.error("Skipping DataHub emit: failed to serialize body", e);
      return false;
    }

    final String gmsUrl = config.gmsUrl().orElse(null);
    if (gmsUrl == null) {
      LOG.error("Skipping DataHub emit: polaris.event-listener.datahub.gms-url is not configured");
      return false;
    }

    final CircuitState entered = tryEnterCircuit();
    if (entered == null) {
      if (openLogged.compareAndSet(false, true)) {
        LOG.warn(
            "DataHub circuit OPEN after {} consecutive failures — skipping emits for ~{}s",
            config.circuitBreakerFailureThreshold(),
            config.circuitBreakerOpenDuration().toSeconds());
      }
      if (enqueueOnFailure) offerToReplayBuffer(path, body, seq);
      return false;
    }
    final boolean isProbe = (entered == CircuitState.HALF_OPEN);

    if (!inflight.tryAcquire()) {
      if (isProbe) releaseProbeSlot();
      long n = droppedCount.incrementAndGet();
      if (n == 1L || n % DROP_LOG_INTERVAL == 0L) {
        LOG.warn(
            "Dropping DataHub emit for {}: inflight limit ({}) reached — DataHub may be slow or"
                + " unavailable. total dropped so far: {}",
            path,
            config.maxInflightEmits(),
            n);
      }
      if (enqueueOnFailure) offerToReplayBuffer(path, body, seq);
      return false;
    }

    // Once the permit is held, ANY exception path on the way to scheduling sendAsync must release
    // the permit and swallow the failure so DataHub config errors (malformed gms-url, invalid
    // request-timeout, non-http scheme, etc.) cannot propagate into Polaris.
    boolean scheduled = false;
    CompletableFuture<HttpResponse<String>> future = null;
    final AtomicBoolean handled = new AtomicBoolean(false);
    try {
      HttpRequest.Builder reqBuilder =
          HttpRequest.newBuilder(URI.create(gmsUrl + path))
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
                        // A direct emit that DataHub accepted (2xx) supersedes pending same-URN
                        // entries in the replay buffer ONLY IF they were enqueued from an
                        // older emit (smaller seq). Keying on URN alone would also evict newer
                        // buffered changes that raced past us, losing the most recent state;
                        // gating on seq < this emit's seq keeps newer same-URN entries safe
                        // while still cleaning up the drop→recreate stale-drop scenario.
                        // Best-effort: in-flight replay sends can't be cancelled.
                        purgeBufferedOlderSameUrn(body, seq);
                      } else if (isRetryableFailure(err, statusCode)) {
                        if (err != null) {
                          // WARN (not ERROR): transient outages are expected; circuit breaker
                          // + replay buffer handle recovery without operator action.
                          LOG.warn("DataHub HTTP emit failed for {}", path, err);
                        } else {
                          LOG.warn(
                              "DataHub HTTP emit retryable non-2xx for {}: {} {}",
                              path,
                              statusCode,
                              resp.body());
                        }
                        recordFailure(entered);
                        if (enqueueOnFailure) offerToReplayBuffer(path, body, seq);
                      } else {
                        // Non-retryable 4xx (e.g. 400 schema, 401/403 auth, 404 not found):
                        // retrying or buffering would never recover. Log and drop — this must NOT
                        // count toward circuit failures or fill the replay buffer, otherwise a
                        // persistent client-side misconfig would falsely open the breaker and
                        // starve healthy emits. Treated as "handled" so the replayer removes it.
                        LOG.warn(
                            "DataHub HTTP emit dropped (non-retryable {}) for {}: {}",
                            statusCode,
                            path,
                            resp.body());
                        // A non-retryable response from a HALF_OPEN probe still indicates DataHub
                        // is reachable, so close the circuit just like a 2xx would.
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
          "Skipping DataHub emit for {}: failed to build or schedule request"
              + " (check gms-url, request-timeout, and other listener config)",
          path,
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
        // WARN (not ERROR): same rationale as async path — transient and self-healing.
        LOG.warn("DataHub HTTP emit failed (sync) for {}", path, e);
        return false;
      }
    }
    return false;
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
          LOG.info("DataHub circuit CLOSED — emits resuming");
          return;
        }
      }
    }
    // entered == CLOSED: success from a normal emit; the state is left alone. If the circuit
    // has since been tripped to OPEN, this stale success doesn't get to undo it.
  }

  /**
   * Classifies an emit failure as retryable. Transport errors (connect/read timeouts, IO errors)
   * and transient server signals (5xx, 408 Request Timeout, 429 Too Many Requests) are retryable.
   * Other 4xx (400 schema, 401/403 auth, 404 not found, etc.) will never recover by retrying and
   * are dropped at the call site.
   */
  private static boolean isRetryableFailure(Throwable err, int statusCode) {
    if (err != null) return true;
    if (statusCode >= 500 && statusCode < 600) return true;
    return statusCode == 408 || statusCode == 429;
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
    } else if (entered == CircuitState.CLOSED && n >= config.circuitBreakerFailureThreshold()) {
      // First trip from CLOSED. Same atomicity guarantee — the (OPEN, now) snapshot is the only
      // (OPEN, *) snapshot any concurrent reader can observe at this transition.
      while (true) {
        CircuitSnapshot snap = circuit.get();
        if (snap.state != CircuitState.CLOSED) return;
        CircuitSnapshot opened = new CircuitSnapshot(CircuitState.OPEN, System.currentTimeMillis());
        if (circuit.compareAndSet(snap, opened)) {
          openLogged.set(false);
          return;
        }
      }
    }
    // entered == CLOSED but the circuit has since moved to OPEN/HALF_OPEN: the CAS-guarded loops
    // above bail out, leaving the cooldown / probe slot owned by whoever set them.
  }

  private void offerToReplayBuffer(String path, Object body, long seq) {
    if (replayBuffer == null) return;
    EmitTask task = new EmitTask(path, urnFromBody(body), seq, new AtomicBoolean(false), body);
    synchronized (bufferLock) {
      while (!replayBuffer.offer(task)) {
        // Buffer full — drop oldest to make room. Bounded memory at the cost of older events.
        if (replayBuffer.poll() == null) break; // race; queue drained — no need to drop
        long n = bufferDropCount.incrementAndGet();
        if (n == 1L || n % DROP_LOG_INTERVAL == 0L) {
          LOG.warn(
              "Replay buffer full ({} cap) — dropped oldest event. total dropped from buffer: {}",
              config.replayBufferCapacity(),
              n);
        }
      }
    }
  }

  private Thread startReplayer() {
    Thread t = new Thread(this::replayLoop, "datahub-emitter-replayer");
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
        // Stale check between peek and send. A concurrent direct emit for the same URN with a
        // newer seq may have purged this entry (and marked it stale) after we peeked but before
        // we hit sendAsync — typical cause is GC or scheduler pauses making the gap non-trivial.
        // Skipping a stale entry avoids resurrecting older state at DataHub when the newer emit
        // has already been accepted.
        //
        // KNOWN LIMIT: once sendAsync has dispatched the request, JDK HttpClient does not honor
        // CompletableFuture.cancel() — the request still reaches DataHub. If strict per-URN
        // ordering is required, set polaris.event-listener.datahub.synchronous-mode=true so
        // each emit blocks the catalog request until DataHub responds, serializing per-thread.
        if (head.stale().get()) {
          synchronized (bufferLock) {
            if (replayBuffer.peek() == head) {
              replayBuffer.poll();
            }
          }
          continue;
        }
        // Peek-and-keep: the buffered item stays in the queue until we know the send succeeded
        // (or was permanently rejected by GMS). post() runs synchronously via forceSync so we
        // can observe the outcome, and with enqueueOnFailure=false so failures don't append a
        // duplicate at the tail. If post() returns false the head remains in place and will be
        // retried after the next cooldown — no silent loss when DataHub is still down at probe
        // time. The circuit gateway in post() also ensures only one HALF_OPEN probe runs at a
        // time, so the replayer can't burst the buffer through a still-degraded GMS.
        // Replay using the buffered entry's ORIGINAL seq, not a fresh one. The seq is the
        // logical identity of the emit; a fresh seq here would (wrongly) be the largest in the
        // system and let the success callback purge legitimately-newer buffered same-URN
        // entries that arrived while this one was waiting.
        boolean handled = post(head.path(), head.body(), false, true, head.seq());
        if (handled) {
          synchronized (bufferLock) {
            // Only remove if the head is still our task; drop-oldest from a concurrent enqueue
            // may have already evicted it, in which case there's nothing to do here.
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
        LOG.warn("DataHub replayer encountered unexpected error", e);
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
   * Drop pending same-URN entries from the replay buffer whose sequence is OLDER than the
   * just-succeeded direct emit. Called from the 2xx success path of {@link #post}: those older
   * buffered entries reflect older state for the same URN and delivering them later would
   * overwrite the just-synced one. Newer-sequence same-URN entries are intentionally retained
   * — they reflect more recent state than the emit that's now succeeding and must still be
   * replayed.
   *
   * <p>Gating on {@code <} (strict less-than) keeps the buffered entry alive when the success
   * path is itself the replayer delivering that same entry (their seqs are equal).
   */
  private void purgeBufferedOlderSameUrn(Object body, long seq) {
    if (replayBuffer == null) return;
    String urn = urnFromBody(body);
    if (urn.isEmpty()) return;
    synchronized (bufferLock) {
      // Mark stale BEFORE removing. If the replayer has already peeked one of these entries,
      // removing it from the queue alone does not stop the impending sendAsync — but setting
      // stale=true lets the replayer's between-peek-and-send check skip the actual network
      // call. (For requests already dispatched to sendAsync, JDK HttpClient does not honor
      // cancellation; the in-flight window is a documented limitation — use synchronous-mode.)
      replayBuffer.removeIf(
          t -> {
            if (urn.equals(t.urn()) && t.seq() < seq) {
              t.stale().set(true);
              return true;
            }
            return false;
          });
    }
  }

  private static String urnFromBody(Object body) {
    // Body is the single-entry JSON array built by emitUpsert / emitStatusRemoved:
    //   [ { "urn": "...", aspectName: { "value": ... }, ... } ]
    // Walk to the first entry and read its "urn" field. Buffered replay tasks carry the same
    // body object so this extractor needs to keep matching it for purge/stale logic.
    if (body instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Map<?, ?> entry) {
      Object u = entry.get("urn");
      if (u instanceof String s) return s;
    }
    return "";
  }

  private record EmitTask(String path, String urn, long seq, AtomicBoolean stale, Object body) {}
}
