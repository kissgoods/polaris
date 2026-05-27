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

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.extension.forwarder.http.HttpEventPoster;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.RequiresPostRenameTableMetadata;
import org.apache.polaris.service.events.listeners.RequiresPostUpdateNamespaceMetadata;
import org.apache.polaris.service.events.listeners.TransactionalCommitTableDeferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates the subset of {@link PolarisEventListener} hooks the forwarder cares about (catalog
 * / namespace / table CRUD) into POSTs of {@link EventEnvelope}s through a {@link
 * HttpEventPoster}. Concrete subclasses inject the poster + serializer; the bundled
 * implementation is HTTP-based ({@code RestForwarderEventListener}).
 *
 * <p>This listener implements both {@link RequiresPostRenameTableMetadata} and {@link
 * RequiresPostUpdateNamespaceMetadata} so the runtime delegator performs the follow-up
 * {@code loadTable} / {@code loadNamespaceMetadata} after rename and updateProperties (the REST
 * responses for those two operations do not carry enough state on their own). The marker check
 * inside the delegator means listeners that don't need this metadata (no-op, persistence buffer,
 * aws-cloudwatch) pay zero cost for the additional load.
 *
 * <p>All emit failures are caught at the poster layer; this listener never throws back into
 * Polaris so that receiver outages cannot break catalog operations.
 */
public abstract class AbstractEventForwarderListener
    implements PolarisEventListener,
        RequiresPostRenameTableMetadata,
        RequiresPostUpdateNamespaceMetadata,
        TransactionalCommitTableDeferred {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractEventForwarderListener.class);

  protected final HttpEventPoster poster;
  protected final EventSerializer serializer;

  /**
   * Per-thread buffer for commit-table envelopes that the runtime delegator told us are part of a
   * still-in-flight {@code commitTransaction}. Set to non-null between {@link #beginTransaction()}
   * and {@link #endTransaction(boolean)}; while non-null, {@link #onAfterCommitTable} appends
   * rather than posting. On {@code endTransaction(true)} the buffer is drained to the poster; on
   * {@code endTransaction(false)} it is discarded so a rolled-back transaction never reaches the
   * receiver. ThreadLocal because each catalog request runs on a single JAX-RS worker thread, and
   * because the marker handshake (begin / per-table commits / end) lives entirely on that thread.
   *
   * <p>Declared {@code static} to satisfy ErrorProne's {@code ThreadLocalUsage} check: the
   * listener is a CDI {@code @ApplicationScoped} singleton, so there is one logical instance for
   * the lifetime of the JVM and the per-thread semantics we want are unaffected by whether the
   * ThreadLocal field is instance- or class-scoped. Multiple proxies share the same per-thread
   * view, which is the intended behavior.
   */
  private static final ThreadLocal<List<EventEnvelope>> TRANSACTION_BUFFER = new ThreadLocal<>();

  protected AbstractEventForwarderListener(HttpEventPoster poster, EventSerializer serializer) {
    this.poster = poster;
    this.serializer = serializer;
  }

  /**
   * No-arg constructor required by Quarkus/Arc to generate a client proxy for
   * {@code @ApplicationScoped} subclasses. Never invoked for live beans — those go through the
   * {@code @Inject} constructors on the concrete subclasses.
   */
  protected AbstractEventForwarderListener() {
    this.poster = null;
    this.serializer = null;
  }

  /**
   * Per-emit audit context (who triggered the operation, when). The default returns
   * {@code (System.currentTimeMillis(), null)} so test subclasses and the abstract base remain
   * usable without request-scoped JAX-RS plumbing. The HTTP listener overrides this to read the
   * caller principal from {@code SecurityContext}.
   */
  protected AuditContext getAuditContext() {
    return new AuditContext(System.currentTimeMillis(), null, null);
  }

  @PreDestroy
  void shutdown() {
    if (poster == null) return;
    try {
      poster.close();
    } catch (Exception e) {
      LOG.warn("Error closing rest-forwarder HTTP poster", e);
    }
  }

  // ============= Catalog events =============

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    poster.post(serializer.forAfterCreateCatalog(event, getAuditContext()));
  }

  @Override
  public void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {
    poster.post(serializer.forAfterUpdateCatalog(event, getAuditContext()));
  }

  @Override
  public void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {
    poster.post(serializer.forAfterDeleteCatalog(event, getAuditContext()));
  }

  // ============= Namespace events =============

  @Override
  public void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {
    poster.post(serializer.forAfterCreateNamespace(event, getAuditContext()));
  }

  @Override
  public void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {
    // currentProperties is supplied by the delegator's post-load (RequiresPostUpdateNamespaceMetadata
    // marker). When that load fails, it is null and the serializer surfaces it to the receiver as
    // null so the receiver can treat the event as "diff-only" using the propertyUpdates summary
    // rather than overwriting state with a partial map.
    poster.post(serializer.forAfterUpdateNamespaceProperties(event, getAuditContext()));
  }

  @Override
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {
    poster.post(serializer.forAfterDropNamespace(event, getAuditContext()));
  }

  // ============= Table events =============

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    poster.post(serializer.forAfterCreateTable(event, getAuditContext()));
  }

  @Override
  public void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {
    poster.post(serializer.forAfterRegisterTable(event, getAuditContext()));
  }

  @Override
  public void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {
    poster.post(serializer.forAfterUpdateTable(event, getAuditContext()));
  }

  @Override
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {
    EventEnvelope envelope = serializer.forAfterCommitTable(event, getAuditContext());
    List<EventEnvelope> buffer = TRANSACTION_BUFFER.get();
    if (buffer != null) {
      // Inside a commitTransaction window — the runtime fires this BEFORE the final atomic
      // metastore update, so the table commit may still be rolled back. Hold the envelope
      // until endTransaction(true) signals durability; discard on endTransaction(false).
      buffer.add(envelope);
    } else {
      poster.post(envelope);
    }
  }

  @Override
  public void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {
    // loadTableResponse is supplied by the delegator's post-load (RequiresPostRenameTableMetadata
    // marker). When that load fails, the serializer leaves tableMetadataJson null — the rename
    // event itself still ships (source + renameTo) so the receiver knows the table moved.
    poster.post(serializer.forAfterRenameTable(event, getAuditContext()));
  }

  @Override
  public void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {
    poster.post(serializer.forAfterDropTable(event, getAuditContext()));
  }

  // onAfterCommitTransaction is intentionally NOT overridden. Multi-table atomic commits surface
  // BOTH onAfterCommitTable (per table, with full TableMetadata) and onAfterCommitTransaction
  // (without metadata). The per-table handler above already forwards each table's commit; emitting
  // again from commitTransaction with no metadata would duplicate the event without adding info,
  // and confuse receivers that key off (catalog, table) -> latest metadata. The
  // PolarisEventListener default no-op is exactly what we want here.
  //
  // Per-transaction durability flush is handled by the TransactionalCommitTableDeferred handshake
  // below: the runtime delegator calls beginTransaction → delegate.commitTransaction (which fires
  // onAfterCommitTable per table; we buffer those) → endTransaction(committed). On a successful
  // commit we drain the buffer; on a thrown CommitFailedException we discard, so the receiver
  // never sees state for a transaction Polaris rolled back.

  // ============= TransactionalCommitTableDeferred =============

  @Override
  public void beginTransaction() {
    // Stack-discipline guard: nested commitTransaction should never happen at the catalog
    // handler layer, but if the runtime ever changes to allow it, replacing a non-null buffer
    // would silently drop the outer transaction's events. Fail loud instead.
    if (TRANSACTION_BUFFER.get() != null) {
      LOG.warn(
          "rest-forwarder: beginTransaction called with an existing buffer in flight ({} entries"
              + " would be lost) — clearing. This indicates a runtime ordering bug.",
          TRANSACTION_BUFFER.get().size());
    }
    TRANSACTION_BUFFER.set(new ArrayList<>());
  }

  @Override
  public void endTransaction(boolean committed) {
    List<EventEnvelope> buffer = TRANSACTION_BUFFER.get();
    // Always clear the ThreadLocal so a leak from a pooled JAX-RS thread can't poison the next
    // request that lands on it.
    TRANSACTION_BUFFER.remove();
    if (buffer == null) {
      // beginTransaction was never called — listener state is inconsistent; bail safely.
      return;
    }
    if (!committed) {
      // Transaction rolled back. Discard the buffered envelopes — Polaris did not persist them
      // and the receiver must not see them.
      return;
    }
    // Atomic commit succeeded: release the buffered envelopes in the order they were captured.
    // Each is posted independently so the poster's 5 isolation guards (circuit breaker, replay
    // buffer, etc.) still apply per envelope.
    for (EventEnvelope env : buffer) {
      poster.post(env);
    }
  }
}
