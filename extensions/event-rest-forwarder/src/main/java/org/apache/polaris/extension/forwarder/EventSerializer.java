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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;

/**
 * Converts Polaris event records into {@link EventEnvelope} instances ready to ship over HTTP.
 * Each {@code forX} method is a pure function over its event + {@link AuditContext} so it is
 * trivially unit-testable without spinning up CDI.
 *
 * <p>Naming convention: every method drops the {@code Event} suffix from the record name (so
 * {@code AfterCreateTableEvent} → {@link #forAfterCreateTable}). The same simple name appears in
 * the envelope's {@code eventType} field — the receiver dispatches on that string.
 *
 * <p>{@link TableMetadata} payloads are serialized via {@link TableMetadataParser#toJson} so the
 * receiver can round-trip them back with {@link TableMetadataParser#fromJson} without us having
 * to mirror Iceberg's evolving schema in our own DTOs. Where the source event does not carry
 * metadata (rename when post-load failed, drop, etc.) the field is left {@code null}.
 *
 * <p>Identity / ordering: each envelope is stamped at creation time with a fresh UUID
 * {@code eventId} (receiver-side idempotency key, stable across listener retries) and a
 * monotonically increasing {@code sequenceNumber} from a single per-serializer {@link
 * AtomicLong}. The sequence is process-local; receivers that need a global order across multiple
 * Polaris pods should compose it with {@code timestampMs} or a pod-identity tag from their proxy
 * layer.
 */
public class EventSerializer {

  /**
   * Per-listener-process counter. Starts at 0 and {@link AtomicLong#incrementAndGet} returns 1
   * for the first envelope, so {@code sequenceNumber >= 1} for every emitted envelope (0 is a
   * safe "unset" sentinel callers can rely on).
   */
  private final AtomicLong sequenceCounter;

  /**
   * EventId source. Override-able via the package-private constructor so tests can pin
   * deterministic IDs and assert ordering / dedup behavior without coupling to UUID randomness.
   */
  private final Supplier<String> eventIdSupplier;

  public EventSerializer() {
    this(new AtomicLong(), () -> UUID.randomUUID().toString());
  }

  /** Visible for tests — pin both the seq counter and the eventId supplier for determinism. */
  EventSerializer(AtomicLong sequenceCounter, Supplier<String> eventIdSupplier) {
    this.sequenceCounter = sequenceCounter;
    this.eventIdSupplier = eventIdSupplier;
  }

  // ============= Catalog events =============

  public EventEnvelope forAfterCreateCatalog(
      CatalogsServiceEvents.AfterCreateCatalogEvent event, AuditContext audit) {
    return catalogEnvelope("AfterCreateCatalog", event.catalog(), audit);
  }

  public EventEnvelope forAfterUpdateCatalog(
      CatalogsServiceEvents.AfterUpdateCatalogEvent event, AuditContext audit) {
    return catalogEnvelope("AfterUpdateCatalog", event.catalog(), audit);
  }

  public EventEnvelope forAfterDeleteCatalog(
      CatalogsServiceEvents.AfterDeleteCatalogEvent event, AuditContext audit) {
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterDeleteCatalog",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  // ============= Namespace events =============

  public EventEnvelope forAfterCreateNamespace(
      IcebergRestCatalogEvents.AfterCreateNamespaceEvent event, AuditContext audit) {
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterCreateNamespace",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        namespaceLevels(event.namespace()),
        null,
        null,
        event.namespaceProperties(),
        null,
        null,
        null,
        null);
  }

  public EventEnvelope forAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event, AuditContext audit) {
    // currentProperties is null when the delegator's post-load failed (or when the listener does
    // not implement RequiresPostUpdateNamespaceMetadata, but ours does). Carry null through so
    // the receiver can decide to leave existing state alone rather than overwriting it with
    // a half-known map. The REST response's updated/removed key lists are surfaced separately as
    // propertyUpdates so the receiver still has the diff even when the full state is unknown.
    UpdateNamespacePropertiesResponse resp = event.updateNamespacePropertiesResponse();
    EventEnvelope.PropertyUpdateSummary summary =
        resp == null
            ? null
            : new EventEnvelope.PropertyUpdateSummary(resp.updated(), resp.removed());
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterUpdateNamespaceProperties",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        namespaceLevels(event.namespace()),
        null,
        null,
        event.currentProperties(),
        null,
        null,
        null,
        summary);
  }

  public EventEnvelope forAfterDropNamespace(
      IcebergRestCatalogEvents.AfterDropNamespaceEvent event, AuditContext audit) {
    // AfterDropNamespaceEvent carries the namespace as a raw String (with U+001F separators
    // for nested levels) rather than as a parsed Namespace object. Preserve the raw form
    // verbatim — the receiver decides whether to split on U+001F itself. Setting namespace
    // (the parsed list) to null avoids guessing at separator semantics here.
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterDropNamespace",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        null,
        event.namespace(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  // ============= Table events =============

  public EventEnvelope forAfterCreateTable(
      IcebergRestCatalogEvents.AfterCreateTableEvent event, AuditContext audit) {
    return tableEnvelopeFromLoad(
        "AfterCreateTable",
        event.catalogName(),
        event.namespace(),
        event.tableName(),
        event.loadTableResponse(),
        audit);
  }

  public EventEnvelope forAfterRegisterTable(
      IcebergRestCatalogEvents.AfterRegisterTableEvent event, AuditContext audit) {
    return tableEnvelopeFromLoad(
        "AfterRegisterTable",
        event.catalogName(),
        event.namespace(),
        event.tableName(),
        event.loadTableResponse(),
        audit);
  }

  public EventEnvelope forAfterUpdateTable(
      IcebergRestCatalogEvents.AfterUpdateTableEvent event, AuditContext audit) {
    return tableEnvelopeFromLoad(
        "AfterUpdateTable",
        event.catalogName(),
        event.namespace(),
        event.sourceTable(),
        event.loadTableResponse(),
        audit);
  }

  public EventEnvelope forAfterCommitTable(
      IcebergRestCatalogEvents.AfterCommitTableEvent event, AuditContext audit) {
    TableIdentifier id = event.identifier();
    // commitTable uniquely carries the post-commit TableMetadata directly (no LoadTableResponse
    // wrapper). Forward metadata as canonical JSON when present; receivers needing the full
    // properties / schema / snapshot history get the same shape they would after a load.
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterCommitTable",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        namespaceLevels(id.namespace()),
        null,
        id.name(),
        null,
        tableMetadataJson(event.metadataAfter()),
        null,
        null,
        null);
  }

  public EventEnvelope forAfterRenameTable(
      IcebergRestCatalogEvents.AfterRenameTableEvent event, AuditContext audit) {
    RenameTableRequest req = event.renameTableRequest();
    TableIdentifier from = req.source();
    TableIdentifier to = req.destination();
    // Source identifier goes into namespace/table; the destination is surfaced via renameTo so
    // the receiver has both ends in a single envelope without having to look up state by URN.
    // tableMetadataJson reflects the DESTINATION (delegator's post-load result via the
    // RequiresPostRenameTableMetadata marker); it may be null when that follow-up load failed.
    LoadTableResponse loadResp = event.loadTableResponse();
    TableMetadata md = loadResp == null ? null : loadResp.tableMetadata();
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterRenameTable",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        namespaceLevels(from.namespace()),
        null,
        from.name(),
        null,
        tableMetadataJson(md),
        new EventEnvelope.RenameTarget(namespaceLevels(to.namespace()), to.name()),
        null,
        null);
  }

  public EventEnvelope forAfterDropTable(
      IcebergRestCatalogEvents.AfterDropTableEvent event, AuditContext audit) {
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        "AfterDropTable",
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        event.catalogName(),
        namespaceLevels(event.namespace()),
        null,
        event.table(),
        null,
        null,
        null,
        event.purgeRequested(),
        null);
  }

  // ============= helpers =============

  private EventEnvelope catalogEnvelope(String eventType, Catalog catalog, AuditContext audit) {
    String name = catalog == null ? null : catalog.getName();
    Map<String, String> properties =
        catalog == null || catalog.getProperties() == null
            ? null
            : catalog.getProperties().toMap();
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        eventType,
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        name,
        null,
        null,
        null,
        properties,
        null,
        null,
        null,
        null);
  }

  private EventEnvelope tableEnvelopeFromLoad(
      String eventType,
      String catalogName,
      Namespace namespace,
      String tableName,
      LoadTableResponse resp,
      AuditContext audit) {
    TableMetadata md = resp == null ? null : resp.tableMetadata();
    return new EventEnvelope(
        nextEventId(),
        nextSequence(),
        eventType,
        audit.timestampMs(),
        audit.principal(),
        audit.realmId(),
        catalogName,
        namespaceLevels(namespace),
        null,
        tableName,
        null,
        tableMetadataJson(md),
        null,
        null,
        null);
  }

  private long nextSequence() {
    return sequenceCounter.incrementAndGet();
  }

  private String nextEventId() {
    return eventIdSupplier.get();
  }

  private static List<String> namespaceLevels(Namespace namespace) {
    if (namespace == null || namespace.isEmpty()) return null;
    return Arrays.asList(namespace.levels());
  }

  private static String tableMetadataJson(TableMetadata md) {
    return md == null ? null : TableMetadataParser.toJson(md);
  }
}
