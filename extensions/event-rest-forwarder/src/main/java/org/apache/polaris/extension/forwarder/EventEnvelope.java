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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;
import java.util.Map;

/**
 * Wire-format DTO POSTed to the receiver. One envelope per Polaris event. {@code null}-valued
 * fields are omitted from the JSON so the payload stays compact and the receiver can use field
 * presence as a discriminator (e.g. {@code table == null} → namespace event).
 *
 * <p>Field meanings:
 *
 * <ul>
 *   <li>{@code eventId} — opaque UUID minted once when the envelope is created. Stable across the
 *       listener's own retries (replay buffer redelivery, sync-mode retry-on-timeout), so the
 *       receiver can use it as an idempotency key to deduplicate timeout-after-commit double
 *       deliveries.
 *   <li>{@code sequenceNumber} — monotonically increasing per-listener-process counter assigned
 *       at envelope creation time. Receivers can use it to detect out-of-order arrivals (a
 *       lower-seq event arriving after a higher-seq event for the same resource is stale and
 *       should be ignored / merged carefully). The listener also uses this internally to suppress
 *       buffered older-seq events for the same resource when a newer direct emit succeeds.
 *   <li>{@code eventType} — Polaris event record's simple class name (e.g. {@code
 *       AfterCreateTable}). Receiver dispatches on this.
 *   <li>{@code timestampMs} — wall-clock instant the listener handler observed the event.
 *   <li>{@code actor} — caller principal name from JAX-RS {@code SecurityContext}; {@code null}
 *       for system-driven events.
 *   <li>{@code realm} — Polaris realm identifier the request was routed to (from {@code
 *       RealmContext.getRealmIdentifier()}); {@code null} in single-realm deployments or when
 *       the listener runs outside a request scope. Same-name catalogs in different realms must
 *       not collide downstream — the realm is included in {@link #resourceKey()}.
 *   <li>{@code catalog} — catalog name.
 *   <li>{@code namespace} — levels list for table/namespace events; {@code null} for catalog-level
 *       events. (Use {@code namespaceRaw} for the raw U+001F-joined form when the source event
 *       carries it.)
 *   <li>{@code namespaceRaw} — only set by {@code AfterDropNamespace}, which carries the namespace
 *       as a raw U+001F-separated string. Preserved verbatim so the receiver can decide whether to
 *       split it.
 *   <li>{@code table} — table name for table events; {@code null} otherwise.
 *   <li>{@code properties} — user-set properties when the event carries them (create namespace,
 *       updateNamespaceProperties post-loaded result, catalog properties, drop-table purge flag).
 *       {@code null} when the source could not determine them — receiver should treat as "leave
 *       existing state alone" rather than "set to empty".
 *   <li>{@code tableMetadataJson} — Iceberg canonical JSON serialization of the {@link
 *       org.apache.iceberg.TableMetadata} when the event ships one (create / register / update /
 *       commit / rename). Produced via {@code TableMetadataParser.toJson} so the receiver can
 *       round-trip via {@code TableMetadataParser.fromJson}.
 *   <li>{@code renameTo} — destination identifier for {@code AfterRenameTable}.
 *   <li>{@code purgeRequested} — set by {@code AfterDropTable} when the drop request included
 *       {@code purge=true}.
 *   <li>{@code propertyUpdates} — set by {@code AfterUpdateNamespaceProperties} to surface the
 *       updated/removed key lists from the original REST response (in addition to the
 *       post-loaded full {@code properties}).
 * </ul>
 */
@JsonInclude(Include.NON_NULL)
public record EventEnvelope(
    String eventId,
    long sequenceNumber,
    String eventType,
    long timestampMs,
    String actor,
    String realm,
    String catalog,
    List<String> namespace,
    String namespaceRaw,
    String table,
    Map<String, String> properties,
    String tableMetadataJson,
    RenameTarget renameTo,
    Boolean purgeRequested,
    PropertyUpdateSummary propertyUpdates) {

  /** Destination identifier of a rename event. */
  @JsonInclude(Include.NON_NULL)
  public record RenameTarget(List<String> namespace, String name) {}

  /**
   * Summary of the keys touched by an {@code updateNamespaceProperties} REST call. Surfaces the
   * REST response shape verbatim (the response only carries which keys changed, not the resulting
   * state — see {@link #properties} for the post-loaded full state).
   */
  @JsonInclude(Include.NON_NULL)
  public record PropertyUpdateSummary(List<String> updated, List<String> removed) {}

  /**
   * Source-side identity key for the resource this envelope's main subject (the {@code
   * namespace} / {@code table} fields, not {@code renameTo}). Always present. Prefer {@link
   * #resourceKeys()} for purge / dedup logic because that variant also surfaces the destination
   * key for rename envelopes — a {@code resourceKey()}-only check misses the
   * old-rename-vs-newer-commit-on-destination race that Codex flagged.
   *
   * <p>Composition (joined by U+001F, the same separator Iceberg uses for nested namespaces):
   * {@code catalog  <namespace><table>}. For {@code AfterDropNamespace}, the raw
   * namespace string is used verbatim (it already carries U+001F separators).
   *
   * <p>Returns {@code ""} when no catalog is set (currently never the case for the 12 events the
   * forwarder handles, but defensive against future event additions).
   */
  public String resourceKey() {
    return composeKey(realm, catalog, namespace, namespaceRaw, table);
  }

  /**
   * All resources this envelope is logically about — usually one key, but a rename envelope
   * touches BOTH the source identity ({@code namespace} / {@code table}) and the destination
   * identity ({@code renameTo}). The replay-buffer's same-resource purge must consider every key
   * so a stale buffered rename {@code old -> new} is correctly evicted when a newer direct commit
   * lands on {@code new} (the source key alone would not match).
   *
   * <p>Composition: each entry is the U+001F-joined triple {@code catalog \u001f <ns> \u001f table},
   * mirroring Iceberg's nested-namespace separator. For {@code AfterDropNamespace} the raw
   * namespace string is reused verbatim (it already carries U+001F separators).
   *
   * <p>The returned set preserves insertion order (source first, then destination) so the receiver
   * can rely on a stable iteration order if it uses these keys directly as ordering hints.
   */
  public java.util.Set<String> resourceKeys() {
    java.util.LinkedHashSet<String> keys = new java.util.LinkedHashSet<>(2);
    keys.add(resourceKey());
    if (renameTo != null) {
      keys.add(composeKey(realm, catalog, renameTo.namespace(), null, renameTo.name()));
    }
    return keys;
  }

  /** Shared key builder used by {@link #resourceKey()} and {@link #resourceKeys()}. */
  private static String composeKey(
      String realm,
      String catalog,
      List<String> namespace,
      String namespaceRaw,
      String table) {
    StringBuilder sb = new StringBuilder();
    // Realm prefix prevents collision between same-named catalogs / tables in different Polaris
    // realms. null realm (single-realm deployments) collapses to an empty prefix so existing
    // single-realm key shapes stay stable.
    sb.append(realm == null ? "" : realm);
    sb.append('\u001f');
    sb.append(catalog == null ? "" : catalog);
    sb.append('\u001f');
    if (namespace != null) {
      for (int i = 0; i < namespace.size(); i++) {
        if (i > 0) sb.append('\u001f');
        sb.append(namespace.get(i));
      }
    } else if (namespaceRaw != null) {
      // Already U+001F-separated; emit verbatim so AfterDropNamespace ends up with the same key
      // as the matching AfterCreateNamespace that built its namespace list from levels().
      sb.append(namespaceRaw);
    }
    sb.append('\u001f');
    if (table != null) sb.append(table);
    return sb.toString();
  }
}
