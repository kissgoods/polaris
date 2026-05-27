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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link EventSerializer} produces the expected wire envelope for every event the
 * forwarder cares about. Each test focuses on a single envelope field group so failures localize
 * to a single property at a time.
 */
class EventSerializerTest {

  private final EventSerializer serializer = new EventSerializer();
  private final AuditContext audit =
      new AuditContext(1_715_760_000_000L, "alice@example.com", "test-realm");

  // ============= Catalog events =============

  @Test
  void createCatalogCarriesNamePropertiesAndAudit() {
    Catalog catalog = sampleCatalog("cat", Map.of("default-base-location", "s3://bucket/cat"));
    EventEnvelope env =
        serializer.forAfterCreateCatalog(
            new CatalogsServiceEvents.AfterCreateCatalogEvent(catalog), audit);

    assertThat(env.eventType()).isEqualTo("AfterCreateCatalog");
    assertThat(env.timestampMs()).isEqualTo(1_715_760_000_000L);
    assertThat(env.actor()).isEqualTo("alice@example.com");
    assertThat(env.catalog()).isEqualTo("cat");
    assertThat(env.namespace()).isNull();
    assertThat(env.table()).isNull();
    assertThat(env.properties()).containsEntry("default-base-location", "s3://bucket/cat");
    assertThat(env.tableMetadataJson()).isNull();
    assertThat(env.renameTo()).isNull();
  }

  @Test
  void updateCatalogUsesUpdateEventType() {
    Catalog catalog = sampleCatalog("cat", Map.of("k", "v"));
    EventEnvelope env =
        serializer.forAfterUpdateCatalog(
            new CatalogsServiceEvents.AfterUpdateCatalogEvent(catalog), audit);
    assertThat(env.eventType()).isEqualTo("AfterUpdateCatalog");
    assertThat(env.catalog()).isEqualTo("cat");
    assertThat(env.properties()).containsEntry("k", "v");
  }

  @Test
  void deleteCatalogCarriesOnlyCatalogName() {
    EventEnvelope env =
        serializer.forAfterDeleteCatalog(
            new CatalogsServiceEvents.AfterDeleteCatalogEvent("cat"), audit);
    assertThat(env.eventType()).isEqualTo("AfterDeleteCatalog");
    assertThat(env.catalog()).isEqualTo("cat");
    assertThat(env.namespace()).isNull();
    assertThat(env.table()).isNull();
    assertThat(env.properties()).isNull();
  }

  // ============= Namespace events =============

  @Test
  void createNamespaceCarriesLevelListAndProperties() {
    EventEnvelope env =
        serializer.forAfterCreateNamespace(
            new IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
                "cat", Namespace.of("ns", "sub"), Map.of("owner", "team-x")),
            audit);
    assertThat(env.eventType()).isEqualTo("AfterCreateNamespace");
    assertThat(env.namespace()).containsExactly("ns", "sub");
    assertThat(env.namespaceRaw()).isNull();
    assertThat(env.properties()).containsEntry("owner", "team-x");
  }

  @Test
  void updateNamespacePropertiesSurfacesUpdatedAndRemovedSummaryAlongsideFullState() {
    UpdateNamespacePropertiesResponse resp =
        UpdateNamespacePropertiesResponse.builder()
            .addUpdated("k1")
            .addUpdated("k2")
            .addRemoved("old")
            .build();
    EventEnvelope env =
        serializer.forAfterUpdateNamespaceProperties(
            new IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent(
                "cat", Namespace.of("ns"), resp, Map.of("k1", "v1", "k2", "v2")),
            audit);

    assertThat(env.eventType()).isEqualTo("AfterUpdateNamespaceProperties");
    assertThat(env.properties()).containsOnly(Map.entry("k1", "v1"), Map.entry("k2", "v2"));
    assertThat(env.propertyUpdates()).isNotNull();
    assertThat(env.propertyUpdates().updated()).containsExactly("k1", "k2");
    assertThat(env.propertyUpdates().removed()).containsExactly("old");
  }

  @Test
  void updateNamespacePropertiesNullCurrentPropertiesIsForwardedAsNull() {
    // currentProperties == null is the delegator's signal that the post-load failed. The
    // serializer must surface null (not an empty map) so the receiver can distinguish
    // "couldn't load" from "actually empty".
    UpdateNamespacePropertiesResponse resp = UpdateNamespacePropertiesResponse.builder().build();
    EventEnvelope env =
        serializer.forAfterUpdateNamespaceProperties(
            new IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent(
                "cat", Namespace.of("ns"), resp, /*currentProperties*/ null),
            audit);
    assertThat(env.properties()).isNull();
  }

  @Test
  void dropNamespacePreservesRawSeparatorString() {
    // AfterDropNamespaceEvent carries the namespace as a raw String containing U+001F
    // separators for nested levels. The serializer must NOT split that — the receiver decides.
    String raw = "nssub";
    EventEnvelope env =
        serializer.forAfterDropNamespace(
            new IcebergRestCatalogEvents.AfterDropNamespaceEvent("cat", raw), audit);

    assertThat(env.eventType()).isEqualTo("AfterDropNamespace");
    assertThat(env.namespaceRaw()).isEqualTo(raw);
    assertThat(env.namespace())
        .as("parsed namespace list must remain null — only namespaceRaw carries the value")
        .isNull();
  }

  // ============= Table events =============

  @Test
  void createTableSerializesTableMetadataAsCanonicalIcebergJson() {
    TableMetadata md = sampleMetadata();
    EventEnvelope env =
        serializer.forAfterCreateTable(
            new IcebergRestCatalogEvents.AfterCreateTableEvent(
                "cat", Namespace.of("ns"), "tbl", loadResponseFor(md)),
            audit);

    assertThat(env.eventType()).isEqualTo("AfterCreateTable");
    assertThat(env.namespace()).containsExactly("ns");
    assertThat(env.table()).isEqualTo("tbl");
    assertThat(env.tableMetadataJson()).isNotNull();
    // Round-trip via Iceberg's parser to confirm we shipped canonical JSON (not a Map/object).
    TableMetadata roundTripped = TableMetadataParser.fromJson(env.tableMetadataJson());
    assertThat(roundTripped.uuid()).isEqualTo(md.uuid());
    assertThat(roundTripped.location()).isEqualTo(md.location());
    assertThat(roundTripped.schema().columns()).hasSize(md.schema().columns().size());
  }

  @Test
  void registerTableUsesRegisterEventType() {
    EventEnvelope env =
        serializer.forAfterRegisterTable(
            new IcebergRestCatalogEvents.AfterRegisterTableEvent(
                "cat", Namespace.of("ns"), "tbl", loadResponseFor(sampleMetadata())),
            audit);
    assertThat(env.eventType()).isEqualTo("AfterRegisterTable");
    assertThat(env.tableMetadataJson()).isNotNull();
  }

  @Test
  void updateTableUsesUpdateEventTypeAndSourceTableName() {
    EventEnvelope env =
        serializer.forAfterUpdateTable(
            new IcebergRestCatalogEvents.AfterUpdateTableEvent(
                "cat", Namespace.of("ns"), "tbl", null, loadResponseFor(sampleMetadata())),
            audit);
    assertThat(env.eventType()).isEqualTo("AfterUpdateTable");
    assertThat(env.table()).isEqualTo("tbl");
  }

  @Test
  void commitTableShipsPostCommitMetadataDirectly() {
    TableMetadata md = sampleMetadata();
    EventEnvelope env =
        serializer.forAfterCommitTable(
            new IcebergRestCatalogEvents.AfterCommitTableEvent(
                "cat", TableIdentifier.of("ns", "tbl"), null, md),
            audit);
    assertThat(env.eventType()).isEqualTo("AfterCommitTable");
    assertThat(env.namespace()).containsExactly("ns");
    assertThat(env.table()).isEqualTo("tbl");
    assertThat(env.tableMetadataJson()).isNotNull();
  }

  @Test
  void commitTableWithNullMetadataLeavesTableMetadataJsonNull() {
    // commitTable for a non-Iceberg table or after a parsing failure carries null metadata.
    // The serializer still emits the envelope (so the receiver sees the commit signal) but
    // leaves tableMetadataJson absent so the receiver doesn't overwrite known state.
    EventEnvelope env =
        serializer.forAfterCommitTable(
            new IcebergRestCatalogEvents.AfterCommitTableEvent(
                "cat", TableIdentifier.of("ns", "tbl"), null, /*metadataAfter*/ null),
            audit);
    assertThat(env.tableMetadataJson()).isNull();
    assertThat(env.table()).isEqualTo("tbl");
  }

  @Test
  void renameTableCarriesSourceAndDestinationPlusPostLoadMetadata() {
    TableMetadata md = sampleMetadata();
    RenameTableRequest req =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of("ns", "old"))
            .withDestination(TableIdentifier.of(Namespace.of("ns", "sub"), "new"))
            .build();
    EventEnvelope env =
        serializer.forAfterRenameTable(
            new IcebergRestCatalogEvents.AfterRenameTableEvent("cat", req, loadResponseFor(md)),
            audit);

    assertThat(env.eventType()).isEqualTo("AfterRenameTable");
    assertThat(env.namespace()).containsExactly("ns");
    assertThat(env.table()).isEqualTo("old");
    assertThat(env.renameTo()).isNotNull();
    assertThat(env.renameTo().namespace()).containsExactly("ns", "sub");
    assertThat(env.renameTo().name()).isEqualTo("new");
    assertThat(env.tableMetadataJson()).isNotNull();
  }

  @Test
  void renameTableWithNullLoadResponseStillEmitsRenameWithoutMetadata() {
    // Post-rename loadTable failures should not silence the rename signal — the receiver still
    // needs to know the table moved. tableMetadataJson is null in that case.
    RenameTableRequest req =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of("ns", "old"))
            .withDestination(TableIdentifier.of("ns", "new"))
            .build();
    EventEnvelope env =
        serializer.forAfterRenameTable(
            new IcebergRestCatalogEvents.AfterRenameTableEvent(
                "cat", req, /*loadTableResponse*/ null),
            audit);
    assertThat(env.tableMetadataJson()).isNull();
    assertThat(env.renameTo().name()).isEqualTo("new");
  }

  @Test
  void dropTableCarriesPurgeFlag() {
    EventEnvelope env =
        serializer.forAfterDropTable(
            new IcebergRestCatalogEvents.AfterDropTableEvent(
                "cat", Namespace.of("ns"), "tbl", /*purgeRequested*/ true),
            audit);
    assertThat(env.eventType()).isEqualTo("AfterDropTable");
    assertThat(env.table()).isEqualTo("tbl");
    assertThat(env.purgeRequested()).isTrue();
    assertThat(env.tableMetadataJson()).isNull();
  }

  // ============= Wire shape =============

  // ============= Identity / ordering (Finding #2 regression) =============

  @Test
  void eachEnvelopeGetsAFreshUuidEventIdAndMonotonicSequence() {
    EventEnvelope a =
        serializer.forAfterDeleteCatalog(
            new CatalogsServiceEvents.AfterDeleteCatalogEvent("cat"), audit);
    EventEnvelope b =
        serializer.forAfterDeleteCatalog(
            new CatalogsServiceEvents.AfterDeleteCatalogEvent("cat"), audit);

    assertThat(a.eventId()).isNotNull().isNotEmpty();
    assertThat(b.eventId()).isNotNull().isNotEmpty();
    assertThat(a.eventId())
        .as("each envelope must mint a fresh eventId — receiver dedup key")
        .isNotEqualTo(b.eventId());

    assertThat(a.sequenceNumber()).isPositive();
    assertThat(b.sequenceNumber()).isGreaterThan(a.sequenceNumber());
  }

  @Test
  void resourceKeyMatchesBetweenCreateAndDropOfSameNestedNamespace() {
    // The same logical resource (nested namespace `a.b`) must produce the same resourceKey
    // regardless of which event surfaces it. AfterCreateNamespace gets a Namespace object;
    // AfterDropNamespace gets a raw U+001F-separated string. Both must end up keyed identically
    // so the HttpEventPoster's purge can recognize them as same-resource.
    EventEnvelope create =
        serializer.forAfterCreateNamespace(
            new org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
                "cat",
                org.apache.iceberg.catalog.Namespace.of("a", "b"),
                java.util.Map.of()),
            audit);
    EventEnvelope drop =
        serializer.forAfterDropNamespace(
            new org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropNamespaceEvent(
                "cat", "a\u001fb"), // Iceberg's nested-namespace separator
            audit);
    assertThat(create.resourceKey()).isEqualTo(drop.resourceKey());
  }

  @Test
  void resourceKeyDiffersForDifferentTablesOnSameNamespace() {
    EventEnvelope tA =
        serializer.forAfterDropTable(
            new org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropTableEvent(
                "cat", org.apache.iceberg.catalog.Namespace.of("ns"), "tableA", false),
            audit);
    EventEnvelope tB =
        serializer.forAfterDropTable(
            new org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropTableEvent(
                "cat", org.apache.iceberg.catalog.Namespace.of("ns"), "tableB", false),
            audit);
    assertThat(tA.resourceKey()).isNotEqualTo(tB.resourceKey());
  }

  @Test
  void renameEnvelopeResourceKeysIncludesBothSourceAndDestination() {
    // Adversarial review #2: a rename envelope must surface BOTH the source and the destination
    // identities so the replay-buffer purge can evict the buffered rename when a newer commit
    // lands on either side. resourceKey() alone only returns source — the dual-key view lives
    // on resourceKeys().
    RenameTableRequest req =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of("ns", "old"))
            .withDestination(TableIdentifier.of(Namespace.of("ns", "sub"), "new"))
            .build();
    EventEnvelope env =
        serializer.forAfterRenameTable(
            new IcebergRestCatalogEvents.AfterRenameTableEvent("cat", req, null), audit);

    java.util.Set<String> keys = env.resourceKeys();
    assertThat(keys).hasSize(2);

    // Build the matching commit envelope and confirm the rename's destination key equals it.
    EventEnvelope sameDestinationCommit =
        serializer.forAfterCommitTable(
            new IcebergRestCatalogEvents.AfterCommitTableEvent(
                "cat",
                TableIdentifier.of(Namespace.of("ns", "sub"), "new"),
                null,
                sampleMetadata()),
            audit);
    assertThat(keys)
        .as("rename's destination key must equal a direct commit's resourceKey for the same target")
        .contains(sameDestinationCommit.resourceKey());
    assertThat(keys).contains(env.resourceKey());
  }

  @Test
  void sameCatalogTableInDifferentRealmsProduceDifferentResourceKeys() {
    // Adversarial review #3 regression. The wire envelope carries `realm` and the resourceKey
    // prepends it, so a catalog/table pair that happens to share names across two Polaris realms
    // does NOT collapse to the same downstream key — the receiver-side dedup / ordering can stay
    // tenant-correct without any extra cooperation.
    AuditContext realmA = new AuditContext(1L, "alice", "realm-A");
    AuditContext realmB = new AuditContext(1L, "alice", "realm-B");
    EventSerializer serializerA = new EventSerializer();
    EventSerializer serializerB = new EventSerializer();

    EventEnvelope envA =
        serializerA.forAfterDropTable(
            new IcebergRestCatalogEvents.AfterDropTableEvent(
                "shared-cat", Namespace.of("ns"), "shared-tbl", false),
            realmA);
    EventEnvelope envB =
        serializerB.forAfterDropTable(
            new IcebergRestCatalogEvents.AfterDropTableEvent(
                "shared-cat", Namespace.of("ns"), "shared-tbl", false),
            realmB);

    assertThat(envA.realm()).isEqualTo("realm-A");
    assertThat(envB.realm()).isEqualTo("realm-B");
    assertThat(envA.resourceKey())
        .as("realm prefix must distinguish identical (catalog, namespace, table) across realms")
        .isNotEqualTo(envB.resourceKey());
  }

  @Test
  void nullRealmKeepsLegacySingleRealmKeyShape() {
    // Single-realm deployments (realmId == null) collapse the realm segment to an empty prefix,
    // so the resourceKey starts with U+001F and matches the pre-realm key shape exactly. This
    // keeps the migration backward-compatible: existing single-realm receivers that compare
    // resource keys to a stored copy don't see them change after upgrade.
    AuditContext noRealm = new AuditContext(1L, "alice", null);
    EventEnvelope env =
        serializer.forAfterDropTable(
            new IcebergRestCatalogEvents.AfterDropTableEvent(
                "cat", Namespace.of("ns"), "tbl", false),
            noRealm);
    assertThat(env.realm()).isNull();
    // Build the expected prefix at runtime to avoid embedding the U+001F separator as a literal
    // in this source file (some tooling strips non-printable bytes from .java sources during
    // round-trips). Same chars Iceberg uses for nested namespaces.
    String sep = "\u001f";
    assertThat(env.resourceKey())
        .as("null realm collapses to empty leading segment; rest of key matches pre-realm shape")
        .startsWith(sep + "cat" + sep + "ns" + sep + "tbl");
  }

  @Test
  void nonRenameEnvelopeResourceKeysIsSingleSourceEntry() {
    EventEnvelope env =
        serializer.forAfterCommitTable(
            new IcebergRestCatalogEvents.AfterCommitTableEvent(
                "cat", TableIdentifier.of("ns", "tbl"), null, sampleMetadata()),
            audit);
    assertThat(env.resourceKeys()).containsExactly(env.resourceKey());
  }

  // ============= Wire shape =============

  @Test
  void jsonOmitsNullFieldsForCompactWireFormat() throws Exception {
    // NON_NULL inclusion is a wire contract — the receiver uses presence to distinguish, e.g.,
    // "no namespace this event" vs "namespace explicitly empty". Lock it in via a serialization
    // round-trip rather than just asserting the annotation.
    EventEnvelope env =
        serializer.forAfterDeleteCatalog(
            new CatalogsServiceEvents.AfterDeleteCatalogEvent("cat"), audit);
    String json = new ObjectMapper().writeValueAsString(env);
    assertThat(json)
        .doesNotContain("\"namespace\"")
        .doesNotContain("\"table\"")
        .doesNotContain("\"properties\"")
        .doesNotContain("\"tableMetadataJson\"")
        .doesNotContain("\"renameTo\"")
        .contains("\"eventType\":\"AfterDeleteCatalog\"")
        .contains("\"catalog\":\"cat\"")
        // eventId + sequenceNumber are always present — receiver-side dedup + ordering keys.
        .contains("\"eventId\":")
        .contains("\"sequenceNumber\":");
  }

  // ============= helpers =============

  /** Sample table metadata with mixed primitive + nested struct fields. */
  private TableMetadata sampleMetadata() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get(), "primary key"),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        "s3://bucket/cat/ns/tbl",
        Map.of("owner", "team-x", "write.parquet.compression-codec", "zstd"));
  }

  private LoadTableResponse loadResponseFor(TableMetadata md) {
    return LoadTableResponse.builder().withTableMetadata(md).build();
  }

  /** Build a minimal Catalog admin-model with the given name + properties. */
  private Catalog sampleCatalog(String name, Map<String, String> properties) {
    // PolarisCatalog (vs ExternalCatalog) avoids needing a remote URL; the serializer only cares
    // about getName() / getProperties() so any concrete subtype works.
    CatalogProperties props =
        CatalogProperties.builder(
                properties.getOrDefault("default-base-location", "s3://bucket/" + name))
            .putAll(properties)
            .build();
    return new PolarisCatalog(
        Catalog.TypeEnum.INTERNAL,
        name,
        props,
        new FileStorageConfigInfo(
            FileStorageConfigInfo.StorageTypeEnum.FILE, List.of("s3://bucket/")));
  }
}
