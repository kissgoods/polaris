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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
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
import org.apache.polaris.extension.forwarder.http.HttpEventPoster;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

/**
 * Behavioural tests for {@link AbstractEventForwarderListener}: each {@code onAfter*} hook must
 * produce exactly one {@link EventEnvelope} POST through the poster, and the listener must keep
 * {@code onAfterCommitTransaction} as the {@link PolarisEventListener} default no-op (see the
 * comment in the production class for why).
 */
class AbstractEventForwarderListenerTest {

  // ============= Catalog handlers =============

  @Test
  void createCatalogPostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterCreateCatalog(
            new CatalogsServiceEvents.AfterCreateCatalogEvent(sampleCatalog("cat")));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterCreateCatalog");
    assertThat(poster.posted.get(0).catalog()).isEqualTo("cat");
  }

  @Test
  void updateCatalogPostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterUpdateCatalog(
            new CatalogsServiceEvents.AfterUpdateCatalogEvent(sampleCatalog("cat")));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterUpdateCatalog");
  }

  @Test
  void deleteCatalogPostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterDeleteCatalog(new CatalogsServiceEvents.AfterDeleteCatalogEvent("cat"));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterDeleteCatalog");
  }

  // ============= Namespace handlers =============

  @Test
  void createNamespacePostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterCreateNamespace(
            new IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
                "cat", Namespace.of("ns"), Map.of("k", "v")));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterCreateNamespace");
    assertThat(poster.posted.get(0).namespace()).containsExactly("ns");
  }

  @Test
  void updateNamespacePropertiesPostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    UpdateNamespacePropertiesResponse resp = UpdateNamespacePropertiesResponse.builder().build();
    new TestableListener(poster)
        .onAfterUpdateNamespaceProperties(
            new IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent(
                "cat", Namespace.of("ns"), resp, Map.of("k", "v")));
    assertThat(poster.posted).hasSize(1);
    EventEnvelope env = poster.posted.get(0);
    assertThat(env.eventType()).isEqualTo("AfterUpdateNamespaceProperties");
    assertThat(env.properties()).containsEntry("k", "v");
  }

  @Test
  void updateNamespacePropertiesNullCurrentPropertiesPostsEnvelopeWithoutProperties() {
    // currentProperties=null is the delegator's post-load-failed signal. The listener must STILL
    // forward the event (so the receiver sees the change happened) but with properties left
    // unset so the receiver doesn't wipe its known state. propertyUpdates (from the REST
    // response) still rides along so the receiver has the diff.
    RecordingPoster poster = new RecordingPoster();
    UpdateNamespacePropertiesResponse resp =
        UpdateNamespacePropertiesResponse.builder().addUpdated("k").build();
    new TestableListener(poster)
        .onAfterUpdateNamespaceProperties(
            new IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent(
                "cat", Namespace.of("ns"), resp, /*currentProperties*/ null));
    assertThat(poster.posted).hasSize(1);
    EventEnvelope env = poster.posted.get(0);
    assertThat(env.properties()).isNull();
    assertThat(env.propertyUpdates().updated()).containsExactly("k");
  }

  @Test
  void dropNamespacePostsExactlyOneEnvelopeWithRawSeparator() {
    RecordingPoster poster = new RecordingPoster();
    String raw = "nssub";
    new TestableListener(poster)
        .onAfterDropNamespace(new IcebergRestCatalogEvents.AfterDropNamespaceEvent("cat", raw));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).namespaceRaw()).isEqualTo(raw);
  }

  // ============= Table handlers =============

  @Test
  void createTablePostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterCreateTable(
            new IcebergRestCatalogEvents.AfterCreateTableEvent(
                "cat", Namespace.of("ns"), "tbl", loadResponseFor(sampleMetadata())));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterCreateTable");
  }

  @Test
  void registerTablePostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterRegisterTable(
            new IcebergRestCatalogEvents.AfterRegisterTableEvent(
                "cat", Namespace.of("ns"), "tbl", loadResponseFor(sampleMetadata())));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterRegisterTable");
  }

  @Test
  void updateTablePostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterUpdateTable(
            new IcebergRestCatalogEvents.AfterUpdateTableEvent(
                "cat", Namespace.of("ns"), "tbl", null, loadResponseFor(sampleMetadata())));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterUpdateTable");
  }

  @Test
  void commitTablePostsExactlyOneEnvelopeEvenWhenMetadataMissing() {
    // Unlike the old DataHub listener, the forwarder ships commit envelopes even with null
    // metadata — the commit signal itself is information the receiver should not lose. The
    // tableMetadataJson is simply absent so the receiver doesn't overwrite known state.
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterCommitTable(
            new IcebergRestCatalogEvents.AfterCommitTableEvent(
                "cat", TableIdentifier.of("ns", "tbl"), null, null));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).tableMetadataJson()).isNull();
  }

  @Test
  void renameTablePostsExactlyOneEnvelopeWithBothEndpoints() {
    RecordingPoster poster = new RecordingPoster();
    RenameTableRequest req =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of("ns", "old"))
            .withDestination(TableIdentifier.of("ns", "new"))
            .build();
    new TestableListener(poster)
        .onAfterRenameTable(
            new IcebergRestCatalogEvents.AfterRenameTableEvent(
                "cat", req, loadResponseFor(sampleMetadata())));
    assertThat(poster.posted).hasSize(1);
    EventEnvelope env = poster.posted.get(0);
    assertThat(env.table()).isEqualTo("old");
    assertThat(env.renameTo().name()).isEqualTo("new");
  }

  @Test
  void dropTablePostsExactlyOneEnvelope() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterDropTable(
            new IcebergRestCatalogEvents.AfterDropTableEvent(
                "cat", Namespace.of("ns"), "tbl", false));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterDropTable");
  }

  // ============= AuditContext flow =============

  @Test
  void principalFromGetAuditContextFlowsIntoEnvelopeActor() {
    RecordingPoster poster = new RecordingPoster();
    AbstractEventForwarderListener listener =
        new TestableListener(poster) {
          @Override
          protected AuditContext getAuditContext() {
            return new AuditContext(99L, "alice@example.com", "test-realm");
          }
        };
    listener.onAfterDeleteCatalog(new CatalogsServiceEvents.AfterDeleteCatalogEvent("cat"));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).timestampMs()).isEqualTo(99L);
    assertThat(poster.posted.get(0).actor()).isEqualTo("alice@example.com");
  }

  // ============= Transactional commit deferral (Adversarial review #1 regression) =============

  @Test
  void commitTableOutsideTransactionPostsImmediately() {
    RecordingPoster poster = new RecordingPoster();
    new TestableListener(poster)
        .onAfterCommitTable(
            new IcebergRestCatalogEvents.AfterCommitTableEvent(
                "cat", TableIdentifier.of("ns", "tbl"), null, sampleMetadata()));
    // No beginTransaction/endTransaction — legacy eager-post path.
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).eventType()).isEqualTo("AfterCommitTable");
  }

  @Test
  void commitTableInsideTransactionIsBufferedUntilCommitSuccess() {
    RecordingPoster poster = new RecordingPoster();
    AbstractEventForwarderListener listener = new TestableListener(poster);

    listener.beginTransaction();
    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "t1"), null, sampleMetadata()));
    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "t2"), null, sampleMetadata()));

    assertThat(poster.posted)
        .as("envelopes must stay buffered until endTransaction(true)")
        .isEmpty();

    listener.endTransaction(true);

    assertThat(poster.posted).hasSize(2);
    assertThat(poster.posted.get(0).table()).isEqualTo("t1");
    assertThat(poster.posted.get(1).table()).isEqualTo("t2");
  }

  @Test
  void commitTableInsideTransactionIsDiscardedOnRollback() {
    // The scenario Codex flagged as no-ship: per-table commit events fired before the runtime's
    // final atomic update, which then failed. The receiver must NOT see these events because
    // Polaris itself rolled back the transaction.
    RecordingPoster poster = new RecordingPoster();
    AbstractEventForwarderListener listener = new TestableListener(poster);

    listener.beginTransaction();
    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "t1"), null, sampleMetadata()));
    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "t2"), null, sampleMetadata()));
    listener.endTransaction(false);

    assertThat(poster.posted)
        .as("rolled-back transaction must not reach the receiver")
        .isEmpty();
  }

  @Test
  void rolledBackTransactionDoesNotLeakBufferIntoNextRequest() {
    // The ThreadLocal buffer is reusable across requests (JAX-RS thread pool). A failed
    // transaction must fully clear it so the next request — which may be a standalone commit
    // on the same thread — sees the legacy eager-post path.
    RecordingPoster poster = new RecordingPoster();
    AbstractEventForwarderListener listener = new TestableListener(poster);

    listener.beginTransaction();
    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "rolled-back"), null, sampleMetadata()));
    listener.endTransaction(false);

    // Subsequent standalone commit must post immediately (no leftover buffer from above).
    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "standalone"), null, sampleMetadata()));
    assertThat(poster.posted).hasSize(1);
    assertThat(poster.posted.get(0).table()).isEqualTo("standalone");
  }

  // ============= commitTransaction guard =============

  @Test
  void commitTransactionMustRemainNoOpToAvoidDuplicateEvents() throws NoSuchMethodException {
    // Multi-table atomic commits surface BOTH onAfterCommitTable (per-table, with metadata) and
    // onAfterCommitTransaction (without metadata). If this class accidentally overrides
    // onAfterCommitTransaction it would re-emit each table with no metadata, confusing receivers
    // that key off (catalog, table) -> latest TableMetadata. Lock the no-op stance in via
    // reflection: walk the inherited method, not the declared one, so a *failure* here means the
    // override accidentally landed on AbstractEventForwarderListener instead of staying on the
    // PolarisEventListener default.
    Method m =
        AbstractEventForwarderListener.class.getMethod(
            "onAfterCommitTransaction",
            IcebergRestCatalogEvents.AfterCommitTransactionEvent.class);
    assertThat(m.getDeclaringClass())
        .as(
            "AbstractEventForwarderListener must NOT override onAfterCommitTransaction — leave"
                + " the PolarisEventListener default no-op")
        .isEqualTo(PolarisEventListener.class);
  }

  // ============= helpers =============

  private static TableMetadata sampleMetadata() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), "s3://b/t", Map.of());
  }

  private static LoadTableResponse loadResponseFor(TableMetadata md) {
    return LoadTableResponse.builder().withTableMetadata(md).build();
  }

  private static Catalog sampleCatalog(String name) {
    return new PolarisCatalog(
        Catalog.TypeEnum.INTERNAL,
        name,
        CatalogProperties.builder("s3://bucket/" + name).build(),
        new FileStorageConfigInfo(
            FileStorageConfigInfo.StorageTypeEnum.FILE, List.of("s3://bucket/")));
  }

  /** Captures every envelope the listener posts. */
  private static final class RecordingPoster {
    final List<EventEnvelope> posted = new ArrayList<>();

    HttpEventPoster asPoster() {
      // Mock the (otherwise concrete) HttpEventPoster so it just records calls. Using Mockito
      // avoids a bespoke subclass and side-steps the replay-buffer thread + HttpClient ctor.
      HttpEventPoster mock = mock(HttpEventPoster.class);
      when(mock.post(org.mockito.ArgumentMatchers.any(EventEnvelope.class)))
          .thenAnswer(
              inv -> {
                posted.add(inv.getArgument(0));
                return true;
              });
      return mock;
    }
  }

  /** Listener under test wired to a recording poster + a no-op serializer (using the real one). */
  private static class TestableListener extends AbstractEventForwarderListener {
    TestableListener(RecordingPoster recorder) {
      super(recorder.asPoster(), new EventSerializer());
    }
  }
}
