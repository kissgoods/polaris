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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

class AbstractDataHubEventListenerTest {

  @Test
  void createNamespaceUpsertsContainerWithNamespaceSubtype() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterCreateNamespace(
        new IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
            "cat", Namespace.of("ns"), Map.of("k", "v")));

    // 3-tier 분류 이후 모든 upsert 핸들러는 [domain, entity] 순으로 emit.
    assertThat(emitter.upserts).hasSize(2);
    assertThat(emitter.upserts.get(0).entityType).isEqualTo(DataHubEventMapper.DOMAIN);
    Upsert u = emitter.upserts.get(1);
    assertThat(u.urn).isEqualTo("urn:li:container:iceberg.polaris.cat.ns");
    assertThat(u.entityType).isEqualTo("container");
    // subType 이 "Namespace" 로 박혀야 DataHub UI 에서 Schema 가 아닌 Namespace 로 표시됨.
    @SuppressWarnings("unchecked")
    Map<String, Object> subTypes = (Map<String, Object>) u.aspects.get("subTypes");
    assertThat(subTypes.get("typeNames")).isEqualTo(java.util.List.of("Namespace"));
  }

  @Test
  void dropTableEmitsStatusRemovedOnDatasetUrn() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterDropTable(
        new IcebergRestCatalogEvents.AfterDropTableEvent("cat", Namespace.of("ns"), "tbl", false));

    assertThat(emitter.upserts).isEmpty();
    assertThat(emitter.removes).hasSize(1);
    StatusRemoved r = emitter.removes.get(0);
    assertThat(r.urn).isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)");
    assertThat(r.entityType).isEqualTo("dataset");
  }

  @Test
  void commitTableSkipsEmitWhenTableMetadataMissing() {
    // Iceberg-form 검증: metadataAfter == null 이면 dataset emit (+ domain emit) 자체를 skip.
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterCommitTable(
        new IcebergRestCatalogEvents.AfterCommitTableEvent(
            "cat", TableIdentifier.of("ns", "tbl"), null, /*metadataAfter*/ null));

    assertThat(emitter.upserts).as("metadata null → skip all upserts (domain 도 X)").isEmpty();
    assertThat(emitter.removes).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  void renameTableEmitsRemoveForOldUrnAndMinimalUpsertForNewUrnWhenLoadFails() {
    // rename 이벤트 자체가 Iceberg 보증이므로, post-rename loadTable 이 실패해도 destination 을
    // 최소 aspect (schemaMetadata 없이) 로 emit — dataset 이 DataHub 에서 사라지지 않게.
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    RenameTableRequest req =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of("ns", "old"))
            .withDestination(TableIdentifier.of("ns", "new"))
            .build();
    listener.onAfterRenameTable(
        new IcebergRestCatalogEvents.AfterRenameTableEvent("cat", req, /*loadTableResponse*/ null));

    // 옛 URN 은 항상 soft-delete
    assertThat(emitter.removes).hasSize(1);
    assertThat(emitter.removes.get(0).urn)
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.old,PROD)");

    // 새 URN 도 emit — [etc Domain entity, destination dataset]
    assertThat(emitter.upserts)
        .as("rename 후 metadata 없어도 destination 은 minimal aspect 로 emit")
        .extracting(u -> u.entityType)
        .containsExactly(DataHubEventMapper.DOMAIN, DataHubEventMapper.DATASET);

    Upsert dataset = emitter.upserts.get(1);
    assertThat(dataset.urn)
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.new,PROD)");
    // schemaMetadata 는 omit (post-rename load 실패 → schema 정보 없음)
    assertThat(dataset.aspects).doesNotContainKey("schemaMetadata");
    // datasetProperties 는 customProperties 없이 emit (metadata 없으므로 enriched 정보 없음)
    Map<String, Object> datasetProps =
        (Map<String, Object>) dataset.aspects.get("datasetProperties");
    assertThat(datasetProps).doesNotContainKey("customProperties");
    // subType 은 여전히 Table 로 pin
    Map<String, Object> subTypes = (Map<String, Object>) dataset.aspects.get("subTypes");
    assertThat(subTypes.get("typeNames")).isEqualTo(java.util.List.of("Table"));
  }

  /**
   * Fallback path: when the delegator's follow-up loadNamespaceMetadata fails (or the listener
   * doesn't opt into the marker), the event arrives with {@code currentProperties == null}. The
   * listener must omit {@code containerProperties} so DataHub keeps its previously-synced state
   * rather than overwriting it with empty/partial data.
   */
  @Test
  void updateNamespacePropertiesOmitsContainerPropertiesWhenCurrentPropertiesAbsent() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    UpdateNamespacePropertiesResponse response =
        UpdateNamespacePropertiesResponse.builder().build();
    listener.onAfterUpdateNamespaceProperties(
        new IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent(
            "cat", Namespace.of("ns"), response, null));

    assertThat(emitter.upserts).hasSize(2);
    assertThat(emitter.upserts.get(0).entityType).isEqualTo(DataHubEventMapper.DOMAIN);
    Upsert u = emitter.upserts.get(1);
    assertThat(u.urn).isEqualTo("urn:li:container:iceberg.polaris.cat.ns");
    assertThat(u.aspects).doesNotContainKey("containerProperties");
  }

  /**
   * Happy path: when the delegator successfully loaded the full namespace state after the update
   * (via the RequiresPostUpdateNamespaceMetadata marker), the listener must emit the resulting
   * properties as {@code containerProperties.customProperties} so DataHub reflects the latest
   * state — fixes the stale-customProperties regression where update events kept DataHub frozen.
   */
  @Test
  void updateNamespacePropertiesEmitsContainerPropertiesWhenCurrentPropertiesPresent() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    UpdateNamespacePropertiesResponse response =
        UpdateNamespacePropertiesResponse.builder().build();
    Map<String, String> current = Map.of("owner", "bob", "purpose", "analytics");
    listener.onAfterUpdateNamespaceProperties(
        new IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent(
            "cat", Namespace.of("ns"), response, current));

    assertThat(emitter.upserts).hasSize(2);
    assertThat(emitter.upserts.get(0).entityType).isEqualTo(DataHubEventMapper.DOMAIN);
    Upsert u = emitter.upserts.get(1);
    assertThat(u.urn).isEqualTo("urn:li:container:iceberg.polaris.cat.ns");
    @SuppressWarnings("unchecked")
    Map<String, Object> containerProps = (Map<String, Object>) u.aspects.get("containerProperties");
    assertThat(containerProps).isNotNull();
    assertThat(containerProps.get("customProperties")).isEqualTo(current);
  }

  /**
   * Regression for the production-impact analysis: {@code onAfterCommitTransaction} must NOT be
   * overridden. Inside a multi-table commitTransaction, {@code TableOperations.doCommit()} in
   * {@code IcebergCatalog} already fires {@code onAfterCommitTable} per table with full {@code
   * TableMetadata}, which the listener uses to emit a correct upsert. If
   * {@code onAfterCommitTransaction} also emitted (with null properties since the event carries
   * no metadata), it would overwrite the just-synced datasetProperties aspect and wipe DataHub's
   * customProperties for every table touched by the transaction.
   */
  @Test
  void commitTransactionMustRemainNoOpToAvoidWipingCustomProperties() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    CommitTransactionRequest req =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(TableIdentifier.of("ns", "t1"), List.of(), List.of()),
                UpdateTableRequest.create(TableIdentifier.of("ns", "t2"), List.of(), List.of())));
    listener.onAfterCommitTransaction(
        new IcebergRestCatalogEvents.AfterCommitTransactionEvent("cat", req));

    assertThat(emitter.upserts).isEmpty();
    assertThat(emitter.removes).isEmpty();
  }

  /**
   * Defensive structural assertion: the {@link PolarisEventListener#onAfterCommitTransaction}
   * default in the interface is no-op, and {@link AbstractDataHubEventListener} must inherit it
   * unchanged. If a future change adds an override here, the behavioural test above would catch
   * it; this assertion makes the contract explicit at the method-resolution level.
   */
  @Test
  void abstractDataHubEventListenerDoesNotOverrideOnAfterCommitTransaction() throws Exception {
    Class<?> declaringClass =
        AbstractDataHubEventListener.class
            .getMethod(
                "onAfterCommitTransaction",
                IcebergRestCatalogEvents.AfterCommitTransactionEvent.class)
            .getDeclaringClass();
    assertThat(declaringClass).isEqualTo(PolarisEventListener.class);
  }

  @Test
  void dropNamespaceWithUnitSeparatorRawStringNormalizesToSameUrn() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    String raw = "ns" + DataHubEventMapper.NS_SEPARATOR + "sub";
    listener.onAfterDropNamespace(new IcebergRestCatalogEvents.AfterDropNamespaceEvent("cat", raw));

    assertThat(emitter.removes).hasSize(1);
    assertThat(emitter.removes.get(0).urn).isEqualTo("urn:li:container:iceberg.polaris.cat.ns.sub");
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternEmitsChildDomainEntityOnNamespaceHandler() {
    // pattern 매칭 catalog 의 namespace upsert 경로에서 자식 Domain entity 도 함께 emit 되는지 검증.
    // ensureCatalogDomain() 이 각 핸들러 시작 부분에서 호출됨 (table handler 도 동일 helper 호출).
    // Table commit 의 Domain emit 검증은 metadata 가 동반된 경로에서만 가능하므로 별도 통합 테스트
    // (e2e mock-GMS) 에서 cover. unit 레벨에서는 namespace handler 의 helper 호출로 충분.
    String catalog = "my_x0173699_catalog";
    String expectedDomainUrn = "urn:li:domain:" + catalog;

    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());
    listener.onAfterCreateNamespace(
        new IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
            catalog, Namespace.of("ns"), Map.of()));
    assertThat(emitter.upserts)
        .as("namespace event upserts = [domain, namespace container]")
        .extracting(u -> u.urn)
        .containsExactly(
            expectedDomainUrn, "urn:li:container:iceberg.polaris." + catalog + ".ns");
    // 첫 emit (자식 Domain) 의 parentDomain 검증
    Map<String, Object> domainProps =
        (Map<String, Object>) emitter.upserts.get(0).aspects.get("domainProperties");
    assertThat(domainProps.get("parentDomain")).isEqualTo("urn:li:domain:user_catalog");
    assertThat(domainProps.get("name")).isEqualTo(catalog);
  }

  @Test
  void deleteCatalogSoftDeletesChildDomainForPatternMatchedCatalog() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterDeleteCatalog(
        new org.apache.polaris.service.events.CatalogsServiceEvents.AfterDeleteCatalogEvent(
            "my_x99999_catalog"));

    assertThat(emitter.removes)
        .as("pattern catalog drop → container + child Domain both soft-deleted")
        .extracting(r -> r.urn)
        .containsExactly(
            "urn:li:container:iceberg.polaris.my_x99999_catalog",
            "urn:li:domain:my_x99999_catalog");
    assertThat(emitter.removes)
        .extracting(r -> r.entityType)
        .containsExactly(DataHubEventMapper.CONTAINER, DataHubEventMapper.DOMAIN);
  }

  @Test
  void deleteNonPatternCatalogDoesNotTouchAnyDomain() {
    // 패턴 미매칭 catalog 삭제는 catalog container 만 soft-delete; Domain 은 건드리지 않음.
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterDeleteCatalog(
        new org.apache.polaris.service.events.CatalogsServiceEvents.AfterDeleteCatalogEvent(
            "regular_cat"));

    assertThat(emitter.removes).hasSize(1);
    assertThat(emitter.removes.get(0).entityType).isEqualTo(DataHubEventMapper.CONTAINER);
  }

  @Test
  @SuppressWarnings("unchecked")
  void nonPatternCatalogEmitsEtcDomainEntity() {
    // Rule 3 회귀 보호: 패턴 미매칭 catalog 는 etc_polaris Domain entity 를 함께 emit.
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterCreateNamespace(
        new IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
            "regular_cat", Namespace.of("ns"), Map.of()));

    assertThat(emitter.upserts)
        .as("non-pattern catalog upserts = [etc Domain, namespace container]")
        .extracting(u -> u.urn)
        .containsExactly(
            DataHubEventMapper.ETC_DOMAIN_URN,
            "urn:li:container:iceberg.polaris.regular_cat.ns");

    Map<String, Object> domainProps =
        (Map<String, Object>) emitter.upserts.get(0).aspects.get("domainProperties");
    assertThat(domainProps.get("name")).isEqualTo("etc_polaris");
    assertThat(domainProps).doesNotContainKey("parentDomain"); // flat

    // namespace container 에 etc domain + corpuser:etc owner 부착 검증
    Map<String, Object> nsAspects = emitter.upserts.get(1).aspects;
    Map<String, Object> domains = (Map<String, Object>) nsAspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly(DataHubEventMapper.ETC_DOMAIN_URN);
    Map<String, Object> ownership = (Map<String, Object>) nsAspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0))
        .containsEntry("owner", "urn:li:corpuser:etc")
        .containsEntry("type", "TECHNICAL_OWNER");
  }

  @Test
  @SuppressWarnings("unchecked")
  void lakeCatalogEmitsLakeDomainEntityAndDatalakeOwner() {
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterCreateNamespace(
        new IcebergRestCatalogEvents.AfterCreateNamespaceEvent(
            "lake_catalog", Namespace.of("ns"), Map.of()));

    assertThat(emitter.upserts)
        .extracting(u -> u.urn)
        .containsExactly(
            DataHubEventMapper.LAKE_CATALOG_DOMAIN_URN,
            "urn:li:container:iceberg.polaris.lake_catalog.ns");

    Map<String, Object> domainProps =
        (Map<String, Object>) emitter.upserts.get(0).aspects.get("domainProperties");
    assertThat(domainProps.get("name")).isEqualTo("lake_catalog_polaris");
    assertThat(domainProps).doesNotContainKey("parentDomain"); // flat

    // namespace container 의 domain + ownership
    Map<String, Object> nsAspects = emitter.upserts.get(1).aspects;
    Map<String, Object> domains = (Map<String, Object>) nsAspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly(DataHubEventMapper.LAKE_CATALOG_DOMAIN_URN);
    Map<String, Object> ownership = (Map<String, Object>) nsAspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0))
        .containsEntry("owner", "urn:li:corpuser:datalake")
        .containsEntry("type", "TECHNICAL_OWNER");
  }

  @Test
  void deleteLakeCatalogDoesNotTouchSharedLakeDomain() {
    // Rule 2 의 Domain 은 다른 catalog 가 공유할 수 있으므로 drop 시 미터치.
    RecordingEmitter emitter = new RecordingEmitter();
    TestableListener listener = new TestableListener(emitter, mapper());

    listener.onAfterDeleteCatalog(
        new org.apache.polaris.service.events.CatalogsServiceEvents.AfterDeleteCatalogEvent(
            "lake_catalog"));

    assertThat(emitter.removes)
        .hasSize(1)
        .allSatisfy(r -> assertThat(r.entityType).isEqualTo(DataHubEventMapper.CONTAINER));
  }

  private DataHubEventMapper mapper() {
    return new DataHubEventMapper(
        new DataHubConfiguration() {
          @Override
          public Optional<String> gmsUrl() {
            return Optional.of("http://localhost:8080");
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
            return false;
          }

          @Override
          public Duration connectTimeout() {
            return Duration.ofSeconds(5);
          }

          @Override
          public Duration requestTimeout() {
            return Duration.ofSeconds(30);
          }

          @Override
          public int maxInflightEmits() {
            return 1000;
          }

          @Override
          public int circuitBreakerFailureThreshold() {
            return 5;
          }

          @Override
          public Duration circuitBreakerOpenDuration() {
            return Duration.ofSeconds(30);
          }

          @Override
          public int replayBufferCapacity() {
            return 0;
          }

          @Override
          public Map<String, String> domainMapping() {
            return Map.of();
          }
        });
  }

  private static class TestableListener extends AbstractDataHubEventListener {
    TestableListener(DataHubEmitter emitter, DataHubEventMapper mapper) {
      super(emitter, mapper);
    }
  }

  private static class RecordingEmitter implements DataHubEmitter {
    final List<Upsert> upserts = new ArrayList<>();
    final List<StatusRemoved> removes = new ArrayList<>();

    @Override
    public void emitUpsert(String urn, String entityType, Map<String, Object> aspects) {
      upserts.add(new Upsert(urn, entityType, aspects));
    }

    @Override
    public void emitStatusRemoved(String urn, String entityType) {
      removes.add(new StatusRemoved(urn, entityType));
    }
  }

  private record Upsert(String urn, String entityType, Map<String, Object> aspects) {}

  private record StatusRemoved(String urn, String entityType) {}
}
