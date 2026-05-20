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

import jakarta.annotation.PreDestroy;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.extension.datahub.DataHubEventMapper.AuditContext;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.RequiresPostRenameTableMetadata;
import org.apache.polaris.service.events.listeners.RequiresPostUpdateNamespaceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates the subset of {@link PolarisEventListener} hooks we care about (catalog / namespace /
 * table CRUD) into emit calls on a {@link DataHubEmitter}. Concrete subclasses inject the emitter;
 * the bundled implementation is HTTP-based ({@code DataHubHttpEventListener}).
 *
 * <p>All emit failures are caught at the emitter layer; this listener never throws back into
 * Polaris so that DataHub outages cannot break catalog operations.
 */
public abstract class AbstractDataHubEventListener
    implements PolarisEventListener,
        RequiresPostRenameTableMetadata,
        RequiresPostUpdateNamespaceMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDataHubEventListener.class);

  protected final DataHubEmitter emitter;
  protected final DataHubEventMapper mapper;

  protected AbstractDataHubEventListener(DataHubEmitter emitter, DataHubEventMapper mapper) {
    this.emitter = emitter;
    this.mapper = mapper;
  }

  /**
   * No-arg constructor required by Quarkus/Arc to generate a client proxy for
   * {@code @ApplicationScoped} subclasses. Never invoked for live beans — those go through the
   * {@code @Inject} constructors on the concrete subclasses.
   */
  protected AbstractDataHubEventListener() {
    this.emitter = null;
    this.mapper = null;
  }

  /**
   * Per-emit audit context (who triggered the operation, when). The default returns
   * {@code (System.currentTimeMillis(), null)} so test subclasses and the abstract base remain
   * usable without request-scoped JAX-RS plumbing. The HTTP listener overrides this to read the
   * caller principal from {@code SecurityContext}.
   */
  protected AuditContext getAuditContext() {
    return new AuditContext(System.currentTimeMillis(), null);
  }

  @PreDestroy
  void shutdown() {
    if (emitter == null) return;
    try {
      emitter.close();
    } catch (Exception e) {
      LOG.warn("Error closing DataHub emitter", e);
    }
  }

  /**
   * Idempotently emit the Domain entity that backs whichever classification rule {@code
   * catalogName} falls into. Called at the top of every upsert handler so DataHub's /domains UI
   * never has a dangling reference for entities the listener attaches to a Domain URN.
   *
   * <ul>
   *   <li>Rule 1 (user_catalog pattern): child Domain {@code urn:li:domain:<catalogName>} with
   *       {@code parentDomain = urn:li:domain:user_catalog}.
   *   <li>Rule 2 ({@code lake_catalog}): top-level {@code urn:li:domain:lake_catalog_polaris}.
   *   <li>Rule 3 (everything else): top-level {@code urn:li:domain:etc_polaris}.
   * </ul>
   *
   * Domain entities for rules 2 and 3 are shared across many catalogs — listener emits the same
   * URN on every upsert, idempotent upsert at DataHub.
   */
  protected void ensureCatalogDomain(String catalogName) {
    if (mapper.isUserCatalog(catalogName)) {
      emitter.emitUpsert(
          mapper.userCatalogChildDomainUrn(catalogName),
          DataHubEventMapper.DOMAIN,
          mapper.userCatalogChildDomainAspects(catalogName));
      return;
    }
    if (mapper.isLakeCatalog(catalogName)) {
      emitter.emitUpsert(
          DataHubEventMapper.LAKE_CATALOG_DOMAIN_URN,
          DataHubEventMapper.DOMAIN,
          mapper.lakeCatalogDomainAspects());
      return;
    }
    emitter.emitUpsert(
        DataHubEventMapper.ETC_DOMAIN_URN,
        DataHubEventMapper.DOMAIN,
        mapper.etcDomainAspects());
  }

  // ============= Catalog events =============

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    Catalog catalog = event.catalog();
    String name = catalog.getName();
    Map<String, String> properties =
        catalog.getProperties() == null ? Map.of() : catalog.getProperties().toMap();
    ensureCatalogDomain(name);
    emitter.emitUpsert(
        mapper.catalogContainerUrn(name),
        DataHubEventMapper.CONTAINER,
        mapper.catalogContainerAspects(name, properties));
  }

  @Override
  public void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {
    Catalog catalog = event.catalog();
    String name = catalog.getName();
    Map<String, String> properties =
        catalog.getProperties() == null ? Map.of() : catalog.getProperties().toMap();
    ensureCatalogDomain(name);
    emitter.emitUpsert(
        mapper.catalogContainerUrn(name),
        DataHubEventMapper.CONTAINER,
        mapper.catalogContainerAspects(name, properties));
  }

  @Override
  public void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {
    emitter.emitStatusRemoved(
        mapper.catalogContainerUrn(event.catalogName()), DataHubEventMapper.CONTAINER);
    // Only rule-1 (user_catalog pattern) auto-creates a *per-catalog* child Domain, so it's the
    // only rule whose Domain entity is 1:1 with the catalog and safe to soft-delete on drop. If
    // the catalog is later recreated with the same name, emitUpsert auto-injects status:
    // removed=false and the Domain resurrects. lake_catalog_polaris and etc_polaris are shared
    // across many catalogs so we never touch them on a single drop.
    if (mapper.isUserCatalog(event.catalogName())) {
      emitter.emitStatusRemoved(
          mapper.userCatalogChildDomainUrn(event.catalogName()), DataHubEventMapper.DOMAIN);
    }
  }

  // ============= Namespace events =============

  @Override
  public void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {
    ensureCatalogDomain(event.catalogName());
    emitter.emitUpsert(
        mapper.namespaceContainerUrn(event.catalogName(), event.namespace()),
        DataHubEventMapper.CONTAINER,
        mapper.namespaceContainerAspects(
            event.catalogName(), event.namespace(), event.namespaceProperties()));
  }

  @Override
  public void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {
    // The updateProperties response only carries the updated/removed key lists, not the resulting
    // state. The delegator does a follow-up loadNamespaceMetadata when the listener implements
    // RequiresPostUpdateNamespaceMetadata (we do) and packs the result into currentProperties().
    // Use it to emit the full current properties so DataHub stays in sync. If the load failed
    // (currentProperties == null), fall back to the conservative path: skip containerProperties
    // to avoid overwriting previously-synced state with stale or partial data.
    ensureCatalogDomain(event.catalogName());
    emitter.emitUpsert(
        mapper.namespaceContainerUrn(event.catalogName(), event.namespace()),
        DataHubEventMapper.CONTAINER,
        mapper.namespaceContainerAspects(
            event.catalogName(), event.namespace(), event.currentProperties()));
  }

  @Override
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {
    emitter.emitStatusRemoved(
        mapper.namespaceContainerUrnFromRaw(event.catalogName(), event.namespace()),
        DataHubEventMapper.CONTAINER);
  }

  // ============= Table events =============

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    upsertTable(
        event.catalogName(), event.namespace(), event.tableName(), event.loadTableResponse());
  }

  @Override
  public void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {
    upsertTable(
        event.catalogName(), event.namespace(), event.tableName(), event.loadTableResponse());
  }

  @Override
  public void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {
    upsertTable(
        event.catalogName(), event.namespace(), event.sourceTable(), event.loadTableResponse());
  }

  @Override
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {
    TableIdentifier id = event.identifier();
    TableMetadata metadataAfter = event.metadataAfter();
    // Iceberg-form 이 아닌 데이터는 DataHub 로 전송하지 않는다 — metadata 가 없으면
    // 진짜 Iceberg table 인지 확신할 수 없어 dataset upsert 자체를 skip.
    if (metadataAfter == null) {
      LOG.debug("Skipping commitTable emit for {}/{}.{} (TableMetadata missing — non-Iceberg or load failed)",
          event.catalogName(), id.namespace(), id.name());
      return;
    }
    ensureCatalogDomain(event.catalogName());
    emitter.emitUpsert(
        mapper.datasetUrn(event.catalogName(), id.namespace(), id.name()),
        DataHubEventMapper.DATASET,
        mapper.datasetAspects(
            event.catalogName(), id.namespace(), id.name(), metadataAfter, getAuditContext()));
  }

  @Override
  public void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {
    RenameTableRequest req = event.renameTableRequest();
    TableIdentifier from = req.source();
    TableIdentifier to = req.destination();

    // 구 URN status removed 는 metadata 와 무관하게 항상 emit (단순 삭제 신호).
    emitter.emitStatusRemoved(
        mapper.datasetUrn(event.catalogName(), from.namespace(), from.name()),
        DataHubEventMapper.DATASET);

    // rename 이벤트 자체가 Iceberg REST API 의 renameTable 엔드포인트에서 fire 되므로 destination
    // 도 이미 Iceberg-form 으로 확정. post-rename loadTable 이 실패해 metadata 가 비어도 dataset 이
    // DataHub 에서 사라지게 두지 않는다 — datasetAspects 는 metadata=null 일 때 schemaMetadata
    // aspect 만 omit 하고 나머지 (datasetProperties / container / dataPlatformInstance / domain /
    // ownership / subTypes) 는 정상 emit 한다. 다음 commit/update 가 오면 schema 가 채워진다.
    TableMetadata md =
        event.loadTableResponse() != null ? event.loadTableResponse().tableMetadata() : null;
    if (md == null) {
      LOG.warn(
          "Post-rename loadTable failed for {}/{}.{} — emitting destination with minimal aspects"
              + " (no schemaMetadata until next commit)",
          event.catalogName(), to.namespace(), to.name());
    }
    ensureCatalogDomain(event.catalogName());
    emitter.emitUpsert(
        mapper.datasetUrn(event.catalogName(), to.namespace(), to.name()),
        DataHubEventMapper.DATASET,
        mapper.datasetAspects(
            event.catalogName(), to.namespace(), to.name(), md, getAuditContext()));
  }

  @Override
  public void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {
    emitter.emitStatusRemoved(
        mapper.datasetUrn(event.catalogName(), event.namespace(), event.table()),
        DataHubEventMapper.DATASET);
  }

  // commitTransaction is intentionally NOT overridden. Multi-table atomic commits surface BOTH
  // onAfterCommitTable (per table, with full TableMetadata) and onAfterCommitTransaction (without
  // metadata). onAfterCommitTable above already syncs each table with the correct properties;
  // emitting again from commitTransaction with null properties would wipe DataHub's customProperties
  // for those same tables. See IcebergCatalog.TableOperations.doCommit which fires
  // onAfterCommitTable for every per-table commit inside a transaction.

  // ============= helpers =============

  private void upsertTable(
      String catalogName, Namespace namespace, String tableName, LoadTableResponse resp) {
    TableMetadata md = resp == null ? null : resp.tableMetadata();
    // Iceberg-form 이 아닌 데이터는 DataHub 로 전송하지 않는다 — TableMetadata 가 없으면
    // 진짜 Iceberg table 인지 확신할 수 없어 dataset upsert 자체를 skip. create / register /
    // update 모두 정상 경로에서는 LoadTableResponse + metadata 를 동반하므로 실제로 trigger
    // 되는 경우는 매우 드물지만 (REST 응답 파싱 실패 등 edge case), 명시적으로 차단한다.
    if (md == null) {
      LOG.debug("Skipping dataset emit for {}/{}.{} (TableMetadata missing — non-Iceberg or load failed)",
          catalogName, namespace, tableName);
      return;
    }
    ensureCatalogDomain(catalogName);
    emitter.emitUpsert(
        mapper.datasetUrn(catalogName, namespace, tableName),
        DataHubEventMapper.DATASET,
        mapper.datasetAspects(catalogName, namespace, tableName, md, getAuditContext()));
  }
}
