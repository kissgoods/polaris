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

package org.apache.polaris.service.catalog.iceberg;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckExistsTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckExistsViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterListNamespacesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterListTablesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterListViewsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadCredentialsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterRegisterTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterRenameTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterRenameViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterReplaceViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterSendNotificationEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterUpdateTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckExistsTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckExistsViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCreateNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCreateTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCreateViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeDropNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeDropTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeDropViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeListNamespacesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeListTablesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeListViewsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadCredentialsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeRegisterTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeRenameTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeRenameViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeReplaceViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeSendNotificationEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeUpdateTableEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.RequiresPostRenameTableMetadata;
import org.apache.polaris.service.events.listeners.RequiresPostUpdateNamespaceMetadata;
import org.apache.polaris.service.events.listeners.TransactionalCommitTableDeferred;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Decorator
@Priority(1000)
public class IcebergRestCatalogEventServiceDelegator
    implements IcebergRestCatalogApiService, CatalogAdapter {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergRestCatalogEventServiceDelegator.class);

  @Inject @Delegate IcebergCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createNamespace(
      String prefix,
      CreateNamespaceRequest createNamespaceRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeCreateNamespace(
        new BeforeCreateNamespaceEvent(catalogName, createNamespaceRequest));
    Response resp =
        delegate.createNamespace(prefix, createNamespaceRequest, realmContext, securityContext);
    CreateNamespaceResponse createNamespaceResponse = (CreateNamespaceResponse) resp.getEntity();
    polarisEventListener.onAfterCreateNamespace(
        new AfterCreateNamespaceEvent(
            catalogName,
            createNamespaceResponse.namespace(),
            createNamespaceResponse.properties()));
    return resp;
  }

  @Override
  public Response listNamespaces(
      String prefix,
      String pageToken,
      Integer pageSize,
      String parent,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeListNamespaces(new BeforeListNamespacesEvent(catalogName, parent));
    Response resp =
        delegate.listNamespaces(prefix, pageToken, pageSize, parent, realmContext, securityContext);
    polarisEventListener.onAfterListNamespaces(new AfterListNamespacesEvent(catalogName, parent));
    return resp;
  }

  @Override
  public Response loadNamespaceMetadata(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeLoadNamespaceMetadata(
        new BeforeLoadNamespaceMetadataEvent(catalogName, decodeNamespace(namespace)));
    Response resp =
        delegate.loadNamespaceMetadata(prefix, namespace, realmContext, securityContext);
    GetNamespaceResponse getNamespaceResponse = (GetNamespaceResponse) resp.getEntity();
    polarisEventListener.onAfterLoadNamespaceMetadata(
        new AfterLoadNamespaceMetadataEvent(
            catalogName, getNamespaceResponse.namespace(), getNamespaceResponse.properties()));
    return resp;
  }

  @Override
  public Response namespaceExists(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCheckExistsNamespace(
        new BeforeCheckExistsNamespaceEvent(catalogName, namespaceObj));
    Response resp = delegate.namespaceExists(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onAfterCheckExistsNamespace(
        new AfterCheckExistsNamespaceEvent(catalogName, namespaceObj));
    return resp;
  }

  @Override
  public Response dropNamespace(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeDropNamespace(
        new BeforeDropNamespaceEvent(catalogName, decodeNamespace(namespace)));
    Response resp = delegate.dropNamespace(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onAfterDropNamespace(new AfterDropNamespaceEvent(catalogName, namespace));
    return resp;
  }

  @Override
  public Response updateProperties(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeUpdateNamespaceProperties(
        new BeforeUpdateNamespacePropertiesEvent(
            catalogName, namespaceObj, updateNamespacePropertiesRequest));
    Response resp =
        delegate.updateProperties(
            prefix, namespace, updateNamespacePropertiesRequest, realmContext, securityContext);
    Map<String, String> currentProperties = null;
    // The updateProperties response only lists updated/removed keys, not the resulting state.
    // Listeners that need the full current properties (e.g. DataHub mapper, which would otherwise
    // omit the containerProperties aspect and leave stale customProperties at DataHub) opt in via
    // RequiresPostUpdateNamespaceMetadata; default listeners skip the extra load.
    if (polarisEventListener instanceof RequiresPostUpdateNamespaceMetadata) {
      try (Response loadResp =
          delegate.loadNamespaceMetadata(prefix, namespace, realmContext, securityContext)) {
        if (loadResp != null && loadResp.getStatus() == Response.Status.OK.getStatusCode()) {
          GetNamespaceResponse loaded = (GetNamespaceResponse) loadResp.getEntity();
          if (loaded != null) {
            currentProperties = loaded.properties();
          }
        }
      } catch (Exception e) {
        // 이벤트는 currentProperties=null 로 전송 — listener 는 기존 동작 (containerProperties
        // 미emit) 으로 fallback 해서 DataHub 의 기존 properties 를 보존. Polaris 의 updateProperties
        // 자체에는 영향 없음.
        LOG.debug(
            "loadNamespaceMetadata after updateProperties failed for {}; "
                + "AfterUpdateNamespacePropertiesEvent will carry null currentProperties",
            namespace,
            e);
      }
    }
    polarisEventListener.onAfterUpdateNamespaceProperties(
        new AfterUpdateNamespacePropertiesEvent(
            catalogName,
            namespaceObj,
            (UpdateNamespacePropertiesResponse) resp.getEntity(),
            currentProperties));
    return resp;
  }

  @Override
  public Response createTable(
      String prefix,
      String namespace,
      CreateTableRequest createTableRequest,
      String accessDelegationMode,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCreateTable(
        new BeforeCreateTableEvent(
            catalogName, namespaceObj, createTableRequest, accessDelegationMode));
    Response resp =
        delegate.createTable(
            prefix,
            namespace,
            createTableRequest,
            accessDelegationMode,
            realmContext,
            securityContext);
    if (!createTableRequest.stageCreate()) {
      polarisEventListener.onAfterCreateTable(
          new AfterCreateTableEvent(
              catalogName,
              namespaceObj,
              createTableRequest.name(),
              (LoadTableResponse) resp.getEntity()));
    }
    return resp;
  }

  @Override
  public Response listTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeListTables(new BeforeListTablesEvent(catalogName, namespaceObj));
    Response resp =
        delegate.listTables(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListTables(new AfterListTablesEvent(catalogName, namespaceObj));
    return resp;
  }

  @Override
  public Response loadTable(
      String prefix,
      String namespace,
      String table,
      String accessDelegationMode,
      String ifNoneMatchString,
      String snapshots,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeLoadTable(
        new BeforeLoadTableEvent(
            catalogName, namespaceObj, table, accessDelegationMode, ifNoneMatchString, snapshots));
    Response resp =
        delegate.loadTable(
            prefix,
            namespace,
            table,
            accessDelegationMode,
            ifNoneMatchString,
            snapshots,
            realmContext,
            securityContext);
    polarisEventListener.onAfterLoadTable(
        new AfterLoadTableEvent(
            catalogName, namespaceObj, table, (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response tableExists(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCheckExistsTable(
        new BeforeCheckExistsTableEvent(catalogName, namespaceObj, table));
    Response resp = delegate.tableExists(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onAfterCheckExistsTable(
        new AfterCheckExistsTableEvent(catalogName, namespaceObj, table));
    return resp;
  }

  @Override
  public Response dropTable(
      String prefix,
      String namespace,
      String table,
      Boolean purgeRequested,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeDropTable(
        new BeforeDropTableEvent(catalogName, namespaceObj, table, purgeRequested));
    Response resp =
        delegate.dropTable(prefix, namespace, table, purgeRequested, realmContext, securityContext);
    polarisEventListener.onAfterDropTable(
        new AfterDropTableEvent(catalogName, namespaceObj, table, purgeRequested));
    return resp;
  }

  @Override
  public Response registerTable(
      String prefix,
      String namespace,
      RegisterTableRequest registerTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeRegisterTable(
        new BeforeRegisterTableEvent(catalogName, namespaceObj, registerTableRequest));
    Response resp =
        delegate.registerTable(
            prefix, namespace, registerTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRegisterTable(
        new AfterRegisterTableEvent(
            catalogName,
            namespaceObj,
            registerTableRequest.name(),
            (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response renameTable(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeRenameTable(
        new BeforeRenameTableEvent(catalogName, renameTableRequest));
    Response resp = delegate.renameTable(prefix, renameTableRequest, realmContext, securityContext);
    LoadTableResponse loadTableResponse = null;
    // Only pay for the extra loadTable when the active listener actually consumes the payload.
    // Listeners opt in via the RequiresPostRenameTableMetadata marker; default (no-op,
    // persistence-in-memory-buffer, aws-cloudwatch, ...) listeners skip this entirely.
    if (polarisEventListener instanceof RequiresPostRenameTableMetadata) {
      TableIdentifier destination = renameTableRequest.destination();
      // CatalogAdapter.decodeNamespace() runs URLEncoder.encode(ns) then
      // RESTUtil.decodeNamespace().
      // URLEncoder converts raw U+001F (0x1F) to "%1F", which RESTUtil.decodeNamespace() splits on.
      // Using RESTUtil.encodeNamespace() would produce "%1F" literals that URLEncoder
      // double-encodes
      // to "%251F", causing decodeNamespace() to see a single-level namespace. Raw U+001F is
      // correct.
      String destinationNs = String.join("", destination.namespace().levels());
      try (Response loadResp =
          delegate.loadTable(
              prefix,
              destinationNs,
              destination.name(),
              null,
              null,
              null,
              realmContext,
              securityContext)) {
        if (loadResp != null && loadResp.getStatus() == Response.Status.OK.getStatusCode()) {
          loadTableResponse = (LoadTableResponse) loadResp.getEntity();
        }
      } catch (Exception e) {
        // 이벤트는 properties 없이 전송 — Polaris rename 결과에는 영향 없음.
        LOG.debug(
            "loadTable after rename failed for {}.{}; AfterRenameTableEvent will carry null"
                + " loadTableResponse",
            destinationNs,
            destination.name(),
            e);
      }
    }
    polarisEventListener.onAfterRenameTable(
        new AfterRenameTableEvent(catalogName, renameTableRequest, loadTableResponse));
    return resp;
  }

  @Override
  public Response updateTable(
      String prefix,
      String namespace,
      String table,
      CommitTableRequest commitTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeUpdateTable(
        new BeforeUpdateTableEvent(catalogName, namespaceObj, table, commitTableRequest));
    Response resp =
        delegate.updateTable(
            prefix, namespace, table, commitTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateTable(
        new AfterUpdateTableEvent(
            catalogName,
            namespaceObj,
            table,
            commitTableRequest,
            (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response createView(
      String prefix,
      String namespace,
      CreateViewRequest createViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCreateView(
        new BeforeCreateViewEvent(catalogName, namespaceObj, createViewRequest));
    Response resp =
        delegate.createView(prefix, namespace, createViewRequest, realmContext, securityContext);
    polarisEventListener.onAfterCreateView(
        new AfterCreateViewEvent(
            catalogName,
            namespaceObj,
            createViewRequest.name(),
            (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response listViews(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeListViews(new BeforeListViewsEvent(catalogName, namespaceObj));
    Response resp =
        delegate.listViews(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListViews(new AfterListViewsEvent(catalogName, namespaceObj));
    return resp;
  }

  @Override
  public Response loadCredentials(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeLoadCredentials(
        new BeforeLoadCredentialsEvent(catalogName, namespaceObj, table));
    Response resp =
        delegate.loadCredentials(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onAfterLoadCredentials(
        new AfterLoadCredentialsEvent(catalogName, namespaceObj, table));
    return resp;
  }

  @Override
  public Response loadView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeLoadView(new BeforeLoadViewEvent(catalogName, namespaceObj, view));
    Response resp = delegate.loadView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterLoadView(
        new AfterLoadViewEvent(
            catalogName, namespaceObj, view, (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response viewExists(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCheckExistsView(
        new BeforeCheckExistsViewEvent(catalogName, namespaceObj, view));
    Response resp = delegate.viewExists(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterCheckExistsView(
        new AfterCheckExistsViewEvent(catalogName, namespaceObj, view));
    return resp;
  }

  @Override
  public Response dropView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeDropView(new BeforeDropViewEvent(catalogName, namespaceObj, view));
    Response resp = delegate.dropView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterDropView(new AfterDropViewEvent(catalogName, namespaceObj, view));
    return resp;
  }

  @Override
  public Response renameView(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeRenameView(
        new BeforeRenameViewEvent(catalogName, renameTableRequest));
    Response resp = delegate.renameView(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRenameView(
        new AfterRenameViewEvent(catalogName, renameTableRequest));
    return resp;
  }

  @Override
  public Response replaceView(
      String prefix,
      String namespace,
      String view,
      CommitViewRequest commitViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeReplaceView(
        new BeforeReplaceViewEvent(catalogName, namespaceObj, view, commitViewRequest));
    Response resp =
        delegate.replaceView(
            prefix, namespace, view, commitViewRequest, realmContext, securityContext);
    polarisEventListener.onAfterReplaceView(
        new AfterReplaceViewEvent(
            catalogName,
            namespaceObj,
            view,
            commitViewRequest,
            (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response commitTransaction(
      String prefix,
      CommitTransactionRequest commitTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeCommitTransaction(
        new IcebergRestCatalogEvents.BeforeCommitTransactionEvent(
            catalogName, commitTransactionRequest));
    // Listeners that forward per-table commit events downstream cannot ship them eagerly during a
    // transaction: IcebergCatalogHandler.commitTransaction fires onAfterCommitTable while still
    // populating a transaction workspace, and the final atomic update may still fail. The opt-in
    // marker TransactionalCommitTableDeferred lets such listeners buffer commit events and only
    // release them after this delegator confirms the underlying call succeeded. Listeners without
    // the marker keep the legacy eager-fire behavior.
    boolean deferred =
        polarisEventListener instanceof TransactionalCommitTableDeferred;
    TransactionalCommitTableDeferred tcd =
        deferred ? (TransactionalCommitTableDeferred) polarisEventListener : null;
    if (deferred) tcd.beginTransaction();
    Response resp;
    try {
      resp =
          delegate.commitTransaction(
              prefix, commitTransactionRequest, realmContext, securityContext);
    } catch (RuntimeException e) {
      if (deferred) tcd.endTransaction(false);
      throw e;
    }
    if (deferred) tcd.endTransaction(true);
    polarisEventListener.onAfterCommitTransaction(
        new IcebergRestCatalogEvents.AfterCommitTransactionEvent(
            catalogName, commitTransactionRequest));
    return resp;
  }

  /** This API is currently a no-op in Polaris. */
  @Override
  public Response reportMetrics(
      String prefix,
      String namespace,
      String table,
      ReportMetricsRequest reportMetricsRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.reportMetrics(
        prefix, namespace, table, reportMetricsRequest, realmContext, securityContext);
  }

  @Override
  public Response sendNotification(
      String prefix,
      String namespace,
      String table,
      NotificationRequest notificationRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeSendNotification(
        new BeforeSendNotificationEvent(catalogName, namespaceObj, table, notificationRequest));
    Response resp =
        delegate.sendNotification(
            prefix, namespace, table, notificationRequest, realmContext, securityContext);
    polarisEventListener.onAfterSendNotification(
        new AfterSendNotificationEvent(catalogName, namespaceObj, table));
    return resp;
  }
}
