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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import org.apache.polaris.extension.datahub.AbstractDataHubEventListener;
import org.apache.polaris.extension.datahub.DataHubConfiguration;
import org.apache.polaris.extension.datahub.DataHubEventMapper;
import org.apache.polaris.extension.datahub.DataHubEventMapper.AuditContext;

/**
 * DataHub event listener that emits via plain HTTP POST against the DataHub OpenAPI v3 endpoints.
 * Selected by setting {@code polaris.event-listener.type=datahub-http}. Adds no new runtime
 * dependencies beyond what Polaris already ships with.
 */
@ApplicationScoped
@Identifier("datahub-http")
public class DataHubHttpEventListener extends AbstractDataHubEventListener {

  // Injected lazily — these are request-scoped, so a CDI proxy is fine. Falling back to
  // System.currentTimeMillis / null principal preserves operation when no JAX-RS request is
  // active (e.g. background tasks, tests).
  @Inject Clock clock;
  @Context SecurityContext securityContext;

  @Inject
  public DataHubHttpEventListener(DataHubConfiguration config) {
    super(new HttpEmitter(config), new DataHubEventMapper(config));
  }

  /** Required by Quarkus/Arc proxy generation; never invoked for the live bean. */
  protected DataHubHttpEventListener() {
    super();
  }

  @Override
  protected AuditContext getAuditContext() {
    long ts = clock != null ? clock.millis() : System.currentTimeMillis();
    String principal = null;
    try {
      if (securityContext != null) {
        Principal p = securityContext.getUserPrincipal();
        principal = p == null ? null : p.getName();
      }
    } catch (Exception ignored) {
      // SecurityContext can throw when accessed outside a request scope — fall back to anonymous.
    }
    return new AuditContext(ts, principal);
  }
}
