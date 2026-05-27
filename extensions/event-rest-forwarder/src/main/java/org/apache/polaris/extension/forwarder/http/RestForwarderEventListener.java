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
package org.apache.polaris.extension.forwarder.http;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.extension.forwarder.AbstractEventForwarderListener;
import org.apache.polaris.extension.forwarder.AuditContext;
import org.apache.polaris.extension.forwarder.EventSerializer;
import org.apache.polaris.extension.forwarder.RestForwarderConfiguration;

/**
 * Generic Polaris event forwarder that POSTs raw event envelopes to a user-configured REST
 * receiver. Selected by setting {@code polaris.event-listener.type=rest-forwarder}. Adds no new
 * runtime dependencies beyond what Polaris already ships with.
 *
 * <p>The receiver is responsible for any backend-specific translation (DataHub URN/aspect
 * building, Kafka topic routing, Atlas type mapping, etc.). This listener stays domain-agnostic
 * so a single Polaris deployment can fan-out to multiple downstream catalogs by switching the
 * receiver URL.
 */
@ApplicationScoped
@Identifier("rest-forwarder")
public class RestForwarderEventListener extends AbstractEventForwarderListener {

  // Injected lazily — these are request-scoped, so a CDI proxy is fine. Falling back to
  // System.currentTimeMillis / null principal / null realm preserves operation when no JAX-RS
  // request is active (e.g. background tasks, tests).
  @Inject Clock clock;
  @Context SecurityContext securityContext;
  @Inject RealmContext realmContext;

  @Inject
  public RestForwarderEventListener(RestForwarderConfiguration config) {
    super(new HttpEventPoster(config), new EventSerializer());
  }

  /** Required by Quarkus/Arc proxy generation; never invoked for the live bean. */
  protected RestForwarderEventListener() {
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
    // Realm identifier feeds both the envelope's `realm` field and the resourceKey prefix so
    // same-named catalogs in different realms can't collide downstream. Same-care exception
    // handling as SecurityContext: a request-scoped proxy may throw outside its scope.
    String realmId = null;
    try {
      if (realmContext != null) {
        realmId = realmContext.getRealmIdentifier();
      }
    } catch (Exception ignored) {
      // Fall back to null realm; the resourceKey collapses the realm segment to an empty prefix,
      // matching the legacy single-realm shape.
    }
    return new AuditContext(ts, principal, realmId);
  }
}
