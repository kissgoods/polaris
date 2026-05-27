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

/**
 * Per-emit audit metadata: the wall-clock timestamp captured at handler entry, the calling
 * principal's name (from {@code SecurityContext.getUserPrincipal()}), and the Polaris realm
 * identifier the request was routed to (from {@code RealmContext.getRealmIdentifier()}).
 *
 * <p>{@code principal} is {@code null} when the listener runs outside a JAX-RS request scope
 * (background tasks, tests) or when the caller is unauthenticated. {@code realmId} is {@code
 * null} when the request was not routed through a realm-aware path (also single-realm
 * deployments where the deployment doesn't configure a discriminator).
 *
 * <p>The realm is part of the audit context — not just a transport-layer header — because it is
 * load-bearing for downstream identity: two Polaris realms can host catalogs with the same name
 * but completely different contents, so the receiver's dedup / ordering keys must include the
 * realm to avoid collapsing events across tenants. {@link EventEnvelope#resourceKey()} prepends
 * the realm precisely for that reason.
 */
public record AuditContext(long timestampMs, String principal, String realmId) {
  /** Convenience: wall-clock now with the given principal (nullable). Realm defaults to null. */
  public static AuditContext now(String principal) {
    return new AuditContext(System.currentTimeMillis(), principal, null);
  }
}
