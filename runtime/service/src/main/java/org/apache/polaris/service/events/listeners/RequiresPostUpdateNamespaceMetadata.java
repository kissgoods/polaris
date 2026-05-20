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
package org.apache.polaris.service.events.listeners;

/**
 * Marker interface for {@link PolarisEventListener} implementations whose {@code
 * onAfterUpdateNamespaceProperties} handler needs the namespace's full current properties after
 * the update.
 *
 * <p>The Iceberg REST {@code updateProperties} response only carries the lists of updated /
 * removed / missing keys, not the resulting state. Without a follow-up load, a downstream sink
 * (e.g. DataHub) cannot send the up-to-date {@code customProperties} for that namespace and
 * eventually goes stale. To populate {@code AfterUpdateNamespacePropertiesEvent.currentProperties()}
 * the delegator must perform an extra {@code loadNamespaceMetadata} after the update succeeds —
 * a cost most listeners do not need. Listeners opt in by implementing this marker; everyone else
 * (including the default {@code no-op} listener) gets a {@code null} payload and avoids the
 * extra metadata read.
 */
public interface RequiresPostUpdateNamespaceMetadata {}
