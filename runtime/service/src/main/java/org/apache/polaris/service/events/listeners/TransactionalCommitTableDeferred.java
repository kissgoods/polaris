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
 * Marker interface for {@link PolarisEventListener} implementations that want
 * {@code onAfterCommitTable} events fired during a multi-table {@code commitTransaction} to be
 * <b>deferred until the transaction's atomic metastore commit succeeds</b>.
 *
 * <p><b>Why this exists</b>: Polaris's {@code IcebergCatalogHandler.commitTransaction} fires
 * {@code onAfterCommitTable} per table as it queues each table's metadata into a transaction
 * workspace, then runs a final {@code updateEntitiesPropertiesIfNotChanged} that may still
 * fail. Listeners that forward commit events downstream (e.g. event-rest-forwarder) must not
 * publish state for a transaction Polaris rolled back — otherwise the receiver accepts metadata
 * that Polaris itself never persisted, with no compensating event to undo it.
 *
 * <p><b>How listeners use it</b>: implement this marker and the two lifecycle methods. The
 * delegator calls {@link #beginTransaction()} before invoking {@code delegate.commitTransaction},
 * then either {@link #endTransaction(boolean)} with {@code true} if the delegate returned
 * successfully or {@link #endTransaction(boolean)} with {@code false} if the delegate threw.
 * Between {@code beginTransaction} and {@code endTransaction}, the listener's {@code
 * onAfterCommitTable} handler is expected to buffer events (typically per-thread) rather than
 * post them; on {@code endTransaction(true)} the listener replays the buffer, on
 * {@code endTransaction(false)} it discards.
 *
 * <p>Listeners that do not implement this marker (e.g. {@code no-op}, {@code
 * persistence-in-memory-buffer}, {@code aws-cloudwatch}) keep the legacy behavior — commit
 * events fire eagerly per table. Marker-aware deferral is a per-listener opt-in so that a
 * listener with no downstream consistency concerns does not pay any cost.
 */
public interface TransactionalCommitTableDeferred {

  /** Called by the delegator immediately before {@code delegate.commitTransaction(...)} runs. */
  void beginTransaction();

  /**
   * Called by the delegator after {@code delegate.commitTransaction(...)} returns or throws.
   *
   * @param committed {@code true} when the underlying transaction's atomic commit succeeded;
   *     {@code false} when it failed (the listener should discard any buffered events for this
   *     transaction).
   */
  void endTransaction(boolean committed);
}
