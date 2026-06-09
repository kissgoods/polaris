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

import java.util.Map;

/**
 * Transport-agnostic abstraction for writing entity aspects to DataHub.
 *
 * <p>The interface is kept transport-agnostic so additional implementations can be plugged in
 * later, but the bundled implementation is HTTP-only ({@code HttpEmitter}).
 */
public interface DataHubEmitter extends AutoCloseable {

  /**
   * Upsert one or more aspects on a DataHub entity.
   *
   * @param urn target entity URN, e.g. {@code urn:li:dataset:(urn:li:dataPlatform:iceberg,...)}
   * @param entityType DataHub entity type name: {@code dataset}, {@code container}, ...
   * @param aspects map from aspect name (e.g. {@code datasetProperties}) to its value object
   */
  void emitUpsert(String urn, String entityType, Map<String, Object> aspects);

  /** Soft-delete an entity by setting the {@code status} aspect's {@code removed} flag. */
  void emitStatusRemoved(String urn, String entityType);

  @Override
  default void close() {}
}
