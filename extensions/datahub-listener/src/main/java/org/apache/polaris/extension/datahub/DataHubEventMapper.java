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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Builds DataHub URNs and aspect payloads from Polaris event data. */
public class DataHubEventMapper {

  /** Iceberg REST encodes nested namespace levels with this unit separator. */
  static final char NS_SEPARATOR = '';

  public static final String PLATFORM = "iceberg";
  public static final String DATASET = "dataset";
  public static final String CONTAINER = "container";
  public static final String DOMAIN = "domain";

  // ── Polaris catalog classification rules (3-tier) ─────────────────────────────
  //
  // Every catalog/namespace/dataset emit gets exactly one domain attached, decided by:
  //   1. catalog name matches USER_CATALOG_PATTERN (my_<alnum>_catalog)
  //      → child Domain urn:li:domain:<catalogName> (parent: user_catalog)
  //      → owner = corpuser:<captured middle segment between the underscores>
  //   2. catalog name == "lake_catalog"
  //      → flat Domain urn:li:domain:lake_catalog_polaris (no parent)
  //      → owner = corpuser:datalake (fixed)
  //   3. else (everything else, including catalogs with explicit domain-mapping)
  //      → flat Domain urn:li:domain:etc_polaris (no parent)
  //      → owner = corpuser:etc (fixed — same role as Tier 2's datalake placeholder)
  //
  // The listener idempotently emits the corresponding Domain entity itself on every relevant
  // upsert so DataHub's /domains UI never has a dangling reference. user_catalog parent must be
  // pre-created once (the only manual step); everything else is automatic.

  // Rule 1 — capture group is the WHOLE middle segment between the two underscores. The owner
  // corpuser id is that value verbatim (e.g. my_x01100_catalog → owner x01100, my_42_catalog →
  // owner 42). Alphanumeric only — `_`, `-`, `.` inside the middle segment cause non-match.
  private static final Pattern USER_CATALOG_PATTERN =
      Pattern.compile("^my_([a-zA-Z0-9]+)_catalog$");
  static final String USER_CATALOG_PARENT_DOMAIN_URN = "urn:li:domain:user_catalog";
  private static final String USER_CATALOG_OWNER_TYPE = "TECHNICAL_OWNER";

  // Rule 2
  private static final String LAKE_CATALOG_NAME = "lake_catalog";
  public static final String LAKE_CATALOG_DOMAIN_URN = "urn:li:domain:lake_catalog_polaris";
  private static final String LAKE_CATALOG_OWNER_ID = "datalake";

  // Rule 3 (fallback)
  public static final String ETC_DOMAIN_URN = "urn:li:domain:etc_polaris";
  private static final String ETC_OWNER_ID = "etc";

  private final DataHubConfiguration config;

  public DataHubEventMapper(DataHubConfiguration config) {
    this.config = config;
  }

  /** Audit metadata (timestamp + principal) attached to created/lastModified on emitted aspects. */
  public record AuditContext(long timestampMs, String principal) {
    public static AuditContext now(String principal) {
      return new AuditContext(System.currentTimeMillis(), principal);
    }
  }

  public String datasetUrn(String catalogName, Namespace namespace, String tableName) {
    String qualifiedName = qualifiedName(catalogName, namespace, tableName);
    return "urn:li:dataset:(urn:li:dataPlatform:"
        + PLATFORM
        + ","
        + qualifiedName
        + ","
        + config.env()
        + ")";
  }

  public String catalogContainerUrn(String catalogName) {
    return "urn:li:container:"
        + PLATFORM
        + "."
        + config.platformInstance()
        + "."
        + escapeSegment(catalogName);
  }

  public String namespaceContainerUrn(String catalogName, Namespace namespace) {
    if (namespace == null || namespace.isEmpty()) {
      return catalogContainerUrn(catalogName);
    }
    return catalogContainerUrn(catalogName) + "." + joinForUrn(namespace);
  }

  /**
   * Build a namespace URN from the raw namespace string carried by Iceberg REST events. Iceberg
   * REST encodes nested levels with {@link #NS_SEPARATOR}; we split on it and emit one
   * dot-separated, escaped segment per level so the result matches {@link
   * #namespaceContainerUrn(String, Namespace)} for the same logical namespace — including when a
   * level itself contains a literal {@code '.'}.
   */
  public String namespaceContainerUrnFromRaw(String catalogName, String rawNamespace) {
    if (rawNamespace == null || rawNamespace.isEmpty()) {
      return catalogContainerUrn(catalogName);
    }
    String joined =
        Arrays.stream(rawNamespace.split(String.valueOf(NS_SEPARATOR), -1))
            .map(DataHubEventMapper::escapeSegment)
            .collect(Collectors.joining("."));
    return catalogContainerUrn(catalogName) + "." + joined;
  }

  public String qualifiedName(String catalogName, Namespace namespace, String tableName) {
    String ns = joinForUrn(namespace);
    String cat = escapeSegment(catalogName);
    String tbl = escapeSegment(tableName);
    return ns.isEmpty() ? cat + "." + tbl : cat + "." + ns + "." + tbl;
  }

  public Map<String, Object> catalogContainerAspects(
      String catalogName, Map<String, String> properties) {
    Map<String, Object> aspects = new LinkedHashMap<>();
    aspects.put("containerProperties", containerProperties(catalogName, "Catalog", properties));
    aspects.put("subTypes", subTypes("Catalog"));
    aspects.put("dataPlatformInstance", dataPlatformInstance());
    addAutoDomainAspects(aspects, catalogName);
    return aspects;
  }

  /**
   * @param properties current namespace properties, or {@code null} when unknown (e.g. on a
   *     property-update event). When {@code null}, the {@code containerProperties} aspect is
   *     omitted so DataHub does not overwrite previously-synced custom properties.
   */
  public Map<String, Object> namespaceContainerAspects(
      String catalogName, Namespace namespace, Map<String, String> properties) {
    Map<String, Object> aspects = new LinkedHashMap<>();
    if (properties != null) {
      // Display name uses unescaped dotted form so the DataHub UI shows the human-readable
      // namespace ("a.b") rather than the URN-safe escaped form ("a%2Eb").
      aspects.put(
          "containerProperties",
          containerProperties(joinForDisplay(namespace), "Namespace", properties));
    }
    aspects.put("subTypes", subTypes("Namespace"));
    aspects.put("dataPlatformInstance", dataPlatformInstance());
    aspects.put("container", Map.of("container", parentContainerUrn(catalogName, namespace)));
    addAutoDomainAspects(aspects, catalogName);
    return aspects;
  }

  /**
   * Legacy minimal dataset aspect builder kept for backward compatibility with existing tests.
   * Prefer {@link #datasetAspects(String, Namespace, String, TableMetadata, AuditContext)} when
   * the full {@link TableMetadata} is available — it adds {@code schemaMetadata} plus enriched
   * {@code customProperties} (table-uuid, location, format-version, snapshot/spec ids, …) so the
   * DataHub UI shows the actual schema rather than a blank dataset page.
   */
  public Map<String, Object> datasetAspects(
      String catalogName,
      Namespace namespace,
      String tableName,
      Map<String, String> tableProperties) {
    return datasetAspectsInternal(
        catalogName, namespace, tableName, tableProperties, /*tableMetadata*/ null, /*audit*/ null);
  }

  /**
   * Full dataset aspect builder. When {@code tableMetadata} is non-null, also emits a {@code
   * schemaMetadata} aspect (Iceberg schema → DataHub SchemaField list) and folds Iceberg
   * top-level metadata fields ({@code table-uuid}, {@code location}, {@code format-version},
   * {@code current-snapshot-id}, …) into {@code datasetProperties.customProperties}. When {@code
   * audit} is non-null, fills {@code datasetProperties.lastModified} so DataHub UI shows who/when.
   */
  public Map<String, Object> datasetAspects(
      String catalogName,
      Namespace namespace,
      String tableName,
      TableMetadata tableMetadata,
      AuditContext audit) {
    Map<String, String> props =
        tableMetadata == null || tableMetadata.properties() == null
            ? null
            : tableMetadata.properties();
    return datasetAspectsInternal(catalogName, namespace, tableName, props, tableMetadata, audit);
  }

  private Map<String, Object> datasetAspectsInternal(
      String catalogName,
      Namespace namespace,
      String tableName,
      Map<String, String> tableProperties,
      TableMetadata tableMetadata,
      AuditContext audit) {
    Map<String, Object> aspects = new LinkedHashMap<>();
    Map<String, Object> datasetProps = new LinkedHashMap<>();
    datasetProps.put("name", tableName);
    datasetProps.put("qualifiedName", qualifiedName(catalogName, namespace, tableName));

    Map<String, String> customProps = enrichedTableProperties(tableMetadata, tableProperties);
    if (customProps != null) {
      datasetProps.put("customProperties", customProps);
    }
    if (audit != null) {
      Map<String, Object> stamp = auditStamp(audit.timestampMs(), audit.principal());
      datasetProps.put("lastModified", stamp);
    }
    aspects.put("datasetProperties", datasetProps);
    // Pin the DataHub subType to "Table" so the entity is always rendered as a Table in the UI
    // (without this aspect, DataHub falls back to the generic "Dataset" label on subsequent
    // upserts, even though the first upsert from the bootstrap path defaults to "Table").
    aspects.put("subTypes", subTypes("Table"));
    aspects.put("container", Map.of("container", namespaceContainerUrn(catalogName, namespace)));
    aspects.put("dataPlatformInstance", dataPlatformInstance());

    // 3-tier classification (Section 4.2): user_catalog > lake_catalog > etc_polaris.
    addAutoDomainAspects(aspects, catalogName);

    if (tableMetadata != null && tableMetadata.schema() != null) {
      aspects.put(
          "schemaMetadata",
          schemaMetadataAspect(
              tableMetadata.schema(), qualifiedName(catalogName, namespace, tableName)));
    }
    return aspects;
  }

  /**
   * Merge Iceberg {@link TableMetadata} top-level fields into the user-facing custom properties
   * map. Returns {@code null} only when both inputs are absent — that signals "do not emit any
   * {@code customProperties} field" so DataHub does not overwrite previously-synced state.
   */
  private Map<String, String> enrichedTableProperties(
      TableMetadata tableMetadata, Map<String, String> tableProperties) {
    if (tableMetadata == null && tableProperties == null) {
      return null;
    }
    Map<String, String> out = new LinkedHashMap<>();
    if (tableProperties != null) {
      out.putAll(tableProperties);
    }
    if (tableMetadata != null) {
      putIfPresent(out, "table-uuid", tableMetadata.uuid());
      putIfPresent(out, "location", tableMetadata.location());
      out.put("format-version", String.valueOf(tableMetadata.formatVersion()));
      out.put("current-schema-id", String.valueOf(tableMetadata.currentSchemaId()));
      out.put("last-updated-ms", String.valueOf(tableMetadata.lastUpdatedMillis()));
      out.put("last-sequence-number", String.valueOf(tableMetadata.lastSequenceNumber()));
      out.put("default-spec-id", String.valueOf(tableMetadata.defaultSpecId()));
      out.put("default-sort-order-id", String.valueOf(tableMetadata.defaultSortOrderId()));
      long currentSnapshot =
          tableMetadata.currentSnapshot() == null
              ? -1L
              : tableMetadata.currentSnapshot().snapshotId();
      out.put("current-snapshot-id", String.valueOf(currentSnapshot));
    }
    return out;
  }

  /**
   * Map an Iceberg {@link Schema} to a DataHub {@code schemaMetadata} aspect payload. Nested
   * struct fields are flattened to dotted-path entries; list/map element types are reflected via
   * the field's {@code nativeDataType} string (the Iceberg {@code toString()} form).
   */
  Map<String, Object> schemaMetadataAspect(Schema schema, String schemaName) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("schemaName", schemaName);
    out.put("platform", "urn:li:dataPlatform:" + PLATFORM);
    out.put("version", 0);
    out.put("hash", "");
    out.put("platformSchema", Map.of("com.linkedin.schema.Schemaless", Map.of()));

    List<Map<String, Object>> fields = new ArrayList<>();
    for (Types.NestedField f : schema.columns()) {
      flattenField(f, "", /*parentNullable*/ false, fields);
    }
    out.put("fields", fields);
    return out;
  }

  private void flattenField(
      Types.NestedField field,
      String pathPrefix,
      boolean parentNullable,
      List<Map<String, Object>> out) {
    String path = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
    boolean nullable = parentNullable || field.isOptional();
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("fieldPath", path);
    entry.put("nativeDataType", field.type().toString());
    entry.put("type", schemaFieldType(field.type()));
    entry.put("nullable", nullable);
    entry.put("description", field.doc() == null ? "" : field.doc());
    out.add(entry);

    if (field.type().isStructType()) {
      for (Types.NestedField child : field.type().asStructType().fields()) {
        flattenField(child, path, nullable, out);
      }
    }
  }

  /**
   * Map an Iceberg {@link Type} to a DataHub {@code SchemaFieldDataType} wrapper. Returns the
   * shape {@code { "type": { "com.linkedin.schema.<X>Type": {} } }} expected by the DataHub
   * OpenAPI v3 generic-entities endpoint.
   */
  static Map<String, Object> schemaFieldType(Type type) {
    String typeName;
    if (type instanceof Types.BooleanType) {
      typeName = "BooleanType";
    } else if (type instanceof Types.IntegerType
        || type instanceof Types.LongType
        || type instanceof Types.FloatType
        || type instanceof Types.DoubleType
        || type instanceof Types.DecimalType) {
      typeName = "NumberType";
    } else if (type instanceof Types.StringType || type instanceof Types.UUIDType) {
      typeName = "StringType";
    } else if (type instanceof Types.DateType) {
      typeName = "DateType";
    } else if (type instanceof Types.TimeType || type instanceof Types.TimestampType) {
      typeName = "TimeType";
    } else if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
      typeName = "BytesType";
    } else if (type instanceof Types.ListType) {
      typeName = "ArrayType";
    } else if (type instanceof Types.MapType) {
      typeName = "MapType";
    } else if (type instanceof Types.StructType) {
      typeName = "RecordType";
    } else {
      typeName = "NullType";
    }
    return Map.of("type", Map.of("com.linkedin.schema." + typeName, Map.of()));
  }

  /** Build a DataHub {@code AuditStamp} ({@code { time, actor }}). */
  static Map<String, Object> auditStamp(long timestampMs, String principal) {
    Map<String, Object> stamp = new LinkedHashMap<>();
    stamp.put("time", timestampMs);
    // DataHub requires a non-null actor URN even when the principal is unknown.
    String actor =
        (principal == null || principal.isEmpty())
            ? "urn:li:corpuser:__system__"
            : "urn:li:corpuser:" + principal;
    stamp.put("actor", actor);
    return stamp;
  }

  private static void putIfPresent(Map<String, String> map, String key, String value) {
    if (value != null && !value.isEmpty()) {
      map.put(key, value);
    }
  }

  private Map<String, Object> containerProperties(
      String name, String description, Map<String, String> custom) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("description", description);
    m.put("customProperties", custom == null ? Map.of() : custom);
    return m;
  }

  private Map<String, Object> subTypes(String typeName) {
    return Map.of("typeNames", List.of(typeName));
  }

  private Map<String, Object> dataPlatformInstance() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("platform", "urn:li:dataPlatform:" + PLATFORM);
    m.put(
        "instance",
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:"
            + PLATFORM
            + ","
            + config.platformInstance()
            + ")");
    return m;
  }

  /**
   * If {@code catalogName} matches the user-catalog convention {@code my_<alnum>_catalog}, return
   * the captured middle segment verbatim (e.g. {@code my_x01100_catalog} → {@code "x01100"},
   * {@code my_42_catalog} → {@code "42"}). Empty otherwise. Package-private so the listener can
   * use it to decide whether to emit the per-catalog child Domain entity.
   */
  static Optional<String> userCatalogOwnerId(String catalogName) {
    if (catalogName == null) return Optional.empty();
    Matcher m = USER_CATALOG_PATTERN.matcher(catalogName);
    return m.matches() ? Optional.of(m.group(1)) : Optional.empty();
  }

  /**
   * Child Domain URN for a pattern-matched catalog. DataHub renders this as
   * {@code user_catalog > <catalogName>} when the entity has {@code parentDomain} set.
   */
  public String userCatalogChildDomainUrn(String catalogName) {
    return "urn:li:domain:" + catalogName;
  }

  /**
   * {@code domainProperties} aspect for the per-catalog child Domain entity. {@code parentDomain}
   * links it under {@link #USER_CATALOG_PARENT_DOMAIN_URN}. Idempotent — emitting on every event
   * keeps DataHub state correct even for catalogs that pre-existed this feature.
   */
  public Map<String, Object> userCatalogChildDomainAspects(String catalogName) {
    Map<String, Object> aspects = new LinkedHashMap<>();
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("name", catalogName);
    props.put(
        "description",
        "Polaris catalog under user_catalog (matched pattern my_[letters]<digits>_catalog).");
    props.put("parentDomain", USER_CATALOG_PARENT_DOMAIN_URN);
    aspects.put("domainProperties", props);
    return aspects;
  }

  /** Whether the catalog name should trigger user_catalog pattern aspects + child Domain emit. */
  public boolean isUserCatalog(String catalogName) {
    return userCatalogOwnerId(catalogName).isPresent();
  }

  /** Whether the catalog is the special {@code lake_catalog}. */
  public boolean isLakeCatalog(String catalogName) {
    return LAKE_CATALOG_NAME.equals(catalogName);
  }

  /**
   * {@code domainProperties} aspect for the shared {@code lake_catalog_polaris} top-level Domain
   * entity. Flat — no {@code parentDomain}. listener auto-emits on every {@code lake_catalog} upsert
   * (idempotent), so no manual pre-creation step.
   */
  public Map<String, Object> lakeCatalogDomainAspects() {
    Map<String, Object> aspects = new LinkedHashMap<>();
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("name", "lake_catalog_polaris");
    props.put("description", "Polaris datalake catalogs.");
    aspects.put("domainProperties", props);
    return aspects;
  }

  /**
   * {@code domainProperties} aspect for the shared {@code etc_polaris} top-level Domain entity —
   * default bucket for catalogs that don't match user_catalog or lake_catalog rules. Flat — no
   * {@code parentDomain}. listener auto-emits on every fallback upsert (idempotent).
   */
  public Map<String, Object> etcDomainAspects() {
    Map<String, Object> aspects = new LinkedHashMap<>();
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("name", "etc_polaris");
    props.put(
        "description",
        "Polaris catalogs that don't match user_catalog (my_<id>_catalog) or lake_catalog rules.");
    aspects.put("domainProperties", props);
    return aspects;
  }

  private static Map<String, Object> ownershipPayload(String ownerId) {
    return Map.of(
        "owners",
        List.of(
            Map.of("owner", "urn:li:corpuser:" + ownerId, "type", USER_CATALOG_OWNER_TYPE)));
  }

  /**
   * Apply the 3-tier catalog classification: attach {@code domains} (and {@code ownership} when
   * applicable) to the given aspects map based on {@code catalogName}.
   *
   * <ol>
   *   <li>user_catalog pattern → child Domain URN + corpuser:{middle segment} ownership
   *   <li>{@code lake_catalog} → lake_catalog_polaris + corpuser:datalake ownership
   *   <li>else → etc_polaris + corpuser:etc ownership
   * </ol>
   *
   * Always populates {@code domains} and {@code ownership}.
   */
  private void addAutoDomainAspects(Map<String, Object> aspects, String catalogName) {
    Optional<String> userCatalogOwnerId = userCatalogOwnerId(catalogName);
    if (userCatalogOwnerId.isPresent()) {
      aspects.put(
          "domains", Map.of("domains", List.of(userCatalogChildDomainUrn(catalogName))));
      aspects.put("ownership", ownershipPayload(userCatalogOwnerId.get()));
      return;
    }
    if (isLakeCatalog(catalogName)) {
      aspects.put("domains", Map.of("domains", List.of(LAKE_CATALOG_DOMAIN_URN)));
      aspects.put("ownership", ownershipPayload(LAKE_CATALOG_OWNER_ID));
      return;
    }
    aspects.put("domains", Map.of("domains", List.of(ETC_DOMAIN_URN)));
    aspects.put("ownership", ownershipPayload(ETC_OWNER_ID));
  }

  private String parentContainerUrn(String catalogName, Namespace namespace) {
    if (namespace == null || namespace.length() <= 1) {
      return catalogContainerUrn(catalogName);
    }
    String[] levels = namespace.levels();
    Namespace parent = Namespace.of(Arrays.copyOf(levels, levels.length - 1));
    return namespaceContainerUrn(catalogName, parent);
  }

  /**
   * Join namespace levels for URN construction. Each level is {@link #escapeSegment escaped} so
   * that a literal {@code '.'} inside a level cannot be confused with the segment separator.
   * Without this, {@code Namespace.of("a.b")} and {@code Namespace.of("a", "b")} would collide on
   * the same URN.
   */
  static String joinForUrn(Namespace namespace) {
    if (namespace == null || namespace.isEmpty()) {
      return "";
    }
    return Arrays.stream(namespace.levels())
        .map(DataHubEventMapper::escapeSegment)
        .collect(Collectors.joining("."));
  }

  /**
   * Join namespace levels for human-readable display (e.g. the DataHub UI {@code name} field). No
   * escaping — the caller is presenting a label, not a URN.
   */
  static String joinForDisplay(Namespace namespace) {
    if (namespace == null || namespace.isEmpty()) {
      return "";
    }
    return String.join(".", namespace.levels());
  }

  /**
   * Percent-escape characters that are reserved as separators in the URN forms we emit:
   *
   * <ul>
   *   <li>{@code '.'} — container URN segment separator ({@code iceberg.polaris.cat.ns})
   *   <li>{@code ','} {@code '('} {@code ')'} — dataset URN tuple delimiters
   *       ({@code urn:li:dataset:(urn:li:dataPlatform:iceberg,<qualifiedName>,<env>)}) — a literal
   *       {@code ','} or {@code ')'} inside a catalog/namespace/table name would otherwise break
   *       the tuple structure and cause DataHub to reject or misparse the URN
   * </ul>
   *
   * {@code '%'} is escaped first so the encoding round-trips: an input {@code "a%2Eb"} becomes
   * {@code "a%252Eb"} and cannot be confused with the escaped form of {@code "a.b"}.
   */
  static String escapeSegment(String segment) {
    if (segment == null || segment.isEmpty()) {
      return segment == null ? "" : segment;
    }
    return segment
        .replace("%", "%25")
        .replace(".", "%2E")
        .replace(",", "%2C")
        .replace("(", "%28")
        .replace(")", "%29");
  }
}
