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

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class DataHubEventMapperTest {

  private final DataHubConfiguration cfg =
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
      };

  private final DataHubEventMapper mapper = new DataHubEventMapper(cfg);

  private DataHubEventMapper mapperWithDomainMapping(Map<String, String> mapping) {
    DataHubConfiguration custom =
        new DataHubConfiguration() {
          @Override
          public Optional<String> gmsUrl() {
            return cfg.gmsUrl();
          }

          @Override
          public Optional<String> token() {
            return cfg.token();
          }

          @Override
          public String platformInstance() {
            return cfg.platformInstance();
          }

          @Override
          public String env() {
            return cfg.env();
          }

          @Override
          public boolean synchronousMode() {
            return cfg.synchronousMode();
          }

          @Override
          public Duration connectTimeout() {
            return cfg.connectTimeout();
          }

          @Override
          public Duration requestTimeout() {
            return cfg.requestTimeout();
          }

          @Override
          public int maxInflightEmits() {
            return cfg.maxInflightEmits();
          }

          @Override
          public int circuitBreakerFailureThreshold() {
            return cfg.circuitBreakerFailureThreshold();
          }

          @Override
          public Duration circuitBreakerOpenDuration() {
            return cfg.circuitBreakerOpenDuration();
          }

          @Override
          public int replayBufferCapacity() {
            return cfg.replayBufferCapacity();
          }

          @Override
          public Map<String, String> domainMapping() {
            return mapping;
          }
        };
    return new DataHubEventMapper(custom);
  }

  /**
   * Verifies that String.join("", levels) produces the correct input for
   * CatalogAdapter.decodeNamespace(), which does:
   * RESTUtil.decodeNamespace(URLEncoder.encode(namespace, defaultCharset))
   *
   * <p>Raw U+001F → URLEncoder → "%1F" → RESTUtil.decodeNamespace splits correctly.
   * RESTUtil.encodeNamespace() would produce "%1F" literals → URLEncoder double-encodes to "%251F"
   * → RESTUtil.decodeNamespace sees no separator → wrong single-level namespace.
   */
  @Test
  void unitSeparatorJoinRoundTripsViaURLEncoderForNestedNamespace() throws Exception {
    String rawJoin = String.join("", new String[] {"ns", "sub"});

    // CatalogAdapter.decodeNamespace step 1: URLEncoder.encode(rawJoin)
    String afterEncoder = URLEncoder.encode(rawJoin, Charset.defaultCharset());
    // must produce "%1F"-separated form so RESTUtil.decodeNamespace can split on it
    assertThat(afterEncoder).isEqualTo("ns%1Fsub");

    // If RESTUtil.encodeNamespace() were used instead, URLEncoder double-encodes "%" → "%25"
    String percentEncoded =
        "ns%1Fsub"; // what RESTUtil.encodeNamespace(Namespace.of("ns","sub")) returns
    String doubleEncoded = URLEncoder.encode(percentEncoded, Charset.defaultCharset());
    assertThat(doubleEncoded).isEqualTo("ns%251Fsub"); // not "ns%1Fsub" — decoding would fail
  }

  @Test
  void datasetUrnUsesDottedQualifiedName() {
    String urn = mapper.datasetUrn("cat", Namespace.of("ns", "sub"), "tbl");
    assertThat(urn).isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.sub.tbl,PROD)");
  }

  @Test
  void datasetUrnWithEmptyNamespaceOmitsExtraDot() {
    String urn = mapper.datasetUrn("cat", Namespace.empty(), "tbl");
    assertThat(urn).isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.tbl,PROD)");
  }

  @Test
  void catalogContainerUrnIncludesPlatformInstance() {
    assertThat(mapper.catalogContainerUrn("cat")).isEqualTo("urn:li:container:iceberg.polaris.cat");
  }

  @Test
  void namespaceContainerUrnFromRawNormalizesUnitSeparator() {
    String raw = "ns" + DataHubEventMapper.NS_SEPARATOR + "sub";
    assertThat(mapper.namespaceContainerUrnFromRaw("cat", raw))
        .isEqualTo(mapper.namespaceContainerUrn("cat", Namespace.of("ns", "sub")));
  }

  @Test
  void datasetAspectsCarryQualifiedNameAndContainerLink() {
    Map<String, Object> aspects =
        mapper.datasetAspects("cat", Namespace.of("ns"), "tbl", Map.of("k", "v"));

    assertThat(aspects).containsKeys("datasetProperties", "container", "dataPlatformInstance");

    @SuppressWarnings("unchecked")
    Map<String, Object> datasetProps = (Map<String, Object>) aspects.get("datasetProperties");
    assertThat(datasetProps).containsEntry("name", "tbl");
    assertThat(datasetProps).containsEntry("qualifiedName", "cat.ns.tbl");
    assertThat(datasetProps.get("customProperties")).isEqualTo(Map.of("k", "v"));

    @SuppressWarnings("unchecked")
    Map<String, Object> container = (Map<String, Object>) aspects.get("container");
    assertThat(container).containsEntry("container", "urn:li:container:iceberg.polaris.cat.ns");
  }

  @Test
  void datasetAspectsWithEmptyNamespaceContainerLinksToCatalog() {
    Map<String, Object> aspects = mapper.datasetAspects("cat", Namespace.empty(), "tbl", Map.of());

    @SuppressWarnings("unchecked")
    Map<String, Object> container = (Map<String, Object>) aspects.get("container");
    assertThat(container).containsEntry("container", "urn:li:container:iceberg.polaris.cat");
  }

  @Test
  void catalogContainerAspectsTagsTypeAsCatalog() {
    Map<String, Object> aspects = mapper.catalogContainerAspects("cat", Map.of());

    @SuppressWarnings("unchecked")
    Map<String, Object> subTypes = (Map<String, Object>) aspects.get("subTypes");
    assertThat(subTypes.get("typeNames")).isEqualTo(java.util.List.of("Catalog"));
  }

  @Test
  void namespaceContainerAspectsTagsTypeAsNamespaceWithCatalogParent() {
    Map<String, Object> aspects =
        mapper.namespaceContainerAspects("cat", Namespace.of("ns"), Map.of());

    @SuppressWarnings("unchecked")
    Map<String, Object> subTypes = (Map<String, Object>) aspects.get("subTypes");
    assertThat(subTypes.get("typeNames")).isEqualTo(java.util.List.of("Namespace"));

    @SuppressWarnings("unchecked")
    Map<String, Object> containerProps =
        (Map<String, Object>) aspects.get("containerProperties");
    assertThat(containerProps).containsEntry("description", "Namespace"); // not "Schema"

    @SuppressWarnings("unchecked")
    Map<String, Object> container = (Map<String, Object>) aspects.get("container");
    assertThat(container).containsEntry("container", "urn:li:container:iceberg.polaris.cat");
  }

  @Test
  void nestedNamespaceContainerAspectsPointsToParentNamespace() {
    Map<String, Object> aspects =
        mapper.namespaceContainerAspects("cat", Namespace.of("ns", "sub"), Map.of());

    @SuppressWarnings("unchecked")
    Map<String, Object> container = (Map<String, Object>) aspects.get("container");
    assertThat(container).containsEntry("container", "urn:li:container:iceberg.polaris.cat.ns");
  }

  @Test
  @SuppressWarnings("unchecked")
  void datasetAspectsPinsSubTypeToTable() {
    // DataHub UI 가 dataset 을 항상 "Table" 로 렌더링하도록 subTypes("Table") 을 명시 부착.
    // 이게 없으면 첫 emit 은 Table 로 표시되지만 후속 emit 부터 generic "Dataset" 으로 보임.
    Map<String, Object> aspects =
        mapper.datasetAspects("cat", Namespace.of("ns"), "tbl", Map.of("k", "v"));
    Map<String, Object> subTypes = (Map<String, Object>) aspects.get("subTypes");
    assertThat(subTypes).isNotNull();
    assertThat(subTypes.get("typeNames")).isEqualTo(java.util.List.of("Table"));
  }

  @Test
  void datasetAspectsWithNullPropertiesOmitsCustomProperties() {
    Map<String, Object> aspects = mapper.datasetAspects("cat", Namespace.of("ns"), "tbl", null);

    @SuppressWarnings("unchecked")
    Map<String, Object> datasetProps = (Map<String, Object>) aspects.get("datasetProperties");
    assertThat(datasetProps).doesNotContainKey("customProperties");
    assertThat(datasetProps).containsKeys("name", "qualifiedName");
    assertThat(aspects).containsKeys("container", "dataPlatformInstance");
  }

  @Test
  void namespaceContainerAspectsWithNullPropertiesOmitsContainerProperties() {
    Map<String, Object> aspects = mapper.namespaceContainerAspects("cat", Namespace.of("ns"), null);

    assertThat(aspects).doesNotContainKey("containerProperties");
    assertThat(aspects).containsKeys("subTypes", "dataPlatformInstance", "container");
  }

  /**
   * Regression for P2-2: a literal {@code '.'} in any segment of the URN (catalog name, namespace
   * level, table name) must be percent-escaped so two structurally-different inputs cannot collide
   * on the same URN. Without escaping, {@code Namespace.of("a.b")} and {@code Namespace.of("a",
   * "b")} would both emit {@code iceberg.polaris.cat.a.b}.
   */
  @Test
  void dotInNamespaceLevelIsEscapedSoFlatAndNestedDoNotCollide() {
    String flat = mapper.namespaceContainerUrn("cat", Namespace.of("a.b"));
    String nested = mapper.namespaceContainerUrn("cat", Namespace.of("a", "b"));
    assertThat(flat).isEqualTo("urn:li:container:iceberg.polaris.cat.a%2Eb");
    assertThat(nested).isEqualTo("urn:li:container:iceberg.polaris.cat.a.b");
    assertThat(flat).isNotEqualTo(nested);
  }

  @Test
  void dotInCatalogNameIsEscaped() {
    assertThat(mapper.catalogContainerUrn("team.a"))
        .isEqualTo("urn:li:container:iceberg.polaris.team%2Ea");
  }

  @Test
  void dotInTableNameIsEscapedInDatasetUrn() {
    String urn = mapper.datasetUrn("cat", Namespace.of("ns"), "t.bl");
    assertThat(urn)
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.t%2Ebl,PROD)");
  }

  @Test
  void percentInSegmentIsEscapedBeforeDotToKeepEncodingReversible() {
    // "a%2Eb" as a LITERAL level (not the escaped form of "a.b") must NOT collide with "a.b".
    String literal = mapper.namespaceContainerUrn("cat", Namespace.of("a%2Eb"));
    String dotted = mapper.namespaceContainerUrn("cat", Namespace.of("a.b"));
    assertThat(literal).isEqualTo("urn:li:container:iceberg.polaris.cat.a%252Eb");
    assertThat(dotted).isEqualTo("urn:li:container:iceberg.polaris.cat.a%2Eb");
    assertThat(literal).isNotEqualTo(dotted);
  }

  @Test
  void namespaceContainerUrnFromRawPreservesEscapeWhenLevelContainsDot() {
    // Raw form from Iceberg REST: two levels separated by the unit separator, the first of which
    // contains a literal '.'. Must round-trip to the same URN as namespaceContainerUrn(Namespace).
    String raw = "a.b" + DataHubEventMapper.NS_SEPARATOR + "c";
    assertThat(mapper.namespaceContainerUrnFromRaw("cat", raw))
        .isEqualTo(mapper.namespaceContainerUrn("cat", Namespace.of("a.b", "c")));
  }

  @Test
  void namespaceDisplayNameUsesUnescapedFormForUiReadability() {
    Map<String, Object> aspects =
        mapper.namespaceContainerAspects("cat", Namespace.of("a.b"), Map.of("k", "v"));

    @SuppressWarnings("unchecked")
    Map<String, Object> containerProps = (Map<String, Object>) aspects.get("containerProperties");
    // The URN gets escaped (verified above) but the human-facing "name" must stay readable.
    assertThat(containerProps).containsEntry("name", "a.b");
  }

  /**
   * The dataset URN tuple format {@code urn:li:dataset:(urn:li:dataPlatform:iceberg,<qualifiedName>,<env>)}
   * uses {@code ','} and parentheses as structural delimiters. A literal {@code ','} or {@code ')'} in
   * a catalog/namespace/table name would otherwise break the tuple and cause DataHub to reject the URN.
   */
  @Test
  void commaAndParenthesesInSegmentsAreEscapedToProtectDatasetUrnTuple() {
    String urn = mapper.datasetUrn("cat", Namespace.of("ns"), "t,1");
    assertThat(urn)
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.t%2C1,PROD)");

    String urnParen = mapper.datasetUrn("cat", Namespace.of("a)b"), "tbl");
    assertThat(urnParen)
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.a%29b.tbl,PROD)");

    String urnOpenParen = mapper.datasetUrn("c(at", Namespace.of("ns"), "tbl");
    assertThat(urnOpenParen)
        .isEqualTo("urn:li:dataset:(urn:li:dataPlatform:iceberg,c%28at.ns.tbl,PROD)");
  }

  // ============================================================================================
  // Rich metadata: schemaMetadata aspect, enriched customProperties, audit stamps.
  // ============================================================================================

  /** Sample table metadata with mixed primitive + nested struct fields. */
  private TableMetadata sampleMetadata() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get(), "primary key"),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()),
            Types.NestedField.optional(4, "active", Types.BooleanType.get()),
            Types.NestedField.optional(5, "created_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(
                6,
                "address",
                Types.StructType.of(
                    Types.NestedField.required(7, "city", Types.StringType.get()),
                    Types.NestedField.optional(8, "zip", Types.IntegerType.get()))));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        "s3://bucket/cat/ns/tbl",
        Map.of("owner", "team-x", "write.parquet.compression-codec", "zstd"));
  }

  @Test
  void datasetAspectsWithTableMetadataIncludesSchemaMetadataAspect() {
    Map<String, Object> aspects =
        mapper.datasetAspects(
            "cat", Namespace.of("ns"), "tbl", sampleMetadata(), /*audit*/ null);
    assertThat(aspects).containsKeys("schemaMetadata", "datasetProperties", "container");

    @SuppressWarnings("unchecked")
    Map<String, Object> schema = (Map<String, Object>) aspects.get("schemaMetadata");
    assertThat(schema).containsEntry("schemaName", "cat.ns.tbl");
    assertThat(schema).containsEntry("platform", "urn:li:dataPlatform:iceberg");
  }

  @Test
  void schemaMetadataPrimitiveTypesMapToDataHubTypes() {
    Map<String, Object> schema =
        mapper.schemaMetadataAspect(sampleMetadata().schema(), "cat.ns.tbl");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");

    // Build a path -> field map for easy lookup
    Map<String, Map<String, Object>> byPath =
        fields.stream().collect(java.util.stream.Collectors.toMap(f -> (String) f.get("fieldPath"), f -> f));

    assertThat(typeName(byPath.get("id"))).isEqualTo("NumberType");
    assertThat(byPath.get("id").get("nullable")).isEqualTo(false);
    assertThat(byPath.get("id").get("description")).isEqualTo("primary key");

    assertThat(typeName(byPath.get("name"))).isEqualTo("StringType");
    assertThat(typeName(byPath.get("amount"))).isEqualTo("NumberType");
    assertThat(typeName(byPath.get("active"))).isEqualTo("BooleanType");
    assertThat(typeName(byPath.get("created_at"))).isEqualTo("TimeType");
    assertThat(byPath.get("amount").get("nullable")).isEqualTo(true);
  }

  @Test
  void schemaMetadataFlattensNestedStructFieldsWithDottedPaths() {
    Map<String, Object> schema =
        mapper.schemaMetadataAspect(sampleMetadata().schema(), "cat.ns.tbl");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");

    Map<String, Map<String, Object>> byPath =
        fields.stream().collect(java.util.stream.Collectors.toMap(f -> (String) f.get("fieldPath"), f -> f));

    // Parent struct is emitted as RecordType
    assertThat(typeName(byPath.get("address"))).isEqualTo("RecordType");
    // Children are flattened with dotted paths
    assertThat(typeName(byPath.get("address.city"))).isEqualTo("StringType");
    assertThat(typeName(byPath.get("address.zip"))).isEqualTo("NumberType");
    // Children inherit nullability from optional parent struct
    assertThat(byPath.get("address.city").get("nullable")).isEqualTo(true);
  }

  @Test
  void datasetCustomPropertiesIncludeIcebergMetadataHeaders() {
    Map<String, Object> aspects =
        mapper.datasetAspects(
            "cat", Namespace.of("ns"), "tbl", sampleMetadata(), /*audit*/ null);
    @SuppressWarnings("unchecked")
    Map<String, Object> datasetProps = (Map<String, Object>) aspects.get("datasetProperties");
    @SuppressWarnings("unchecked")
    Map<String, String> custom = (Map<String, String>) datasetProps.get("customProperties");

    // Original table properties preserved
    assertThat(custom).containsEntry("owner", "team-x");
    assertThat(custom).containsEntry("write.parquet.compression-codec", "zstd");
    // Iceberg metadata header fields added
    assertThat(custom).containsKey("table-uuid");
    assertThat(custom).containsEntry("location", "s3://bucket/cat/ns/tbl");
    assertThat(custom).containsEntry("format-version", "2");
    assertThat(custom).containsEntry("current-snapshot-id", "-1");
    assertThat(custom).containsKey("last-updated-ms");
    assertThat(custom).containsKey("default-spec-id");
  }

  @Test
  void datasetAspectsWithAuditContextEmitsLastModifiedActorAndTime() {
    Map<String, Object> aspects =
        mapper.datasetAspects(
            "cat",
            Namespace.of("ns"),
            "tbl",
            sampleMetadata(),
            new DataHubEventMapper.AuditContext(1_700_000_000_000L, "root"));
    @SuppressWarnings("unchecked")
    Map<String, Object> datasetProps = (Map<String, Object>) aspects.get("datasetProperties");
    @SuppressWarnings("unchecked")
    Map<String, Object> lastModified = (Map<String, Object>) datasetProps.get("lastModified");
    assertThat(lastModified).containsEntry("time", 1_700_000_000_000L);
    assertThat(lastModified).containsEntry("actor", "urn:li:corpuser:root");
  }

  @Test
  void auditStampWithNullPrincipalUsesSystemActor() {
    Map<String, Object> stamp = DataHubEventMapper.auditStamp(42L, null);
    assertThat(stamp).containsEntry("time", 42L);
    assertThat(stamp).containsEntry("actor", "urn:li:corpuser:__system__");
  }

  @Test
  void legacyFourArgDatasetAspectsStillProducesNoSchemaAspect() {
    // Backward-compat: callers that don't pass TableMetadata must not get a schemaMetadata aspect
    // (and existing behavior — qualifiedName + container link — is preserved).
    Map<String, Object> aspects =
        mapper.datasetAspects("cat", Namespace.of("ns"), "tbl", Map.of("k", "v"));
    assertThat(aspects).doesNotContainKey("schemaMetadata");
    assertThat(aspects).containsKey("datasetProperties");
  }

  // explicit domain-mapping 기반 테스트들은 3-tier 분류 (user_catalog / lake_catalog / etc) 로
  // 대체됨. config.domainMapping() 메서드는 API 호환성을 위해 interface 에 남아있으나 listener
  // 가 더 이상 consult 하지 않음.

  // 기존 `containerAspectsOmitDomainsForExplicitMappingOnly` 테스트는 3-tier 분류 (etc_polaris
  // 기본 적용) 로 의미가 사라져 제거. container 의 domain 부착은 아래 lake/etc/user_catalog
  // 테스트로 커버.

  // ─────────────────────────────────────────────────────────────────────────────
  // user_catalog 패턴: catalog 이름이 my_<alnum>_catalog 면
  //   (a) 자식 Domain URN urn:li:domain:<catalog> 가 catalog/namespace/dataset 의 domains 에 부착
  //       (자식 Domain 엔티티는 parentDomain=urn:li:domain:user_catalog 로 listener 가 별도 emit)
  //   (b) `_` 와 `_` 사이의 전체 영숫자 segment 가 corpuser ownership 으로 부착
  //       (my_x01100_catalog → x01100, my_42_catalog → 42)
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternAttachesChildDomainAndMiddleSegmentOwnerToDataset() {
    // (catalog, expectedOwner) — 영문 prefix 가 있으면 그것까지 포함된 전체 segment.
    String[][] cases = {
        {"my_42_catalog", "42"},
        {"my_x1234_catalog", "x1234"},
        {"my_i1234_catalog", "i1234"},
        {"my_x0173699_catalog", "x0173699"},
        {"my_x01100_catalog", "x01100"},
        {"my_abc_catalog", "abc"},
    };
    for (String[] c : cases) {
      String catalog = c[0];
      String expectedId = c[1];
      Map<String, Object> aspects =
          mapper.datasetAspects(catalog, Namespace.of("ns"), "tbl", Map.of("k", "v"));

      Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
      assertThat((List<String>) domains.get("domains"))
          .as("dataset domain for %s", catalog)
          .containsExactly("urn:li:domain:" + catalog);

      Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
      List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
      assertThat(owners).as("owners list for %s", catalog).hasSize(1);
      assertThat(owners.get(0))
          .as("owner entry for %s", catalog)
          .containsEntry("owner", "urn:li:corpuser:" + expectedId)
          .containsEntry("type", "TECHNICAL_OWNER");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternAttachesChildDomainAndMiddleSegmentOwnerToCatalogContainer() {
    Map<String, Object> aspects =
        mapper.catalogContainerAspects("my_x0173699_catalog", Map.of("k", "v"));

    Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly("urn:li:domain:my_x0173699_catalog");

    Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0))
        .containsEntry("owner", "urn:li:corpuser:x0173699")
        .containsEntry("type", "TECHNICAL_OWNER");
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternAttachesChildDomainAndMiddleSegmentOwnerToNamespaceContainer() {
    Map<String, Object> aspects =
        mapper.namespaceContainerAspects(
            "my_i1234_catalog", Namespace.of("ns", "sub"), Map.of("k", "v"));

    Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly("urn:li:domain:my_i1234_catalog");

    Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0))
        .containsEntry("owner", "urn:li:corpuser:i1234")
        .containsEntry("type", "TECHNICAL_OWNER");
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogChildDomainAspectsLinksParentAndUsesCatalogName() {
    // 자식 Domain 엔티티의 domainProperties: name = catalogName, parentDomain = user_catalog URN.
    Map<String, Object> aspects = mapper.userCatalogChildDomainAspects("my_x0173699_catalog");
    Map<String, Object> props = (Map<String, Object>) aspects.get("domainProperties");
    assertThat(props.get("name")).isEqualTo("my_x0173699_catalog");
    assertThat(props.get("parentDomain")).isEqualTo("urn:li:domain:user_catalog");
    assertThat(props.get("description")).asString().contains("user_catalog");
  }

  @Test
  void userCatalogChildDomainUrnDerivedFromCatalogName() {
    assertThat(mapper.userCatalogChildDomainUrn("my_x0173699_catalog"))
        .isEqualTo("urn:li:domain:my_x0173699_catalog");
    assertThat(mapper.userCatalogChildDomainUrn("my_42_catalog"))
        .isEqualTo("urn:li:domain:my_42_catalog");
  }

  @Test
  void isUserCatalogReflectsPatternMatch() {
    assertThat(mapper.isUserCatalog("my_42_catalog")).isTrue();
    assertThat(mapper.isUserCatalog("my_x0173699_catalog")).isTrue();
    assertThat(mapper.isUserCatalog("my_abc_catalog")).isTrue(); // 영문 only 도 매칭
    assertThat(mapper.isUserCatalog("regular_cat")).isFalse();
    assertThat(mapper.isUserCatalog("my_a_b_catalog")).isFalse(); // _ 가 캡처 영역에 들어가면 미매칭
    assertThat(mapper.isUserCatalog("my__catalog")).isFalse(); // 빈 캡처
    assertThat(mapper.isUserCatalog(null)).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternWinsEvenWithExplicitMappingPresent() {
    // explicit domain-mapping 은 listener 가 더 이상 참조하지 않지만, 혹시 누군가 mapping 을 박았어도
    // user_catalog 자식 도메인이 이긴다 (rule 1).
    DataHubEventMapper m =
        mapperWithDomainMapping(Map.of("my_42_catalog", "urn:li:domain:marketing"));
    Map<String, Object> aspects =
        m.datasetAspects("my_42_catalog", Namespace.of("ns"), "tbl", Map.of("k", "v"));

    Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly("urn:li:domain:my_42_catalog");
    assertThat(aspects).containsKey("ownership");
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternMatchesAnyAlphanumericMiddle() {
    // letters-only / digits-with-trailing-letters / mixed alphanumeric 모두 user_catalog 매칭.
    String[][] cases = {
        {"my_abc_catalog", "abc"},
        {"my_x_catalog", "x"},
        {"my_1234x_catalog", "1234x"},
        {"my_x1234y_catalog", "x1234y"},
        {"my_a1b2_catalog", "a1b2"},
    };
    for (String[] c : cases) {
      String catalog = c[0];
      String expectedId = c[1];
      Map<String, Object> aspects =
          mapper.datasetAspects(catalog, Namespace.of("ns"), "tbl", Map.of("k", "v"));
      Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
      assertThat((List<String>) domains.get("domains"))
          .as("user_catalog domain for %s", catalog)
          .containsExactly("urn:li:domain:" + catalog);
      Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
      List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
      assertThat(owners.get(0))
          .as("owner for %s", catalog)
          .containsEntry("owner", "urn:li:corpuser:" + expectedId);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternRejectsUnderscoreInMiddleAndFallsToEtc() {
    // `_` 가 캡처 영역 안에 있으면 미매칭 (`_` 가 delimiter 이므로) → etc + owner=etc.
    for (String catalog : new String[] {"my_a_b_catalog", "my_x_y_z_catalog"}) {
      Map<String, Object> aspects =
          mapper.datasetAspects(catalog, Namespace.of("ns"), "tbl", Map.of("k", "v"));
      Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
      assertThat((List<String>) domains.get("domains"))
          .as("etc fallback for %s", catalog)
          .containsExactly("urn:li:domain:etc_polaris");
      Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
      List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
      assertThat(owners.get(0))
          .as("etc owner for %s", catalog)
          .containsEntry("owner", "urn:li:corpuser:etc");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternRejectsEmptyMiddleAndFallsToEtc() {
    Map<String, Object> aspects =
        mapper.datasetAspects("my__catalog", Namespace.of("ns"), "tbl", Map.of("k", "v"));
    Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly("urn:li:domain:etc_polaris");
    Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0)).containsEntry("owner", "urn:li:corpuser:etc");
  }

  @Test
  @SuppressWarnings("unchecked")
  void userCatalogPatternRejectsNonAlphanumericAndFallsToEtc() {
    for (String catalog : new String[] {"my_a-b_catalog", "my_a.b_catalog", "my_a b_catalog"}) {
      Map<String, Object> aspects =
          mapper.datasetAspects(catalog, Namespace.of("ns"), "tbl", Map.of("k", "v"));
      Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
      assertThat((List<String>) domains.get("domains"))
          .as("etc fallback for %s", catalog)
          .containsExactly("urn:li:domain:etc_polaris");
      Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
      List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
      assertThat(owners.get(0))
          .as("etc owner for %s", catalog)
          .containsEntry("owner", "urn:li:corpuser:etc");
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Rule 2: catalog 이름이 정확히 "lake_catalog" 면 lake_catalog_polaris 도메인 + datalake owner
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void lakeCatalogAttachesLakeDomainAndDatalakeOwnerToDataset() {
    Map<String, Object> aspects =
        mapper.datasetAspects("lake_catalog", Namespace.of("ns"), "tbl", Map.of("k", "v"));

    Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly("urn:li:domain:lake_catalog_polaris");

    Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0))
        .containsEntry("owner", "urn:li:corpuser:datalake")
        .containsEntry("type", "TECHNICAL_OWNER");
  }

  @Test
  @SuppressWarnings("unchecked")
  void lakeCatalogAttachesLakeDomainAndDatalakeOwnerToContainers() {
    Map<String, Object> catalog =
        mapper.catalogContainerAspects("lake_catalog", Map.of("k", "v"));
    Map<String, Object> namespace =
        mapper.namespaceContainerAspects("lake_catalog", Namespace.of("ns"), Map.of("k", "v"));

    for (Map<String, Object> aspects : List.of(catalog, namespace)) {
      Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
      assertThat((List<String>) domains.get("domains"))
          .containsExactly("urn:li:domain:lake_catalog_polaris");
      Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
      List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
      assertThat(owners.get(0))
          .containsEntry("owner", "urn:li:corpuser:datalake")
          .containsEntry("type", "TECHNICAL_OWNER");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void lakeCatalogDomainAspectsAreFlatWithoutParent() {
    Map<String, Object> aspects = mapper.lakeCatalogDomainAspects();
    Map<String, Object> props = (Map<String, Object>) aspects.get("domainProperties");
    assertThat(props.get("name")).isEqualTo("lake_catalog_polaris");
    assertThat(props).doesNotContainKey("parentDomain"); // flat, top-level
  }

  @Test
  void isLakeCatalogReflectsExactMatch() {
    assertThat(mapper.isLakeCatalog("lake_catalog")).isTrue();
    assertThat(mapper.isLakeCatalog("lake_catalog_v2")).isFalse();
    assertThat(mapper.isLakeCatalog("my_42_catalog")).isFalse();
    assertThat(mapper.isLakeCatalog(null)).isFalse();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Rule 3: 그 외 모두 → etc_polaris + corpuser:etc owner
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void fallbackCatalogAttachesEtcDomainAndEtcOwnerToDataset() {
    // explicit mapping 이 있어도 listener 가 더 이상 참조하지 않으므로 etc 적용 — 회귀 가드.
    DataHubEventMapper m =
        mapperWithDomainMapping(Map.of("regular_cat", "urn:li:domain:marketing"));
    Map<String, Object> aspects =
        m.datasetAspects("regular_cat", Namespace.of("ns"), "tbl", Map.of("k", "v"));

    Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
    assertThat((List<String>) domains.get("domains"))
        .containsExactly("urn:li:domain:etc_polaris");
    Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
    List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
    assertThat(owners.get(0))
        .containsEntry("owner", "urn:li:corpuser:etc")
        .containsEntry("type", "TECHNICAL_OWNER");
  }

  @Test
  @SuppressWarnings("unchecked")
  void fallbackCatalogAttachesEtcDomainAndEtcOwnerToContainers() {
    Map<String, Object> catalog =
        mapper.catalogContainerAspects("regular_cat", Map.of("k", "v"));
    Map<String, Object> namespace =
        mapper.namespaceContainerAspects("regular_cat", Namespace.of("ns"), Map.of("k", "v"));

    for (Map<String, Object> aspects : List.of(catalog, namespace)) {
      Map<String, Object> domains = (Map<String, Object>) aspects.get("domains");
      assertThat((List<String>) domains.get("domains"))
          .containsExactly("urn:li:domain:etc_polaris");
      Map<String, Object> ownership = (Map<String, Object>) aspects.get("ownership");
      List<Map<String, Object>> owners = (List<Map<String, Object>>) ownership.get("owners");
      assertThat(owners.get(0))
          .containsEntry("owner", "urn:li:corpuser:etc")
          .containsEntry("type", "TECHNICAL_OWNER");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void etcDomainAspectsAreFlatWithoutParent() {
    Map<String, Object> aspects = mapper.etcDomainAspects();
    Map<String, Object> props = (Map<String, Object>) aspects.get("domainProperties");
    assertThat(props.get("name")).isEqualTo("etc_polaris");
    assertThat(props).doesNotContainKey("parentDomain"); // flat, top-level
  }

  /** Helper: extract the DataHub type-name from a schema field entry. */
  @SuppressWarnings("unchecked")
  private static String typeName(Map<String, Object> field) {
    Map<String, Object> typeWrapper = (Map<String, Object>) field.get("type");
    Map<String, Object> inner = (Map<String, Object>) typeWrapper.get("type");
    String key = inner.keySet().iterator().next();
    return key.substring("com.linkedin.schema.".length());
  }
}
