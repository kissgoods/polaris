# DataHub Event Listener — 기술 설계 문서

## 1. 배경 및 목적

Apache Polaris는 Iceberg REST Catalog 서버로, 테이블·네임스페이스·카탈로그에 대한 CRUD 작업을 처리한다.
DataHub는 데이터 계보·메타데이터 관리를 위한 플랫폼이다.

이 확장은 **Polaris에서 발생하는 카탈로그 이벤트를 DataHub에 실시간으로 동기화**한다.
테이블을 생성·삭제·이름 변경하거나 네임스페이스를 추가·삭제하면 DataHub의 Dataset·Container 엔티티가 자동으로 갱신된다.

---

## 2. Polaris 이벤트 시스템 개요

```
HTTP 요청
   │
   ▼
IcebergRestCatalogEventServiceDelegator   ← CDI Decorator
   │  onBefore*, onAfter* 호출
   ▼
PolarisEventListener (interface)
   │  @Identifier("datahub-http") 빈 선택
   ▼
AbstractDataHubEventListener
   │  emitter.emitUpsert / emitStatusRemoved 호출
   ▼
DataHubEmitter (interface)
   └─ HttpEmitter — JDK HttpClient 사용
```

### 2.1 IcebergRestCatalogEventServiceDelegator

`runtime/service/.../catalog/iceberg/IcebergRestCatalogEventServiceDelegator.java`

Quarkus CDI Decorator 패턴으로 Iceberg REST Catalog의 모든 API 메서드를 가로챈다.
API 호출 전후로 `polarisEventListener.onBefore*` / `onAfter*` 를 호출한다.

DataHub 리스너를 위해 **두 메서드가 수정**되었다 — `renameTable` 과 `updateProperties`. 두 경로 모두 opt-in marker interface (`RequiresPostRenameTableMetadata`, `RequiresPostUpdateNamespaceMetadata`) 로 추가 비용을 격리한다. marker 미구현 listener (`no-op`, `aws-cloudwatch`, `persistence-in-memory-buffer`) 는 추가 호출을 전혀 부담하지 않는다 — 현재 두 marker 를 모두 구현한 listener 는 `AbstractDataHubEventListener` 하나뿐이다.

이 수정에 앞서 `IcebergRestCatalogEvents.java` 의 두 레코드에 필드와 하위호환 생성자를 추가했다:

```java
// IcebergRestCatalogEvents.java — AfterRenameTableEvent
// 기존: (String catalogName, RenameTableRequest renameTableRequest)
// 수정: LoadTableResponse 필드 추가 + 2-arg 하위호환 생성자 유지
public record AfterRenameTableEvent(
    String catalogName,
    RenameTableRequest renameTableRequest,
    LoadTableResponse loadTableResponse)   // nullable — rename 후 loadTable 호출 결과
    implements PolarisEvent {
  public AfterRenameTableEvent(String catalogName, RenameTableRequest renameTableRequest) {
    this(catalogName, renameTableRequest, null);
  }
}

// IcebergRestCatalogEvents.java — AfterUpdateNamespacePropertiesEvent
// 추가: currentProperties (nullable, marker 미구현 시 null)
public record AfterUpdateNamespacePropertiesEvent(
    String catalogName,
    Namespace namespace,
    UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse,
    Map<String, String> currentProperties)
    implements PolarisEvent {
  public AfterUpdateNamespacePropertiesEvent(
      String catalogName,
      Namespace namespace,
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse) {
    this(catalogName, namespace, updateNamespacePropertiesResponse, null);
  }
}
```

```java
// IcebergRestCatalogEventServiceDelegator.java — renameTable (요약)
Response resp = delegate.renameTable(prefix, renameTableRequest, realmContext, securityContext);
LoadTableResponse loadTableResponse = null;
// Marker 를 구현한 listener (DataHub 등) 만 추가 loadTable 비용을 부담한다.
if (polarisEventListener instanceof RequiresPostRenameTableMetadata) {
  TableIdentifier destination = renameTableRequest.destination();

  // CatalogAdapter.decodeNamespace() 는 URLEncoder.encode(ns) → RESTUtil.decodeNamespace() 흐름이다.
  // URLEncoder 는 raw U+001F(0x1F) 를 "%1F" 로 변환하고, RESTUtil.decodeNamespace() 는 "%1F" 를
  // 구분자로 파싱한다. RESTUtil.encodeNamespace() 를 쓰면 "%1F" 리터럴이 URLEncoder 에서 "%251F" 로
  // 이중 인코딩되어 decodeNamespace() 가 단일 레벨 네임스페이스로 잘못 파싱한다.
  // 따라서 raw U+001F 구분자가 정답이다.
String destinationNs = String.join("", destination.namespace().levels());
  try (Response loadResp = delegate.loadTable(
        prefix, destinationNs, destination.name(),
        null, null, null, realmContext, securityContext)) {
    if (loadResp != null && loadResp.getStatus() == Response.Status.OK.getStatusCode()) {
      loadTableResponse = (LoadTableResponse) loadResp.getEntity();
    }
  } catch (Exception e) {
    LOG.debug("loadTable after rename failed for {}.{}; AfterRenameTableEvent will carry null"
        + " loadTableResponse", destinationNs, destination.name(), e);
  }
}
polarisEventListener.onAfterRenameTable(
    new AfterRenameTableEvent(catalogName, renameTableRequest, loadTableResponse));
```

```java
// IcebergRestCatalogEventServiceDelegator.java — updateProperties (요약)
Response resp = delegate.updateProperties(
    prefix, namespace, updateNamespacePropertiesRequest, realmContext, securityContext);
Map<String, String> currentProperties = null;
// updateProperties 응답은 변경/제거된 키 목록만 포함하고 현재 상태 전체를 포함하지 않는다.
// 전체 properties 가 필요한 listener (DataHub 등) 만 marker 로 opt-in 한다.
if (polarisEventListener instanceof RequiresPostUpdateNamespaceMetadata) {
  try (Response loadResp =
      delegate.loadNamespaceMetadata(prefix, namespace, realmContext, securityContext)) {
    if (loadResp != null && loadResp.getStatus() == Response.Status.OK.getStatusCode()) {
      GetNamespaceResponse loaded = (GetNamespaceResponse) loadResp.getEntity();
      if (loaded != null) currentProperties = loaded.properties();
    }
  } catch (Exception e) {
    LOG.debug("loadNamespaceMetadata after updateProperties failed for {}; "
        + "AfterUpdateNamespacePropertiesEvent will carry null currentProperties", namespace, e);
  }
}
polarisEventListener.onAfterUpdateNamespaceProperties(
    new AfterUpdateNamespacePropertiesEvent(
        catalogName, namespaceObj, (UpdateNamespacePropertiesResponse) resp.getEntity(),
        currentProperties));
```

**이 변경이 필요한 이유**:
- `renameTable` 의 REST 응답은 204 No Content 라서 이벤트에서 테이블 properties 를 얻으려면 rename 완료 후 별도 `loadTable` 호출이 필요하다.
- `updateProperties` 의 REST 응답은 변경된 키 목록만 가지므로 DataHub 의 `customProperties` 를 갱신하려면 후속 `loadNamespaceMetadata` 가 필요하다.

**Marker interface 기반 opt-in 가드**: 두 추가 호출 모두 `polarisEventListener instanceof Requires…Metadata` 가드 안에서만 수행된다. 현재 marker 를 구현하는 listener 는 `AbstractDataHubEventListener` 하나뿐이므로 `no-op`, `aws-cloudwatch`, `persistence-in-memory-buffer` 운영 시에는 비용이 0 이다. Quarkus ARC client proxy 도 원본 클래스를 extend 하므로 `instanceof` 가 정상 동작한다. 새 listener 가 동일 metadata 가 필요하면 marker 만 구현하면 된다 — capability 가 명시적으로 의도되는 구조라서 다중 listener 환경에서도 일반화 부담이 없다.

**부수효과 — 운영 시 인지 사항** (DataHub listener 활성 시에만 해당; 다른 listener 는 marker 미구현이라 추가 호출 자체가 없다):

- *인가 분리 시나리오*: 추가 `loadTable` / `loadNamespaceMetadata` 는 각각 `LOAD_TABLE`(`TABLE_READ_PROPERTIES`) / `LOAD_NAMESPACE_METADATA` 권한을 별도로 평가한다. principal 이 `RENAME_TABLE` / `UPDATE_NAMESPACE_PROPERTIES` 만 가지고 후속 권한이 없으면 추가 호출이 실패해 listener 가 메타데이터를 잃는다 (Polaris 동작 자체에는 영향 없음). 현재 실패는 `LOG.debug` 로만 기록되므로 운영 가시성을 위해 INFO/WARN 으로 한 번만 로그하는 방안 검토 권장.
- *Race condition*: 1차 변경 (rename / updateProperties) 과 후속 load 사이에 동일 target 에 drop / 재 rename / 다른 update 가 끼어들면 stale 또는 다른 메타데이터를 emit 할 수 있다. 빈도는 낮지만 listener 측 일관성은 보장되지 않음.
- *성능*: 매 rename / updateProperties 마다 메타데이터 fetch 1 회 + 권한평가 1 회가 추가된다. `loadTable` 은 `snapshots=null` 로 호출되어 metadata-cache 가 적용되지 않는 점도 인지. 두 작업 모두 hot path 가 아니라 운영 영향은 작음.

### 2.2 네임스페이스 인코딩

Iceberg REST는 다중 레벨 네임스페이스(`ns.sub`)를 URL 경로에서 U+001F (Unit Separator, 0x1F) 로 인코딩한다.

```
URL 경로:  /v1/{prefix}/namesp    aces/ns%1Fsub/tables/tbl
                                     ^^^
                                   U+001F를 %1F로 percent-encoding
```

JAX-RS는 `@PathParam`을 URL 디코딩해서 Java 메서드에 전달한다 (`@Encoded` 없으면 기본 디코딩).
따라서 path parameter `namespace`에는 실제 U+001F 바이트가 포함된 raw 문자열이 온다.

`CatalogAdapter.decodeNamespace()` 내부 흐름:

```java
// 입력: "nssub"  (U+001F가 포함된 raw 문자열,  = 0x1F)
URLEncoder.encode("nssub")  →  "ns%1Fsub"
RESTUtil.decodeNamespace("ns%1Fsub")  →  Namespace.of("ns", "sub")
```

따라서 `delegate.loadTable()`에 전달하는 namespace 문자열도 `String.join("", levels)`로 raw U+001F를 구분자로 사용해야 한다.

이 동작은 `DataHubEventMapperTest.unitSeparatorJoinRoundTripsViaURLEncoderForNestedNamespace` 테스트로 검증되어 있다.

> ⚠️ **코드 리뷰 함정**: 프로덕션 코드(`IcebergRestCatalogEventServiceDelegator.java:402`)와 위 테스트(`DataHubEventMapperTest.java:109`) 의 `String.join(...)` 호출은 **빈 문자열 구분자처럼 보이지만, 문자열 리터럴 안에 U+001F (0x1F) 가 직접 인라인되어 있다** (`hex: 22 1f 22`). 이는 의도된 동작으로 위 round-trip 의 핵심이지만, 일반 텍스트 뷰에서는 식별이 어렵다. 가독성을 위해 명명 상수(`NS_SEPARATOR`)로 cosmetic refactor 하는 것은 검토 가치가 있다 — 동작은 동일하다.

---

## 3. 모듈 구조

```
extensions/datahub-listener/            # 공통 코드 + HTTP 리스너
└── src/main/java/.../datahub/
    ├── DataHubConfiguration.java          # SmallRye @ConfigMapping
    ├── DataHubEmitter.java                # 전송 추상화 인터페이스 (AutoCloseable)
    ├── DataHubEventMapper.java            # URN·Aspect 생성 (escape, schemaMetadata, auditStamp, domains)
    ├── AbstractDataHubEventListener.java  # 이벤트 → Emitter 오케스트레이션 + RequiresPost* marker 구현
    └── http/
        ├── HttpEmitter.java               # JDK HttpClient 구현 (circuit breaker, replay buffer)
        └── DataHubHttpEventListener.java  # @Identifier("datahub-http") + SecurityContext 기반 AuditContext
```

전송 방식은 JDK `HttpClient` 기반의 단일 모듈이다. 한 번의 upsert 가 한 entity 의 여러 aspect 를
단일 JSON 배열 entry 로 묶어 한 번의 POST 로 전송된다 — 추가 런타임 의존성 없음 (Jackson 은
Polaris 기본 포함).

---

## 4. 핵심 클래스 상세

### 4.1 DataHubConfiguration

```java
@ConfigMapping(prefix = "polaris.event-listener.datahub")
@ApplicationScoped
public interface DataHubConfiguration {
    @WithName("gms-url")    Optional<String> gmsUrl();  // 선택 — 미설정 시 emit skip
    @WithName("token")      Optional<String> token();   // Bearer 토큰 (선택)
    @WithName("platform-instance") @WithDefault("polaris") String platformInstance();
    @WithName("env")               @WithDefault("PROD")    String env();
    @WithName("synchronous-mode")  @WithDefault("false")   boolean synchronousMode();

    // ── DataHub 장애 격리 가드 (Section 4.4 참조) ─
    // ─
    @WithName("connect-timeout")    @WithDefault("5s")     Duration connectTimeout();
    @WithName("request-timeout")    @WithDefault("10s")    Duration requestTimeout();
    @WithName("max-inflight-emits") @WithDefault("1000")   int maxInflightEmits();
    @WithName("circuit-breaker-failure-threshold") @WithDefault("5")    int circuitBreakerFailureThreshold();
    @WithName("circuit-breaker-open-duration")     @WithDefault("30s")  Duration circuitBreakerOpenDuration();
    @WithName("replay-buffer-capacity")            @WithDefault("1000") int replayBufferCapacity(); // 0 = 비활성

    // ── Catalog → DataHub Domain URN 매핑 (dataset 한정, fallback 없음) ──
    @WithName("domain-mapping") Map<String, String> domainMapping();
}
```

`gms-url` 미설정 시 동작: emit마다 에러 로그 후 skip — catalog 요청 차단 없음.

`gmsUrl()`이 `Optional`인 이유: SmallRye Config는 `@ConfigMapping` 인터페이스를 기동 시 eager-validate한다. `no-op` listener를 선택해도 `DataHubConfiguration` 빈은 클래스패스에 존재하므로, 필수 `String`으로 두면 URL 미설정 시 no-op 서버 기동도 실패한다.

`synchronous-mode` 의 본 용도는 테스트·디버깅이지만, **strict per-URN ordering** 이 필요한 운영에도
선택지로 두었다. async 모드에서는 (1) HTTP/2 multiplexing/커넥션 풀로 동일 URN 의 두 emit 이
DataHub 측에서 뒤바뀌어 도달할 수 있고, (2) `sendAsync` 가 디스패치된 직후의 stale replay 는 JDK
`HttpClient` 가 cancellation 을 honor 하지 않으므로 멈출 수 없다 — sync 모드로 두면 매 emit 이
catalog 요청 thread 에서 직렬화되어 두 race 모두 사라진다 (지연 비용은 catalog op 마다 발생).

`domain-mapping` 은 **API 호환성을 위해 interface 에 남아있으나 listener 가 더 이상 참조하지 않는다.**
Section 4.2 의 3-tier 분류 (user_catalog / lake_catalog / etc_polaris) 가 모든 catalog 를 cover 하므로
redundant. 향후 operator override 가 필요해지면 Tier 1과 Tier 2 사이의 새 Tier 로 도입 가능.

SmallRye Config는 환경변수를 자동으로 config 속성에 매핑한다:

```
POLARIS_EVENT_LISTENER_DATAHUB_GMS_URL                      →  polaris.event-listener.datahub.gms-url
POLARIS_EVENT_LISTENER_DATAHUB_TOKEN                        →  polaris.event-listener.datahub.token
POLARIS_EVENT_LISTENER_DATAHUB_DOMAIN_MAPPING_<CATALOG>     →  polaris.event-listener.datahub.domain-mapping.<catalog>
```

### 4.2 DataHubEventMapper

Polaris 이벤트 데이터로부터 DataHub URN과 Aspect payload를 생성한다.

**URN 스킴**

| 대상 | URN 형식 | 예시 |
|------|----------|------|
| Dataset (테이블) | `urn:li:dataset:(urn:li:dataPlatform:iceberg,{catalog}.{ns}.{table},{ENV})` | `urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.sub.tbl,PROD)` |
| Catalog Container | `urn:li:container:iceberg.{platformInstance}.{catalog}` | `urn:li:container:iceberg.polaris.cat` |
| Namespace Container | `urn:li:container:iceberg.{platformInstance}.{catalog}.{ns}` | `urn:li:container:iceberg.polaris.cat.ns.sub` |

**Segment escape (`escapeSegment`)**

URN 의 구분자로 쓰이는 문자를 segment 안에서 percent-escape 한다 — `%`, `.`, `,`, `(`, `)`. `%` 를
가장 먼저 escape 해서 round-trip 을 보장한다 (`"a%2Eb"` → `"a%252Eb"` ≠ escape(`"a.b"`)).

| 문자 | 이유 |
|---|---|
| `.` | container URN segment 분리자 (`iceberg.polaris.cat.ns`) — 이게 없으면 `Namespace.of("a.b")` 와 `Namespace.of("a","b")` 가 같은 URN 으로 충돌 |
| `,` `(` `)` | dataset URN tuple 구분자 (`urn:li:dataset:(...,qualifiedName,env)`) — 이름에 들어가면 tuple 구조 깨짐 |
| `%` | escape 자체의 round-trip 보장 |

**Namespace join 두 형태 (URN vs Display)**

같은 namespace 라도 URN 으로 쓰일 때와 UI 에 보일 때 다른 형식을 쓴다.

| 메서드 | 용도 | escape | 분리자 |
|---|---|---|---|
| `joinForUrn(ns)` | URN segment | O | `.` |
| `joinForDisplay(ns)` | DataHub UI `name` | X | `.` |

UI 에는 사람-읽기-쉽게 `a.b` 로 보이고, URN 에는 `a%2Eb` 로 들어가 충돌이 없다.

**중첩 네임스페이스 계층 구조**

DataHub UI에서 올바른 트리를 표시하려면 container 엔티티의 부모 링크가 직접 부모를 가리켜야 한다.

```
cat                 (Catalog Container)
└── ns              (Namespace Container, parent = cat)
    └── sub         (Namespace Container, parent = ns)
        └── tbl     (Dataset, container = sub)
```

`parentContainerUrn` 헬퍼가 이 계산을 담당한다:

```java
private String parentContainerUrn(String catalogName, Namespace namespace) {
    if (namespace == null || namespace.length() <= 1) {
        return catalogContainerUrn(catalogName);       // 최상위 → catalog가 부모
    }
    String[] levels = namespace.levels();
    Namespace parent = Namespace.of(Arrays.copyOf(levels, levels.length - 1));
    return namespaceContainerUrn(catalogName, parent); // 중첩 → 직접 상위 namespace가 부모
}
```

**빈 네임스페이스 처리**

`namespaceContainerUrn("cat", Namespace.empty())` 호출 시 trailing dot URN(`"...cat."`) 이 생성되는 것을 방지한다:

```java
public String namespaceContainerUrn(String catalogName, Namespace namespace) {
    if (namespace == null || namespace.isEmpty()) {
        return catalogContainerUrn(catalogName);  // root 테이블은 catalog를 container로 사용
    }
    return catalogContainerUrn(catalogName) + "." + joinForUrn(namespace);
}
```

**`namespaceContainerUrnFromRaw` — drop namespace 경로**

`AfterDropNamespaceEvent` 는 (다른 namespace 이벤트들과 달리) namespace 를 `Namespace` 객체가 아니라
원본 `String` (U+001F 구분자 포함) 으로 전달한다. 같은 logical namespace 에 대해 URN 이 정확히 일치해야
DataHub 가 동일 entity 의 status 를 갱신할 수 있으므로, `namespaceContainerUrnFromRaw` 가 raw string 을
U+001F 로 split 한 뒤 각 level 을 `escapeSegment` 처리해 결과가 `namespaceContainerUrn(cat, ns)` 와
동일하게 매치되도록 한다 (level 에 literal `.` 이 들어가는 경우 포함).

**null properties 처리**

`tableProperties == null`이면 `customProperties` 키 자체를 omit한다. DataHub는 absent key를 기존 값 보존으로 처리한다 (rename 후 loadTable 실패 시 기존 properties를 덮어쓰지 않기 위해). namespace
container 도 동일 — `currentProperties == null` 이면 `containerProperties` aspect 자체를 omit (Section 4.3 참조).

**Iceberg 메타데이터 → `customProperties` 확장 (`enrichedTableProperties`)**

`TableMetadata` 가 동반된 dataset emit (create / register / update / commit / rename) 은 사용자 정의
properties 외에도 Iceberg top-level 메타데이터를 customProperties 에 함께 넣어 DataHub UI 에서 바로
보이도록 한다:

| 키 | 값 |
|---|---|
| `table-uuid` | `TableMetadata.uuid()` (있을 때) |
| `location` | `TableMetadata.location()` (있을 때) |
| `format-version` | `TableMetadata.formatVersion()` |
| `current-schema-id` | `TableMetadata.currentSchemaId()` |
| `last-updated-ms` | `TableMetadata.lastUpdatedMillis()` |
| `last-sequence-number` | `TableMetadata.lastSequenceNumber()` |
| `default-spec-id` | `TableMetadata.defaultSpecId()` |
| `default-sort-order-id` | `TableMetadata.defaultSortOrderId()` |
| `current-snapshot-id` | 현재 snapshot 의 id, 없으면 `-1` |

`tableProperties` 와 `tableMetadata` 가 **모두** null 일 때만 `customProperties` 키 자체를 omit (위 "null
properties 처리" 와 일관).

**Schema metadata aspect (`schemaMetadataAspect`)**

`TableMetadata` 가 동반된 dataset emit 은 추가로 `schemaMetadata` aspect 를 emit 해 DataHub UI 에 실제
Iceberg schema 가 보이도록 한다. payload 의 골격:

```json
{
  "schemaName": "<qualifiedName>",
  "platform": "urn:li:dataPlatform:iceberg",
  "version": 0,
  "hash": "",
  "platformSchema": { "com.linkedin.schema.Schemaless": {} },
  "fields": [ { "fieldPath": "...", "nativeDataType": "...", "type": {...}, "nullable": ..., "description": "..." } ]
}
```

nested struct 는 dotted path 로 flatten 되며 (`parent.child`), parent 가 optional 이면 child 도
nullable=true 로 전파한다. Iceberg type → DataHub `SchemaFieldDataType` 매핑은 `schemaFieldType`
헬퍼가 담당: `Integer/Long/Float/Double/Decimal → NumberType`, `String/UUID → StringType`,
`Date → DateType`, `Time/Timestamp → TimeType`, `Binary/Fixed → BytesType`, `List → ArrayType`,
`Map → MapType`, `Struct → RecordType`, 그 외 `NullType`.

**Audit stamp (`AuditContext`, `auditStamp`)**

`AuditContext(long timestampMs, String principal)` 가 emit 시점 audit 정보를 운반하며, dataset emit 의
`datasetProperties.lastModified` 에 `{ time, actor }` 형태로 들어간다. `principal == null/empty` 이면
fallback actor 는 `urn:li:corpuser:__system__`. 기본 `AuditContext.now(principal)` 는 wall-clock 을
사용. 실제 principal 주입은 `DataHubHttpEventListener.getAuditContext()` 가 `SecurityContext.getUserPrincipal()`
에서 가져온다 (Section 4.5 참조).

**3-tier 카탈로그 분류 → domain & ownership**

모든 catalog/namespace/dataset emit 에 정확히 하나의 도메인이 자동 부착된다. 우선순위:

| Tier | 조건 | Domain URN | Owner | Domain entity 부모 |
|---|---|---|---|---|
| 1 | catalog 이름이 `^my_([a-zA-Z0-9]+)_catalog$` | `urn:li:domain:<catalogName>` (catalog 별 자식) | `urn:li:corpuser:<중간 segment>` (TECHNICAL_OWNER) | `urn:li:domain:user_catalog` |
| 2 | catalog 이름 == `lake_catalog` | `urn:li:domain:lake_catalog_polaris` (공유 top-level) | `urn:li:corpuser:datalake` (TECHNICAL_OWNER) | — (flat) |
| 3 | 그 외 모두 | `urn:li:domain:etc_polaris` (공유 top-level) | `urn:li:corpuser:etc` (TECHNICAL_OWNER) | — (flat) |

listener 가 각 룰에 해당하는 **Domain 엔티티 자체** 도 idempotent emit 한다 (`ensureCatalogDomain`):
- Tier 1: per-catalog 자식 Domain (`name=<catalogName>`, `parentDomain=urn:li:domain:user_catalog`)
- Tier 2: 공유 `lake_catalog_polaris` (flat, no parent)
- Tier 3: 공유 `etc_polaris` (flat, no parent)

이렇게 해서 DataHub `/domains` UI 에 dangling reference 가 절대 생기지 않는다. **사전 수동 생성이 필요한 것은
오직 `urn:li:domain:user_catalog` 부모 하나뿐** (Tier 1 자식들의 `parentDomain` 이 가리키는 대상).

**Tier 별 `ownership` aspect 차이**:
- Tier 1: `_` 와 `_` 사이의 전체 영숫자 segment 를 corpuser URN id 로 사용. 예: `my_x0173699_catalog` → `urn:li:corpuser:x0173699`, `my_x01100_catalog` → `urn:li:corpuser:x01100`, `my_42_catalog` → `urn:li:corpuser:42`, `my_abc_catalog` → `urn:li:corpuser:abc` (영문 only 도 허용). `_` 가 segment 안에 들어가면 미매칭 (delimiter 충돌).
- Tier 2: 고정 `urn:li:corpuser:datalake`.
- Tier 3: 고정 `urn:li:corpuser:etc` placeholder owner — etc 트리 전체가 동일 가상 사용자로 묶여 UI 탐색 일관성을 확보 (Tier 2 의 `datalake` placeholder 와 같은 역할).

**explicit `domain-mapping` config**: `DataHubConfiguration.domainMapping()` 메서드는 API 호환성을 위해
interface 에 남아있으나 listener 가 더 이상 consult 하지 않는다. 3-tier 분류가 모든 catalog 를 cover 하므로
redundant. 향후 operator override 가 필요해지면 Tier 1과 Tier 2 사이의 새 Tier 로 도입 가능.

**Tier 1 자식 Domain — 자동 생성 & 자동 정리**

Tier 1 (`my_<alnum>_catalog`) 만 *catalog 별로* 고유한 자식 Domain 을 갖는다. Tier 2 (lake_catalog_polaris)
와 Tier 3 (etc_polaris) 는 모든 매칭 catalog 가 같은 Domain URN 을 공유한다.

- **생성**: 각 upsert 핸들러 (`onAfterCreateCatalog`, `onAfterCreateNamespace`, `onAfterCreateTable` 등)
  의 시작 부분에서 listener 의 `ensureCatalogDomain(catalogName)` 가 적합한 Domain entity 를 idempotent
  emit. Tier 1 은 `parentDomain = urn:li:domain:user_catalog` 를 가지는 자식 entity. Tier 2/3 은 공유
  top-level entity (flat).
- **삭제**: Tier 1 만 catalog drop 시 자식 Domain 도 함께 `status: removed=true` 로 soft-delete
  (`onAfterDeleteCatalog` 안의 `mapper.isUserCatalog(catalogName)` 가드). Tier 2/3 의 공유 Domain 은
  다른 catalog 가 사용 중일 수 있어 미터치. 같은 이름의 catalog 가 다시 생성되면 `ensureCatalogDomain`
  의 `emitUpsert` 가 `status: removed=false` 자동 주입 (Section 4.4 의 status-resurrect 로직) — Domain 부활.
  회귀 가드: `deleteCatalogSoftDeletesChildDomainForPatternMatchedCatalog`,
  `deleteLakeCatalogDoesNotTouchSharedLakeDomain`, `deleteNonPatternCatalogDoesNotTouchAnyDomain`.

**자식 Domain entity 의 자동 emit 이 필요한 이유**: DataHub 의 `domains` aspect 는 Domain URN *참조*
만 담는다. Domain entity 자체가 존재하지 않으면 dangling reference 가 되어 `/domains` UI 페이지에
나타나지 않는다. listener 가 각 upsert 마다 해당 tier 의 Domain entity 를 idempotent emit 해서 이
사전 등록 부담을 운영자에게 떠넘기지 않는다. **수동 사전 생성이 필요한 것은 오직 `urn:li:domain:user_catalog`
부모 하나뿐** (Tier 1 자식들의 `parentDomain` 이 가리키는 대상). 부모는 한 번 GMS REST `POST /openapi/v3/entity/domain`
으로 만들면 끝.

**Container 까지 domain 부착하는 이유**: 일반 explicit `domain-mapping` (이미 deprecated) 은 dataset
한정이었으나, 3-tier 분류는 *catalog 트리 전체가 같은 도메인* 이라는 운영 의미가 명확하므로 catalog/namespace
container 도 동일 aspect 부착. DataHub UI 의 lineage·탐색에서 일관성 확보.

### 4.3 AbstractDataHubEventListener

`PolarisEventListener` 의 구현체. 이벤트 종류별로 `emitUpsert` 또는 `emitStatusRemoved` 를 호출한다.
**모든 upsert 핸들러는 시작 부분에서 `ensureCatalogDomain(catalogName)` 을 호출** 해 해당 catalog 가
속한 tier 의 Domain entity 도 idempotent 함께 emit (Section 4.2 참조). 따라서 한 이벤트의 wire 결과는
`[Domain entity upsert, 주 entity upsert]` 두 POST 가 된다.

| 이벤트 | DataHub 동작 |
|--------|------------|
| `onAfterCreateCatalog` | `ensureCatalogDomain` + Catalog Container upsert (subType: Catalog) |
| `onAfterUpdateCatalog` | `ensureCatalogDomain` + Catalog Container upsert |
| `onAfterDeleteCatalog` | Catalog Container `status.removed=true` + (Tier 1 만) 자식 Domain `status.removed=true` |
| `onAfterCreateNamespace` | `ensureCatalogDomain` + Namespace Container upsert (subType: **Namespace**) |
| `onAfterUpdateNamespaceProperties` | `ensureCatalogDomain` + Namespace Container upsert (subType: Namespace, delegator 가 marker 로 추가 load 한 currentProperties 사용; 실패 시 containerProperties 생략 fallback) |
| `onAfterDropNamespace` | Namespace Container `status.removed=true` |
| `onAfterCreateTable` | TableMetadata 있을 때만: `ensureCatalogDomain` + Dataset upsert (subType=Table). metadata 없으면 skip |
| `onAfterRegisterTable` | 위와 동일 — metadata 없으면 skip |
| `onAfterUpdateTable` | 위와 동일 — metadata 없으면 skip |
| `onAfterCommitTable` | `metadataAfter` 있을 때만 emit. 없으면 skip (Iceberg-form 검증) |
| `onAfterRenameTable` | 구 URN `status.removed` 는 항상 emit. **신 URN 도 항상 emit** — metadata 있으면 schemaMetadata 포함 full upsert, metadata 없으면 minimal aspect (schemaMetadata 만 omit) 로 emit. rename 이벤트 자체가 Iceberg-form 증명이므로 dataset 이 사라지지 않게 한다 |
| `onAfterDropTable` | Dataset `status.removed=true` (metadata 무관 — 단순 삭제 신호) |

**`ensureCatalogDomain(catalogName)` helper**

각 upsert 핸들러에서 호출되는 helper. catalog 분류 (Section 4.2) 에 따라 정확히 하나의 Domain entity
를 emit:

```java
protected void ensureCatalogDomain(String catalogName) {
    if (mapper.isUserCatalog(catalogName)) {
        // Tier 1: catalog 별 자식 Domain (parentDomain = urn:li:domain:user_catalog)
        emitter.emitUpsert(
            mapper.userCatalogChildDomainUrn(catalogName),
            DataHubEventMapper.DOMAIN,
            mapper.userCatalogChildDomainAspects(catalogName));
        return;
    }
    if (mapper.isLakeCatalog(catalogName)) {
        // Tier 2: 공유 flat Domain (lake_catalog_polaris)
        emitter.emitUpsert(
            DataHubEventMapper.LAKE_CATALOG_DOMAIN_URN,
            DataHubEventMapper.DOMAIN,
            mapper.lakeCatalogDomainAspects());
        return;
    }
    // Tier 3: 공유 flat Domain (etc_polaris)
    emitter.emitUpsert(
        DataHubEventMapper.ETC_DOMAIN_URN,
        DataHubEventMapper.DOMAIN,
        mapper.etcDomainAspects());
}
```

idempotent — 같은 Domain URN 에 대한 반복 emit 은 DataHub 가 upsert 로 무시. drop 핸들러는 `ensureCatalogDomain`
을 호출하지 않으며 Tier 1 만 `onAfterDeleteCatalog` 안에서 자식 Domain 도 symmetric soft-delete.

**`onAfterUpdateNamespaceProperties` 특이 처리**

REST 응답 자체는 변경된 키 목록만 포함하고 전체 현재 properties 는 포함하지 않는다. 그래서
`AbstractDataHubEventListener` 가 `RequiresPostUpdateNamespaceMetadata` marker 를 구현하고,
`IcebergRestCatalogEventServiceDelegator.updateProperties` 가 marker 를 본 경우 추가로
`delegate.loadNamespaceMetadata()` 를 호출해서 결과를 `AfterUpdateNamespacePropertiesEvent.currentProperties`
에 담는다 (Section 2.1 참조). listener 는 그 값을 `namespaceContainerAspects` 에 그대로 넘겨 전체
`containerProperties` 를 emit 한다:

```java
emitter.emitUpsert(
    mapper.namespaceContainerUrn(event.catalogName(), event.namespace()),
    DataHubEventMapper.CONTAINER,
    mapper.namespaceContainerAspects(
        event.catalogName(), event.namespace(), event.currentProperties()));
```

후속 load 가 실패해 `currentProperties == null` 이 되면 listener 는 보수적 fallback (containerProperties
aspect 미전송) 으로 동작해서 DataHub 의 기존 properties 를 덮어쓰지 않는다 (Section 4.2 의 "null
properties 처리" 참조).

**Quarkus 프록시를 위한 no-args 생성자**

Quarkus는 `@ApplicationScoped` 빈의 CDI 프록시 서브클래스를 생성할 때 `super()` 를 호출한다.
no-args 생성자가 없으면 빌드 시 *"It's not possible to automatically add a synthetic no-args constructor"* 오류가 발생한다.

```java
protected AbstractDataHubEventListener() {
    this.emitter = null;   // 프록시 인스턴스용 — 실제 빈은 @Inject 생성자로 초기화됨
    this.mapper = null;
}

@PreDestroy
void shutdown() {
    if (emitter == null) return;  // 프록시 인스턴스 호출 시 NPE 방지
    emitter.close();
}
```

**`onAfterCommitTransaction` 는 의도적으로 override 하지 않음**

multi-table atomic commit 은 두 종류의 이벤트를 동시에 발생시킨다 — 테이블별 `onAfterCommitTable`
(전체 `TableMetadata` 포함) 과 트랜잭션 전체의 `onAfterCommitTransaction` (메타데이터 없음).
전자 처리만으로 모든 테이블이 올바르게 sync 되며, 후자에서 한 번 더 emit 하면 properties 없는 dataset
upsert 가 발생해 DataHub 의 `customProperties` 를 비워버린다. 회귀 방지 테스트:
`commitTransactionMustRemainNoOpToAvoidWipingCustomProperties` 및
`abstractDataHubEventListenerDoesNotOverrideOnAfterCommitTransaction` (reflection 으로 override 부재
검증).

**Audit context 주입 지점 (`getAuditContext()` 가상 메서드)**

```java
protected AuditContext getAuditContext() {
    return new AuditContext(System.currentTimeMillis(), null);  // 기본 — 테스트·offline 경로용
}
```

abstract base 는 wall-clock + null principal 을 반환하고, 실제 운영 listener 는 이 메서드를 override
해서 `SecurityContext` 의 caller principal 을 주입한다 (Section 4.5 참조). dataset emit 이 모두 이
메서드를 통해 `AuditContext` 를 얻으므로 abstract base 자체는 JAX-RS request scope 없이도 단위 테스트
가능하다.

### 4.4 HttpEmitter

JDK 11+ `HttpClient` 를 사용해 DataHub GMS의 OpenAPI v3 엔드포인트에 POST한다.
추가 런타임 의존성이 없다.

**DataHub 장애 격리 (Polaris 운영 영향 0 보장)**

다섯 단계 방어:

| 메커니즘 | 동작 | 인터페이스 `@WithDefault` (= 실효 운영 default) | 설정 키 |
|---|---|---|---|
| Connect timeout | TCP 연결 대기 한계. DataHub 가 black-hole / 미응답일 때 OS-level timeout (~수십초~수분) 으로 묶이는 것을 차단 | 5s | `polaris.event-listener.datahub.connect-timeout` |
| Request timeout | 단일 emit 의 end-to-end 한계. async 모드에서는 미완료 future 의 최대 수명, sync 모드에서는 catalog 요청 최대 차단 시간 | 10s | `polaris.event-listener.datahub.request-timeout` |
| Inflight backpressure | 동시 미완료 emit 수에 cap (`Semaphore.tryAcquire`). 한도 초과 시 즉시 drop + WARN (100건마다 로그). DataHub 다운 시 메모리/소켓/스레드 unbounded 증가 차단 | 1000 | `polaris.event-listener.datahub.max-inflight-emits` |
| Circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED\|OPEN) | 연속 실패 N회 누적 시 회로 OPEN → cooldown 동안 모든 emit skip. cooldown 만료 후 **CAS 로 단일 caller 만 HALF_OPEN probe 진입** — 동시 emit 들은 skip. probe 성공이면 CLOSED 전이, 실패면 OPEN + fresh cooldown. state 와 timestamp 는 immutable `CircuitSnapshot` 으로 묶여 단일 `AtomicReference` 에 CAS 갱신 → 두 값의 publish 사이 race window 제거. `recordSuccess/Failure` 는 caller 가 진입한 상태(`entered`)를 받아 **HALF_OPEN probe 결과만 CLOSED 전이를 허용** — stale async callback 이 OPEN 을 무효화하지 못함. probe 가 schedule 못한 경우 (inflight cap full, build error) `releaseProbeSlot()` 이 fresh cooldown 으로 OPEN 갱신해 busy loop 차단. 10분 outage 동안 수만번 시도 → 약 20번으로 단축 | 5회 / 30s | `circuit-breaker-failure-threshold`, `circuit-breaker-open-duration` |
| Replay buffer (peek-and-keep) | 실패·skip 된 emit 을 bounded 큐(`LinkedBlockingQueue`)에 적재 → daemon thread 가 cooldown 만료 후 재전송. **`peek()` 후 `forceSync=true` 로 전송한 결과 (성공 또는 permanent-rejection) 일 때만 `poll()` 로 head 제거** — failed probe 시 head 가 큐에 보존되어 다음 cooldown 후 재시도. 단순 `poll`-then-`post` 였다면 outage 중 cooldown 직후 첫 probe 실패가 head 를 silently 잃는 race 가 있었음. 큐 가득 시 oldest drop (`bufferLock` 으로 race 보호) 으로 메모리 unbounded 증가 차단 | 1000 events | `replay-buffer-capacity` (0 = 비활성) |

`runtime/defaults/src/main/resources/application.properties` 에는 위 키들이 주석으로만 표기되어 있고 실제 값은 설정되어 있지 않다 — 따라서 실효 운영 default 는 `@WithDefault` 값과 같다. 더 보수적인 값이 필요한 환경에서는 env vars (`POLARIS_EVENT_LISTENER_DATAHUB_*`) 또는 Helm 의 `extraEnv` 로 override 한다 (CLAUDE.md 의 "헬름/배포 시 세부 설정은 env vars 로만 주입" 컨벤션 참조). 더불어 모든 request 빌드/스케줄링 경로(`URI.create`, `HttpRequest.timeout`, `sendAsync`)는 단일 try/finally 로 감싸 잘못된 설정값(예: malformed URL, `Duration.ZERO`)이 들어와도 예외가 caller 로 전파되지 않고 permit 도 누수되지 않는다.

**응답 분류 (`isRetryableFailure`)**

응답 도착 후 다음 분류로 분기한다:

| 분류 | 매칭 | 동작 |
|---|---|---|
| Success | `err == null && 2xx` | `recordSuccess(entered)` — `entered == HALF_OPEN` 이면 CLOSED 전이 |
| Retryable failure | `err != null` ∪ 5xx ∪ 408 ∪ 429 | `recordFailure(entered)` — circuit threshold/HALF_OPEN probe 결정 + replay buffer 적재 |
| Non-retryable 4xx | 그 외 4xx (400/401/403/404 등) | WARN 로그 + drop. circuit failure 카운터 미증가, replay buffer 미적재. HALF_OPEN probe 시에는 GMS 도달 신호로 보고 `recordSuccess` 호출 → CLOSED 전이 (replayer 가 buffered item 을 "handled" 로 보고 제거) |

핵심 의도: persistent client-side 오류 (잘못된 schema, 만료 token, 권한 미부여) 가 circuit 를 잘못 OPEN 시키거나 replay buffer 를 영구 미배달 항목으로 가득 채우는 것을 차단.

이 다섯 메커니즘은 독립적으로 동작하며, **drop 된 이벤트는 catalog 트랜잭션에 영향을 주지 않는다** — listener 측 데이터 누락만 발생한다 (영구 outbox 미적용 — 알려진 제약).

**Circuit breaker / Replay buffer 동작 흐름 (10분 outage 시나리오)**

```
t=0      DataHub down 시작
t=0~50s  emit 시도 → 5번 연속 실패 (각 ~10s request-timeout) → 회로 OPEN
t=50s~   모든 emit skip (네트워크 시도 0). 실패한 emit 들은 replay buffer 적재 (cap 1000)
         buffer 가득 시 oldest drop (bounded memory)
t=80s    cooldown (30s) 경과 → 다음 emit = probe 시도 → 실패 → OPEN 재진입 (+ 30s)
...      약 20회 probe 반복 (cooldown 30s × ~20 ≈ 10분)
t=600s   DataHub 회복 → probe 성공 → 회로 CLOSE → INFO 로그
         replayer thread 가 buffer 드레인 → 적재된 이벤트 (최근 cap 만큼) 재전송
```

요점: catalog operations 는 t=0 부터 끝까지 무중단. DataHub 회복 후 buffer 안에 있던 이벤트는 자동 복구됨.

```java
// Pseudo-flow inside HttpEmitter.post(path, body, enqueueOnFailure, forceSync)
final CircuitState entered = tryEnterCircuit();           // OPEN(cooldown 중) 또는 HALF_OPEN(probe 진행 중) 이면 null
if (entered == null) {
    if (enqueueOnFailure) offerToReplayBuffer(path, body);
    return false;
}
final boolean isProbe = (entered == CircuitState.HALF_OPEN);

if (!inflight.tryAcquire()) {                              // backpressure — DataHub hang 시 빠르게 drop
    if (isProbe) releaseProbeSlot();                       // HALF_OPEN→OPEN with fresh cooldown (busy loop 방지)
    if (enqueueOnFailure) offerToReplayBuffer(path, body);
    return false;
}

HttpRequest req = HttpRequest.newBuilder(URI.create(gmsUrl + path))
    .timeout(config.requestTimeout())                      // per-request timeout
    ...build();

httpClient.sendAsync(req, BodyHandlers.ofString())
    .whenComplete((resp, err) -> {
        try {
            int status = (err == null) ? resp.statusCode() : 0;
            if (err == null && status / 100 == 2) {
                recordSuccess(entered);                    // HALF_OPEN probe → CLOSED 전이
                handled.set(true);
            } else if (isRetryableFailure(err, status)) {  // err / 5xx / 408 / 429
                recordFailure(entered);                    // CLOSED→OPEN trip 또는 HALF_OPEN→OPEN + fresh cooldown
                if (enqueueOnFailure) offerToReplayBuffer(path, body);
            } else {                                       // non-retryable 4xx (400/401/403/404 등)
                LOG.warn("...dropped (non-retryable {})...", status);
                recordSuccess(entered);                    // GMS 도달 신호 — HALF_OPEN→CLOSED 허용
                handled.set(true);
            }
        } finally { inflight.release(); }
    });

if (config.synchronousMode() || forceSync) {               // replayer 는 forceSync=true 로 호출
    future.join();
    return handled.get();                                  // success/permanent-rejection 시 true → replayer 가 buffer head 제거
}
return false;                                              // async path 에서는 결과 미확정
```

`HttpClient` 는 `connectTimeout(config.connectTimeout())` 으로 빌드된다. `tryEnterCircuit` / `recordSuccess` / `recordFailure` / `releaseProbeSlot` 은 모두 `circuit.compareAndSet(oldSnap, newSnap)` 의 CAS 루프로 구현되어 state 와 timestamp 의 publish 가 race-free 다.

**Wire 형식 (POST `/openapi/v3/entity/{entityType}`)**

DataHub OpenAPI v3 generic-entity 엔드포인트는 **entity entry 의 JSON 배열** 을 받으며, 각 entry 에서
aspect 이름은 **top-level flat key** (envelope 없음) 이고, 각 aspect 값은 `{ "value": <payload> }` 로
한 번 wrap 된다.

```json
[
  {
    "urn": "urn:li:dataset:(urn:li:dataPlatform:iceberg,cat.ns.tbl,PROD)",
    "datasetProperties": {
      "value": {
        "name": "tbl",
        "qualifiedName": "cat.ns.tbl",
        "customProperties": { "table-uuid": "...", "format-version": "2", "...": "..." },
        "lastModified": { "time": 1715760000000, "actor": "urn:li:corpuser:alice" }
      }
    },
    "container": {
      "value": { "container": "urn:li:container:iceberg.polaris.cat.ns" }
    },
    "dataPlatformInstance": {
      "value": {
        "platform": "urn:li:dataPlatform:iceberg",
        "instance": "urn:li:dataPlatformInstance:(urn:li:dataPlatform:iceberg,polaris)"
      }
    },
    "schemaMetadata": { "value": { "schemaName": "cat.ns.tbl", "...": "..." } },
    "domains":        { "value": { "domains": ["urn:li:domain:my_x0173699_catalog"] } },
    "ownership":      { "value": { "owners": [{ "owner": "urn:li:corpuser:x0173699", "type": "TECHNICAL_OWNER" }] } },
    "status":         { "value": { "removed": false } }
  }
]
```

한 entity 의 여러 aspect 가 **단일 POST 요청** 으로 전송되며, `emitUpsert` 는 호출자가 명시적으로
넣지 않은 경우 `status: { removed: false }` 를 항상 자동 주입한다. 이유: 직전에 `emitStatusRemoved`
로 soft-delete 된 entity 가 drop→recreate 시퀀스로 같은 URN 에 다시 만들어졌을 때 DataHub UI 에서
"removed" 상태로 묶여있는 것을 자동 해제하기 위함 (호출자가 명시적으로 `removed=true` 를 넣으면 그
값을 그대로 보존).

**Same-URN purge on success (`emitSeq` + `purgeBufferedOlderSameUrn`)**

매 emit 은 단조 증가 `emitSeq` 를 부여받아 replay buffer 의 `EmitTask` 에 함께 저장된다. 직접 emit
이 2xx 로 성공하면 그 URN 의 **더 작은 seq** 를 가진 buffered entry 를 purge — 동일 entity 의 더
오래된 상태가 나중에 replay 로 도착해 방금 성공한 상태를 덮어쓰는 것을 차단한다. 같거나 큰 seq 의
entry 는 보존 (그게 더 최신이므로 여전히 replay 되어야 한다). purge 직전에 `stale=true` 마킹을 먼저
해 두므로, replayer 가 이미 peek 한 entry 라도 send 직전에 stale 검사로 skip 한다. 단, 이미
`sendAsync` 로 디스패치된 in-flight 요청은 JDK `HttpClient` 가 cancellation 을 honor 하지 않아 멈출
수 없다 (alarmed 한 경우 `synchronous-mode=true` 가 정답 — Section 4.1 참조).

**비동기 발송 (기본)**

```java
httpClient.sendAsync(request, BodyHandlers.ofString())
    .whenComplete((resp, err) -> {
        if (err != null) LOG.error(...);
        else if (resp.statusCode() / 100 != 2) LOG.warn(...);
    });
// synchronousMode=false: 여기서 반환 → Polaris 요청 처리 계속
// synchronousMode=true: future.join() 으로 DataHub 응답 대기
```

DataHub 장애 시 로그만 남기고 예외를 삼키므로 Polaris 카탈로그 연산에 영향을 주지 않는다.
`gms-url` 미설정 시에도 예외 전파 없이 에러 로그 후 skip한다 (Section 4.1 참조).

### 4.5 DataHubHttpEventListener

`@ApplicationScoped @Identifier("datahub-http")` — `polaris.event-listener.type=datahub-http` 설정 시
선택되는 concrete listener. `AbstractDataHubEventListener` 를 상속해서 emitter/mapper 를 주입한다.

```java
@ApplicationScoped
@Identifier("datahub-http")
public class DataHubHttpEventListener extends AbstractDataHubEventListener {
    @Inject  Clock clock;                  // 테스트에서 시계 swap 가능
    @Context SecurityContext securityContext;  // request-scoped — CDI proxy 가 lazy 주입

    @Inject
    public DataHubHttpEventListener(DataHubConfiguration config) {
        super(new HttpEmitter(config), new DataHubEventMapper(config));
    }

    protected DataHubHttpEventListener() { super(); }  // Quarkus/Arc proxy 용

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
            // SecurityContext 가 request scope 밖에서 throw 할 수 있음 — anonymous fallback
        }
        return new AuditContext(ts, principal);
    }
}
```

- `getAuditContext()` override 로 dataset emit 의 `lastModified.actor` 가 실제 호출자 principal URN
  으로 채워진다 (`urn:li:corpuser:<name>`). 호출자 미식별 시 `urn:li:corpuser:__system__` fallback
  (Section 4.2 의 `auditStamp` 참조).
- `Clock` 은 `@Inject` 라서 테스트에서 fixed Clock 으로 시간 결정성을 확보할 수 있다. 미주입 (e.g.
  abstract base 단위 테스트) 시 `System.currentTimeMillis()` fallback.
- `SecurityContext` 가 background task 등 request scope 밖에서 호출되면 일부 구현이 throw 할 수 있어
  광범위 catch 로 anonymous fallback. 운영 트래픽은 항상 request scope 안에서 실행된다.

---

## 5. 이벤트 흐름 예시: 테이블 생성

```
클라이언트                Polaris                     DataHub
    │                       │                            │
    │── POST /tables ───────►│                            │
    │                       │ onBeforeCreateTable()       │
    │                       │ delegate.createTable()      │
    │                       │ onAfterCreateTable()        │
    │                       │   emitUpsert(              │
    │                       │     datasetUrn,            │
    │                       │     "dataset",             │
    │                       │     aspects)               │
    │                       │────────────────────────────►│ POST /openapi/v3/entity/dataset
    │◄── 200 OK ────────────│                            │ (비동기 — Polaris 응답과 독립)
    │                       │                            │◄── 200 OK
```

---

## 6. 설정 참조

### application.properties (로컬 실행)

```properties
# 리스너 활성화
polaris.event-listener.type=datahub-http

# DataHub GMS 주소 (미설정이면 emit skip, catalog 차단 없음)
polaris.event-listener.datahub.gms-url=http://localhost:8080

# 인증 토큰 (DataHub PAT, 선택)
polaris.event-listener.datahub.token=your-datahub-token

# 플랫폼 인스턴스 식별자 (기본값: polaris)
polaris.event-listener.datahub.platform-instance=polaris

# 환경 레이블 — Dataset URN에 포함 (기본값: PROD)
polaris.event-listener.datahub.env=PROD

# true: 매 emit마다 DataHub 응답을 기다림 (기본값: false)
polaris.event-listener.datahub.synchronous-mode=false

# ── DataHub 장애 격리 가드 (운영 실효 default = DataHubConfiguration 의 @WithDefault) ──
# runtime/defaults/.../application.properties 에는 아래 키들이 주석으로만 표기되어
# 있고 실제 값은 설정되어 있지 않다. 따라서 다음 값이 기본 적용되며, 더 좁히고 싶으면
# env vars 또는 Helm override 로 변경한다.
# polaris.event-listener.datahub.connect-timeout=5s
# polaris.event-listener.datahub.request-timeout=10s
# polaris.event-listener.datahub.max-inflight-emits=1000
# polaris.event-listener.datahub.circuit-breaker-failure-threshold=5
# polaris.event-listener.datahub.circuit-breaker-open-duration=30s
# polaris.event-listener.datahub.replay-buffer-capacity=1000

# Catalog → DataHub Domain URN 매핑.
# DEPRECATED — listener 가 더 이상 참조하지 않는다 (Section 4.1 / 4.2 참조). 3-tier 분류
# (user_catalog / lake_catalog_polaris / etc_polaris) 가 모든 catalog 를 cover. interface 는
# API 호환성 차원에서 유지되지만 설정해도 무시된다.
# polaris.event-listener.datahub.domain-mapping.cat=urn:li:domain:my-domain
```

### Helm values.yaml (Kubernetes 배포)

```yaml
image:
  repository: skthynix/polaris
  tag: "v1.3.0-integration-datahub"   # DataHub 리스너 JAR이 포함된 커스텀 이미지 (필수)

advancedConfig:
  polaris:
    event-listener:
      type: datahub-http
      datahub:
        gms-url: "http://datahub-gms.datahub.svc.cluster.local:8080"
        platform-instance: "polaris"
        env: "PROD"
        synchronous-mode: "false"
        # 장애 격리 가드는 @WithDefault(5s/10s/1000)가 자동 적용되므로 advancedConfig 에
        # 명시할 필요 없다. 환경별로 더 보수적/완화 조정이 필요할 때만 override.
        # 단 SmallRye `validate-unknown=true` 가 ConfigMap 의 file source 키를 root 매핑 단계에서
        # 검증하므로, 운영에서는 ConfigMap 대신 extraEnv (POLARIS_EVENT_LISTENER_DATAHUB_*) 를
        # 사용하는 것이 안전하다 (CLAUDE.md 컨벤션 5번 참조).
        # connect-timeout: "5s"
        # request-timeout: "10s"
        # max-inflight-emits: "1000"

extraEnv:
  - name: POLARIS_EVENT_LISTENER_DATAHUB_TOKEN
    valueFrom:
      secretKeyRef:
        name: datahub-token-secret
        key: token
        optional: true
```

> **주의**: `image.repository / tag` 는 반드시 커스텀 빌드 이미지로 지정해야 한다.
> Quarkus는 CDI 빈을 **빌드 타임**에 인덱싱하므로, 공식 `apache/polaris` 이미지에는 DataHub 리스너 클래스가 존재하지 않는다.

---

## 7. 빌드 및 등록

### Gradle 모듈 등록 (`gradle/projects.main.properties`)

```properties
polaris-extensions-datahub-listener=extensions/datahub-listener
```

### 서버 런타임 포함 (`runtime/server/build.gradle.kts`)

```gradle
runtimeOnly(project(":polaris-extensions-datahub-listener"))
```

`runtimeOnly` 로 등록하는 이유: 서버 모듈은 이 확장 모듈에 컴파일 의존성이 없지만,
모듈 JAR 안에 포함된 Jandex 인덱스(`META-INF/jandex.idx`)를 Quarkus가 런타임 classpath에서 읽어야
CDI 빈으로 등록할 수 있기 때문이다.

---

## 8. 설계 결정 및 근거

| 결정 | 근거 |
|------|------|
| **HTTP 단일 모듈** | 초기에는 SDK(`datahub-client`) 기반 모듈도 함께 보관했으나 SDK jar 의 Avro 1.11.4 가 Polaris classpath 의 Avro 1.12.0 과 충돌(`IncompatibleClassChangeError` 위험). HTTP 모드로 동일 결과를 더 가볍게 달성할 수 있어 SDK 모듈을 제거했다 |
| **비동기 발송 기본** | DataHub 장애가 Polaris 카탈로그 연산의 지연·실패로 전파되어서는 안 된다 |
| **updateNamespaceProperties 후 loadNamespaceMetadata 추가 호출 (`RequiresPostUpdateNamespaceMetadata` opt-in)** | REST 응답은 변경/제거된 키 목록만 가지므로 DataHub 의 `customProperties` 를 갱신하려면 후속 load 가 필요하다. listener 측 marker 로 opt-in 해서 DataHub 외 listener (no-op, aws-cloudwatch, persistence-in-memory-buffer) 는 비용 0 |
| **post-load 실패 시 customProperties 생략 fallback** | load 가 실패해 `currentProperties == null` 인 경우 listener 가 `containerProperties` aspect 를 빼서 DataHub 의 기존 값 덮어쓰기를 방지 |
| **renameTable 후 loadTable 추가 호출 (`RequiresPostRenameTableMetadata` opt-in)** | rename REST 응답은 204 No Content. 목적지 테이블 properties 를 이벤트에 담으려면 명시적 loadTable 호출이 필요. opt-in marker 로 DataHub listener 만 추가 비용 부담 |
| **`AfterRenameTableEvent`/`AfterUpdateNamespacePropertiesEvent` 에 nullable 필드 + 하위호환 생성자** | 새 필드로 메타데이터를 전달하면서, 기존 2/3-arg 생성자를 유지해 이벤트 API 호환성을 보존한다 |
| **namespace join에 raw U+001F 사용** | `CatalogAdapter.decodeNamespace()`가 `URLEncoder.encode()` 후 `RESTUtil.decodeNamespace()`를 호출하므로, 입력은 raw 0x1F여야 한다. `RESTUtil.encodeNamespace()` 사용 시 이중 인코딩(`%251F`)이 발생한다 |
| **namespaceContainerUrn에 빈 네임스페이스 가드** | `Namespace.empty()` 전달 시 `"...cat."` (trailing dot) URN 생성 방지 |
| **no-args 생성자 (protected)** | Quarkus CDI 프록시가 `super()` 를 호출한다. 없으면 빌드 실패 |
| **HttpEmitter가 모든 aspect를 단일 POST로 전송** | DataHub OpenAPI v3는 한 요청에 여러 aspect를 받으므로, aspect당 별도 요청 (= 추가 RTT) 을 보내지 않아도 된다 |
| **marker interface 기반 opt-in (`RequiresPostRenameTableMetadata`, `RequiresPostUpdateNamespaceMetadata`)** | "no-op 만 차단" 방식의 negative 가드 대신 "필요한 listener 만 marker 구현" 방식의 positive 가드. 추가 load 비용은 메타데이터를 실제로 쓰는 listener (현재는 `AbstractDataHubEventListener` 뿐) 만 부담하고, no-op·aws-cloudwatch·persistence-in-memory-buffer 는 비용 0. 새 listener 가 동일 metadata 필요 시 marker 만 구현하면 됨 — capability 가 코드로 명시되어 다중 listener 환경에서도 일반화 부담이 없다. CDI client proxy 도 원본 클래스를 extend 하므로 `instanceof` 가 정상 동작한다 |
| **HttpEmitter 의 connect/request timeout + inflight semaphore** | DataHub 가 down/hang 상태일 때 Polaris 카탈로그 운영에 영향이 없도록 보장한다. timeout 만으로는 동시 미완료 future 누적 가능 → semaphore 로 cap 후 drop. drop 은 listener 측 데이터 누락만 유발하고 catalog 트랜잭션은 정상 진행한다 |
| **HALF_OPEN 단일 probe (CAS-claimed)** | cooldown 직후 N 개 concurrent emit/replay 가 모두 "elapsed > cooldown" 이라 판단해 GMS 로 burst 하는 thundering-herd 차단. CAS 승자 1 명만 통과, 나머지는 가드에서 skip |
| **`CircuitSnapshot(state, openedAtMs)` immutable record + 단일 `AtomicReference`** | state 와 cooldown timestamp 가 별개 변수면 한 thread 가 state 만 갱신한 직후 다른 thread 가 stale timestamp 로 cooldown 만료라 판단하고 즉시 HALF_OPEN claim 하는 race 가능. 묶어서 한 번의 CAS 로 publish → race window 자체 제거 |
| **`recordSuccess(entered)` / `recordFailure(entered)`** | 트립 직전 발송된 async in-flight emit 의 callback 이 늦게 도착해 OPEN 을 강제 CLOSED 로 덮어쓰거나, 다른 emit 의 실패가 HALF_OPEN probe 의 결정을 가로채는 race 차단. HALF_OPEN probe 결과만 명시적으로 상태 전이를 허용 |
| **`releaseProbeSlot()` 시 fresh cooldown 발행** | probe 가 inflight cap full / malformed URL 등으로 schedule 못한 경우 timestamp 를 `System.currentTimeMillis()` 로 갱신 — 그렇지 않으면 다음 emit 이 stale timestamp 를 보고 즉시 다시 HALF_OPEN claim 해서 busy loop 발생 |
| **Replay buffer peek-and-keep** | 단순 `poll()` 후 `enqueueOnFailure=false` 로 재전송했다면 cooldown 직후 첫 probe 실패 시 head item 이 silently 손실. `peek()` 후 sync wait 결과 (성공 또는 permanent-rejection) 일 때만 `poll()` 하여 outage 동안 buffered 이벤트 무손실 보장. drop-oldest 가 head 를 미리 제거한 race 는 `bufferLock` + identity-equality 검증으로 보호 |
| **5xx/408/429 만 retryable 분류** | persistent client-side 오류 (잘못된 schema, 만료 token, 권한 미부여, not-found URN) 가 circuit 를 잘못 OPEN 시키거나 replay buffer 를 영구 미배달 항목으로 가득 채우는 것을 차단. HALF_OPEN probe 가 non-retryable 4xx 를 받으면 GMS 도달 신호로 보고 CLOSED 전이 |
| **dataset emit 에 `schemaMetadata` aspect 포함** | dataset URN 만 떠 있고 UI 에 schema 가 보이지 않으면 운영자가 "DataHub 가 안 보이네" 라고 느끼게 된다. `TableMetadata` 가 있을 때 Iceberg schema 를 DataHub `SchemaField` 리스트로 변환해 함께 emit — nested struct 는 dotted path 로 flatten, 타입은 `schemaFieldType` 매핑 |
| **dataset `customProperties` 에 Iceberg 메타 헤더 폴드** | `table-uuid`, `location`, `format-version`, `current-snapshot-id` 등은 운영자가 DataHub UI 에서 가장 자주 보는 정보. 별도 aspect 없이 customProperties 에 합쳐 emit — DataHub UI 가 자동으로 key-value 테이블로 보여줌. `tableProperties` 와 `tableMetadata` 가 모두 null 일 때만 customProperties 자체를 omit |
| **dataset `lastModified` 에 caller principal URN** | `AuditContext(timestampMs, principal)` 를 `getAuditContext()` 가상 메서드로 주입. HTTP listener 는 `SecurityContext.getUserPrincipal()` 에서 가져오고, abstract base 는 wall-clock + null principal 로 unit-test 가능. null/empty principal 은 `urn:li:corpuser:__system__` 로 fallback (DataHub 가 non-null actor 를 요구함) |
| **3-tier 카탈로그 분류 (user_catalog / lake_catalog_polaris / etc_polaris)** | catalog 별 explicit mapping 보다 (a) 일관성 (모든 catalog 가 정확히 하나의 도메인), (b) 관리 부담 0 (운영자가 새 catalog 마다 매핑 추가할 필요 없음), (c) 운영 명명 컨벤션 활용 (`my_<id>_catalog` 가 곧 owner ID 의 의미를 가짐) 의 이점. 1번 (user_catalog) 은 패턴 매칭으로 catalog 별 자식 Domain 생성, 2/3번 (lake/etc) 은 공유 flat Domain 으로 단순화. 어떤 catalog 도 분류 누락 없이 정확히 한 Domain 에 들어감. catalog/namespace/dataset 모두 동일 분류 적용 (tree-wide) |
| **`ensureCatalogDomain` 으로 Domain entity 자체도 자동 emit** | DataHub `domains` aspect 는 URN 참조만 담는다 → 참조되는 Domain entity 가 없으면 `/domains` UI 페이지에 안 보임. listener 가 모든 upsert 핸들러 시작 부분에서 idempotent emit. 부모 `user_catalog` 만 한 번 수동 사전 생성 (Tier 1 자식들의 parentDomain 대상), 나머지는 자동. operator 의 사전 설정 부담 0 |
| **lake_catalog → 고정 datalake owner / etc → 고정 etc owner (placeholder)** | 시스템성 catalog 는 특정 개인 소유자가 없지만, DataHub UI 가 ownership aspect 의 부재를 "소유자 미할당" 으로 표시하면 운영자 입장에서는 분류 누락처럼 보임. Tier 2/3 에 placeholder corpuser (`datalake`, `etc`) 를 박아 tree 전체가 같은 가상 사용자로 묶여 UI 탐색 일관성 확보. 실 사용자 indication 이 필요한 경우 DataHub UI 에서 수동으로 owner 추가 가능 |
| **Catalog 삭제 시 user_catalog 자식 Domain 만 symmetric soft-delete** | Tier 1 자식 Domain 은 catalog 와 1:1 관계라 catalog 가 없어지면 Domain 도 의미 없음 → orphan 누적 방지를 위해 자동 soft-delete. Tier 2/3 의 공유 Domain (`lake_catalog_polaris`, `etc_polaris`) 은 다른 catalog 도 사용 중일 수 있어 단일 catalog drop 으로 건드리지 않음. 재생성 시 `emitUpsert` 의 status:removed=false 자동 주입으로 부활 |
| **explicit `domain-mapping` 비활성화 (interface 만 유지)** | 3-tier 분류 도입 후 explicit mapping 은 모든 케이스에서 redundant — operator override 가 필요하면 분류 룰을 늘리는 게 낫다. interface 메서드는 backward compat 으로 유지하되 listener 가 무시 (helm chart 에서 이전 키들을 일제히 제거할 필요 없음). 미래 operator override 가 필요하면 새 Tier 로 도입 가능 |
| **TableMetadata 없는 dataset emit 차단 (Iceberg-form 검증, rename 예외)** | "Iceberg 형태가 아닌 데이터는 DataHub 로 전송하지 않는다" 정책. **create/register/update/commit** 경로 (catalog 데이터를 처음 보는 시점) 에서 `TableMetadata == null` 이면 dataset emit 자체를 skip — orphan dataset 이 새기는 것을 차단. **rename 은 예외** — 이벤트 자체가 Iceberg REST API 의 renameTable 에서 fire 되어 destination 이 Iceberg-form 임이 이미 보증되므로, metadata 없어도 minimal aspect (schemaMetadata 만 omit) 로 emit 해서 dataset 이 DataHub 에서 사라지지 않게 한다. drop 은 단순 삭제 신호라 metadata 와 무관하게 항상 emit |
| **namespace container subType = "Namespace"** | 이전엔 `"Schema"` 로 박혔고 DataHub UI 도 "Schema" 로 표시 — Iceberg / Polaris 도메인 용어와 불일치. `containerProperties.description` 과 `subTypes.typeNames` 둘 다 `"Namespace"` 로 통일 |
| **dataset emit 에 `subTypes("Table")` 명시 부착** | aspect 없으면 DataHub UI 가 첫 emit (bootstrap) 에는 Table 로 표시하다가 후속 upsert 부터 generic "Dataset" 으로 fallback 하는 버그성 동작. 모든 dataset upsert 에 `subTypes: { typeNames: ["Table"] }` 을 명시 부착해 일관 표시 |
| **`emitUpsert` 가 `status: removed=false` 자동 주입** | drop → 같은 URN 재생성 (catalog/namespace/table) 시 DataHub UI 에 "removed" 표식이 stick 되는 것을 자동 복구. 호출자가 명시적으로 status aspect 를 제공했을 때 (`emitStatusRemoved` 등) 는 그 값을 보존 (test: `emitUpsertPreservesCallerProvidedStatusAspect`) |
| **`emitSeq` 기반 same-URN purge** | direct emit 이 2xx 로 성공하면 동일 URN 의 *더 오래된* (smaller seq) buffered entry 를 purge — stale state 가 나중에 replay 로 도착해 최신 state 를 덮어쓰는 것을 차단. *같거나 큰* seq 는 보존 (그게 더 최신이므로 여전히 replay 필요). 동시에 `stale=true` 마킹으로 replayer 가 peek 한 entry 도 send 전에 skip |
| **`synchronous-mode=true` 가 strict per-URN ordering 의 escape hatch** | async 모드는 (1) HTTP/2 multiplexing 으로 동일 URN emit 이 재정렬될 수 있고 (2) `sendAsync` 디스패치 후 stale replay 가 멈출 수 없는 한계가 있다. 둘 다 한 catalog request 안에서 emit 을 직렬화하면 사라지므로, ordering 이 데이터 정합성에 중요한 워크로드는 이 knob 으로 latency 와 trade-off 한다 |
| **`my_<id>_catalog` 패턴은 코드에 하드코딩** | 운영 명명 컨벤션은 한 곳에만 존재하고 환경별 가변값이 없다. 설정 노출 시 키 3개 (regex / domain-urn / owner-urn-template) 가 늘고 SmallRye `validate-unknown` / 헬름 env vars 도 따라온다. 미래에 다른 패턴이 더 필요해지면 그때 일반화 (config-driven CatalogClassifier) — 지금은 YAGNI |
| **패턴 매칭 catalog 는 container 에도 `domains` + `ownership` 부착** | 기본 design 은 "container 에 domain 미부착" (explicit mapping 이 dataset 한정이라 의도 왜곡 방지). 그러나 `my_*_catalog` 는 *트리 전체가 한 도메인/소유자* 라는 의미가 catalog 이름 자체에서 명확하므로, UI 의 lineage·탐색에서 일관성을 위해 catalog/namespace container 까지 동일 aspect 부착. 이 완화는 패턴 매칭에 한정되어 일반 explicit mapping 의 의도 보존과 양립 |

---

## 9. 테스트 구조

```
datahub-listener/src/test/
└── .../datahub/
    ├── DataHubEventMapperTest.java            (45 tests)
    │   ── URN 생성·escape·empty namespace 가드, dataset/catalog/namespace aspect 구조,
    │      중첩 네임스페이스 계층, null properties 보존, U+001F 인코딩 round-trip,
    │      level 안의 literal '.' / ',' / '(' / ')' / '%' escape 회귀,
    │      `schemaMetadata` aspect (primitive 타입 매핑 + nested struct flatten),
    │      `customProperties` 의 Iceberg 메타 헤더 (table-uuid, format-version 등),
    │      `lastModified` audit stamp (principal → actor URN, null → __system__),
    │      3-tier 카탈로그 분류:
    │        - Tier 1 (user_catalog): `my_<alnum>_catalog` 매칭 시 자식 Domain URN +
    │          중간 segment 전체를 corpuser id 로 부착 (`my_x01100_catalog` → `x01100`)
    │        - Tier 2 (lake_catalog): 정확히 `lake_catalog` 일 때 lake_catalog_polaris +
    │          corpuser:datalake (flat domain, no parentDomain)
    │        - Tier 3 (etc): 그 외 모두 etc_polaris + corpuser:etc
    │      catalog/namespace/dataset 모두에 tree-wide 적용 검증, 자식 Domain aspect 의
    │      parentDomain 검증, isUserCatalog/isLakeCatalog 가드, underscore/empty/
    │      non-alphanum 미매칭 → etc fallback 회귀
    ├── AbstractDataHubEventListenerTest.java  (15 tests)
    │   ── 이벤트 종류별 emitter 호출 검증 (RecordingEmitter 사용):
    │      각 upsert 핸들러가 [Domain entity, 주 entity] 순으로 두 번 emit 하는지
    │      (createNamespace / commitTable / renameTable),
    │      Tier 1 매칭 시 자식 Domain URN + parentDomain=user_catalog,
    │      Tier 2 (lake_catalog) 매칭 시 lake_catalog_polaris flat Domain + datalake owner,
    │      Tier 3 (regular_cat 등) 매칭 시 etc_polaris flat Domain + etc owner,
    │      onAfterUpdateNamespaceProperties 의 currentProperties pass-through 및
    │      null fallback (containerProperties omit),
    │      onAfterDropNamespace 가 raw U+001F namespace string 을 normalize 해 동일 URN
    │      산출,
    │      onAfterCommitTransaction 의도적 미override 회귀
    │      (`commitTransactionMustRemainNoOpToAvoidWipingCustomProperties`,
    │       `abstractDataHubEventListenerDoesNotOverrideOnAfterCommitTransaction`),
    │      Catalog drop symmetric soft-delete: Tier 1 은 자식 Domain 도 함께 removed,
    │      Tier 2/3 (lake/regular) 은 공유 Domain 미터치
    └── HttpEmitterTest.java                   (22 tests)
        ── 실제 JDK HttpServer로 wire 포맷 검증 (Authorization 헤더, JSON 배열 +
           flat aspect key 구조, 호출자 명시 status 보존, gms-url 미설정 시 skip),
           inflight backpressure drop, sync 모드 timeout, malformed gms-url permit
           release, invalid request-timeout permit release; 그리고 circuit breaker /
           replay buffer 회귀 보호:
           ── circuitOpensAfterConsecutiveFailuresAndSkipsSubsequentEmits
           ── circuitClosesAfterCooldownAndSuccessfulProbe
           ── circuitReopensAfterCooldownAndFailedProbe
           ── halfOpenAllowsOnlyOneProbeUnderConcurrentEmits
           ── staleInflightSuccessDoesNotForceClosedOverOpenCircuit
           ── releaseProbeSlotRefreshesCooldownWhenProbeFailsToSchedule
           ── nonRetryable4xxDoesNotOpenCircuitOrBufferForReplay
           ── retryable429OpensCircuitAndBuffersForReplay
           ── replayBufferRedeliversAfterDataHubRecovers
           ── replayBufferSurvivesFailedProbeAndDeliversAfterEventualRecovery
           ── replayBufferDropsOldestWhenFull
           ── successfulDirectEmitPurgesStaleSameUrnEntriesFromReplayBuffer
           ── purgeOnSuccessPreservesNewerSameUrnBufferedEntriesByExecutionSequence
           ── replayerSkipsBufferedEntryMarkedStaleByConcurrentPurgeBeforeSend
```

총 **82개 테스트** (= 45 + 15 + 22). `./gradlew :polaris-extensions-datahub-listener:test` 로 전수 실행된다.
CLAUDE.md 가 인용하는 49 개와 차이가 나는 것은 (a) schemaMetadata / customProperties / audit 관련
mapper 회귀, (b) namespace currentProperties pass-through + drop-namespace raw-string 회귀, 그리고
(c) 3-tier 카탈로그 분류 (user_catalog 자식 + lake_catalog_polaris + etc_polaris) 와 catalog drop
시 자식 Domain symmetric soft-delete 회귀가 이후 추가되었기 때문이다.

**테스트 실행**

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./gradlew :polaris-extensions-datahub-listener:test
```

---

## 10. 알려진 제약

- **이벤트 재전송 — 부분적 보전 (in-memory)**: replay buffer (default 1000 events) 가 일시 outage 동안의 이벤트를 보전한다. 단 (1) 큐 가득 시 drop-oldest, (2) 서버 재시작 시 큐 손실. 영구 보전이 필요하면 별도 outbox 설계가 필요하다.
- **View 이벤트 미처리**: Iceberg View(`AfterCreateViewEvent` 등)는 현재 DataHub로 동기화되지 않는다.
- **초기 일괄 동기화 없음**: 리스너 활성화 이전에 존재하던 카탈로그·테이블은 DataHub에 나타나지 않는다. 초기 동기화가 필요하면 별도 스크립트가 필요하다.
- **중복 upsert**: 동일 URN에 대해 여러 번 upsert가 발생할 수 있다 (예: `onAfterUpdateTable` + `onAfterCommitTable`). DataHub가 멱등적으로 처리하므로 데이터 정합성에는 문제없다.
- **post-load 부수효과 (DataHub listener 활성 시)**: (Section 2.1 참조) rename / updateProperties 후 marker 기반 추가 load 호출이 동반된다. 인가 분리 시 메타데이터 손실 가능 (DEBUG 로그만), 1차 변경과 후속 load 사이 race condition 시 stale 메타데이터 emit 가능, 매 호출마다 metadata fetch + 권한평가 1회 추가 비용. `no-op` 등 marker 미구현 listener 활성 시에는 추가 호출 자체가 없다.
- **4xx 분류의 단순화**: `isRetryableFailure` 는 5xx, 408, 429 만 retryable 로 분류한다. 그 외 4xx (422 Unprocessable Entity, 451 Unavailable for Legal Reasons 등 포함) 는 모두 non-retryable 로 보고 replay buffer 에 들어가지 않는다. 의도된 동작 (영구 거절 항목이 buffer 를 점유하지 않게) 이지만, DataHub 가 일시적으로 422 를 반환한 후 곧 복구되는 운영 환경에서는 이벤트 손실로 보일 수 있음.
- **`releaseProbeSlot()` 의 cooldown reset**: probe 가 schedule 실패 (inflight cap full, build error) 한 경우 fresh cooldown 으로 OPEN 갱신한다. 실제 GMS 응답을 받은 게 아니므로 cooldown 초기화는 약간 보수적인 선택 — DataHub 가 정상인데 우리 측 일시 문제로 schedule 못한 경우에도 다음 cooldown 만큼 추가 대기. busy loop 차단의 trade-off 이며, 빈도가 낮으면 영향은 작음.
- **explicit `domain-mapping` config 비활성화**: `DataHubConfiguration.domainMapping()` 메서드와 그에 해당하는 env vars / properties 키는 backward compat 으로 interface 에 남아있으나 listener 가 더 이상 참조하지 않는다 (3-tier 분류로 대체됨). 이전에 설정한 키가 있으면 silently ignore — helm chart / properties 에서 제거 권장. 운영자별 catalog override 가 필요하면 새 Tier 추가 형태의 코드 변경이 필요.
- **`user_catalog` 부모 Domain 의 수동 사전 생성**: 3-tier 분류의 Tier 1 자식 Domain 들이 `parentDomain = urn:li:domain:user_catalog` 를 가리키므로, 운영 시작 전에 GMS REST POST 로 부모 Domain entity 를 한 번 만들어야 UI 에서 계층 구조가 정상 표시된다 (listener 가 자동 생성하지 않음 — 부모는 1개뿐이고 잘못된 description / name 으로 들어가면 운영자가 직접 정정해야 하기 때문에 의도적으로 수동 단계로 분리). Tier 2/3 의 공유 Domain (`lake_catalog_polaris`, `etc_polaris`) 은 listener 가 자동 생성.
- **TableMetadata 없는 케이스의 dataset 누락 (create/register/update/commit 만)**: Iceberg-form 검증 정책상 첫-인식 경로 (create/register/update/commit) 에서 metadata 가 없으면 emit 자체가 skip 된다. 정상 경로는 항상 metadata 동반이라 거의 발생하지 않음. **rename 은 예외 처리 적용** — post-rename loadTable 이 실패해도 destination dataset 은 minimal aspect 로 항상 emit 되어 DataHub 에서 사라지지 않는다 (단 schemaMetadata 는 다음 commit 이벤트 때 채워짐, log 는 WARN).
