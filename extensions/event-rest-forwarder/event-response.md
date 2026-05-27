# Polaris Event REST Forwarder — Receiver 가 받는 응답값 (Envelope) 정의

각 catalog operation 발생 시 listener (`@Identifier("rest-forwarder")`) 가 `endpoint-url` 로
`POST` 하는 JSON envelope 의 정확한 형태입니다. `@JsonInclude(NON_NULL)` 가 적용되어
**null 필드는 wire 에서 제외**되므로, 각 케이스별로 실제 도착하는 키 집합은 아래 표와 같습니다.

전송 형태:
```
POST <endpoint-url>
Content-Type: application/json
Accept:       application/json
Authorization: Bearer <token>          # config.token 설정 시
```

Body 는 **단일 envelope JSON object** (배열 아님 — listener 는 batching 안 함).

공통 헤더 필드 (모든 envelope 에 항상 포함):

| 필드 | 타입 | 의미 |
|---|---|---|
| `eventId` | string (UUID v4) | 환제드라 — receiver 의 idempotency key. listener-side retry 에도 stable |
| `sequenceNumber` | long | per-Polaris-pod monotonic counter (1+). pod 재시작 시 1부터 |
| `eventType` | string | 이벤트 종류 (이 문서의 각 절 제목) |
| `timestampMs` | long | listener handler 진입 시점의 epoch ms |
| `actor` | string \| null | `SecurityContext.getUserPrincipal().getName()`; 비인증 시 null |
| `realm` | string \| null | `RealmContext.getRealmIdentifier()`; 단일 realm 환경에선 null 가능 |

---

## 1. Catalog 이벤트

### 1.1 Create Catalog

**트리거**: `POST /api/management/v1/catalogs`
**이벤트 타입**: `AfterCreateCatalog`

```json
{
  "eventId": "3e467299-dfd9-48ad-b044-bcd7c9d765f1",
  "sequenceNumber": 1,
  "eventType": "AfterCreateCatalog",
  "timestampMs": 1779763436883,
  "actor": "root",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog",
  "properties": {
    "default-base-location": "s3://data-catalog-bucket/smoke"
  }
}
```

| 필드 | 출처 |
|---|---|
| `catalog` | `event.catalog().getName()` |
| `properties` | `event.catalog().getProperties().toMap()` — catalog 의 user-set properties 전체 |

> **참고**: storage config / type / connection info 등은 envelope 에 포함되지 않음.
> receiver 가 추가로 필요하면 별도 `GET /api/management/v1/catalogs/{name}` 호출.

---

### 1.2 Update Catalog

**트리거**: `PUT /api/management/v1/catalogs/{catalog}`
**이벤트 타입**: `AfterUpdateCatalog`

```json
{
  "eventId": "8c4f1a3b-2d7e-4f8a-9c1b-5d6e7f8a9b0c",
  "sequenceNumber": 5,
  "eventType": "AfterUpdateCatalog",
  "timestampMs": 1779763500123,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog",
  "properties": {
    "default-base-location": "s3://data-catalog-bucket/smoke",
    "owner": "team-x"
  }
}
```

`AfterCreateCatalog` 와 envelope 구조 동일 (eventType 만 다름). `properties` 는 update 후
catalog 의 **현재 전체 properties** 를 가진다 (delta 가 아님).

---

### 1.3 Delete Catalog

**트리거**: `DELETE /api/management/v1/catalogs/{catalog}`
**이벤트 타입**: `AfterDeleteCatalog`

```json
{
  "eventId": "f1e2d3c4-b5a6-7980-1234-567890abcdef",
  "sequenceNumber": 12,
  "eventType": "AfterDeleteCatalog",
  "timestampMs": 1779764000456,
  "actor": "root",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog"
}
```

`properties` 미포함 — `AfterDeleteCatalogEvent` 는 이름만 가지고 있어 catalog 객체 자체가
없다. Receiver 는 이전에 본 `AfterCreateCatalog` envelope 로 메타데이터를 알아야 함.

---

## 2. Namespace 이벤트

### 2.1 Create Namespace

**트리거**: `POST /api/catalog/v1/{catalog}/namespaces`
**이벤트 타입**: `AfterCreateNamespace`

```json
{
  "eventId": "11111111-2222-3333-4444-555555555555",
  "sequenceNumber": 2,
  "eventType": "AfterCreateNamespace",
  "timestampMs": 1779763500000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog",
  "namespace": ["ns", "sub"],
  "properties": {
    "owner": "team-x"
  }
}
```

| 필드 | 출처 |
|---|---|
| `namespace` | `event.namespace().levels()` — 중첩 네임스페이스 levels 배열 |
| `properties` | `event.namespaceProperties()` — namespace 의 user-set properties |

> **단일 레벨 namespace**: `"namespace": ["ns"]` 같이 1개 요소 배열.
> **빈 namespace**: 발생하지 않음 (Iceberg REST 에서 namespace 생성 시 비어있을 수 없음).

---

### 2.2 Update Namespace Properties

**트리거**: `POST /api/catalog/v1/{catalog}/namespaces/{ns}/properties`
**이벤트 타입**: `AfterUpdateNamespaceProperties`

#### (a) 정상 — post-load 성공

```json
{
  "eventId": "22222222-3333-4444-5555-666666666666",
  "sequenceNumber": 6,
  "eventType": "AfterUpdateNamespaceProperties",
  "timestampMs": 1779763600000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog",
  "namespace": ["ns"],
  "properties": {
    "owner": "team-x",
    "comment": "updated"
  },
  "propertyUpdates": {
    "updated": ["comment"],
    "removed": ["legacy_flag"]
  }
}
```

| 필드 | 출처 |
|---|---|
| `properties` | `event.currentProperties()` — `RequiresPostUpdateNamespaceMetadata` marker 로 추가 `loadNamespaceMetadata` 호출 결과. **update 후 namespace 의 전체 properties** |
| `propertyUpdates.updated` | `updateNamespacePropertiesResponse.updated()` — REST 응답이 알려주는 새로/덮어쓴 키 목록 |
| `propertyUpdates.removed` | `updateNamespacePropertiesResponse.removed()` — REST 응답이 알려주는 삭제된 키 목록 |

#### (b) post-load 실패 — receiver 는 기존 state 보존

```json
{
  "eventId": "22222222-3333-4444-5555-666666666666",
  "sequenceNumber": 6,
  "eventType": "AfterUpdateNamespaceProperties",
  "timestampMs": 1779763600000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog",
  "namespace": ["ns"],
  "propertyUpdates": {
    "updated": ["comment"],
    "removed": ["legacy_flag"]
  }
}
```

`properties` 필드가 **누락** (`@JsonInclude(NON_NULL)`). `currentProperties == null` 이면
receiver 는 "load 실패" 로 인식해 기존 properties 를 유지하고 `propertyUpdates` 의 diff
만 적용해야 함 (empty map 과 구별 필수).

---

### 2.3 Delete Namespace

**트리거**: `DELETE /api/catalog/v1/{catalog}/namespaces/{ns}`
**이벤트 타입**: `AfterDropNamespace`

```json
{
  "eventId": "33333333-4444-5555-6666-777777777777",
  "sequenceNumber": 7,
  "eventType": "AfterDropNamespace",
  "timestampMs": 1779763700000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_smoke_test_catalog",
  "namespaceRaw": "nssub"
}
```

| 필드 | 출처 |
|---|---|
| `namespaceRaw` | `event.namespace()` — `AfterDropNamespaceEvent` 는 namespace 를 **raw String** 으로 전달 (Iceberg REST 의 U+001F 구분자 그대로). 즉 `nssub` 형태. receiver 가 split 결정 |

> **`namespace` 필드 없음** — drop event 만 raw 보존 (다른 namespace 이벤트는 levels 배열).
> 위 예시의 `"nssub"` 은 ns + U+001F + sub 인데 markdown 에서 U+001F 가 보이지 않게 표기됨.

---

## 3. Table 이벤트

### 3.1 Create Table

**트리거**: `POST /api/catalog/v1/{catalog}/namespaces/{ns}/tables`
**이벤트 타입**: **`AfterCommitTable` + `AfterCreateTable`** (2개 — Iceberg 내부 commit + REST wrapper 둘 다 fire)

#### 첫 envelope: `AfterCommitTable` (Iceberg internal commit)

```json
{
  "eventId": "44444444-5555-6666-7777-888888888888",
  "sequenceNumber": 7,
  "eventType": "AfterCommitTable",
  "timestampMs": 1779763850000,
  "actor": "root",
  "realm": "POLARIS",
  "catalog": "my_e2e_smoke_catalog",
  "namespace": ["e2e"],
  "table": "events",
  "tableMetadataJson": "{\"format-version\":2,\"table-uuid\":\"aeb93eab-8616-44fc-9536-83e8f727232b\",\"location\":\"s3://data-catalog-bucket/my_e2e_smoke_catalog/e2e/events\",\"last-sequence-number\":0,\"last-column-id\":2,\"current-schema-id\":0,\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"event_id\",\"required\":true,\"type\":\"string\"},{\"id\":2,\"name\":\"ts\",\"required\":true,\"type\":\"timestamptz\"}]}],\"default-spec-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],\"last-partition-id\":999,\"default-sort-order-id\":0,\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{\"owner\":\"smoke-team\"},\"current-snapshot-id\":-1,\"refs\":{},\"snapshots\":[],\"statistics\":[],\"snapshot-log\":[],\"metadata-log\":[]}"
}
```

#### 두 번째 envelope: `AfterCreateTable` (REST wrapper)

```json
{
  "eventId": "1e0a1f8d-6a55-475f-896b-6b5ad0f0cbd2",
  "sequenceNumber": 8,
  "eventType": "AfterCreateTable",
  "timestampMs": 1779763850810,
  "actor": "root",
  "realm": "POLARIS",
  "catalog": "my_e2e_smoke_catalog",
  "namespace": ["e2e"],
  "table": "events",
  "tableMetadataJson": "{\"format-version\":2,\"table-uuid\":\"aeb93eab-8616-44fc-9536-83e8f727232b\", ... (위와 동일 또는 거의 동일)}"
}
```

| 필드 | 출처 |
|---|---|
| `namespace` | `event.namespace().levels()` (Iceberg `Namespace`) |
| `table` | `event.tableName()` |
| `tableMetadataJson` | `TableMetadataParser.toJson(loadTableResponse.tableMetadata())` — Iceberg 공식 canonical JSON. receiver 는 `TableMetadataParser.fromJson(json)` 으로 round-trip 가능 |

> **왜 2개?**: REST API `createTable` 이 내부적으로 Iceberg `TableOperations.commit()` 을
> 호출하면서 commit 이벤트가 먼저 fire, 그 후 REST 핸들러가 create 이벤트를 fire.
> 두 envelope 의 `tableMetadataJson` 은 동일한 상태를 가리킴 (대부분 같지만 timestamp /
> last-updated-ms 등이 미세 차이 가능).
>
> **Receiver dedup**: `eventId` 가 다르므로 dedup table 만으로는 구분 안 됨. eventType +
> (catalog, namespace, table) 로 구분 → "이미 이 테이블의 create 본 적 있다" 면 commit 만
> 적용. 또는 그냥 모두 적용 (멱등성).

---

### 3.2 Update Table

**트리거**: `POST /api/catalog/v1/{catalog}/namespaces/{ns}/tables/{table}`
**이벤트 타입**: **`AfterCommitTable` + `AfterUpdateTable`** (2개)

#### 첫 envelope: `AfterCommitTable`

위 3.1 의 commit envelope 와 동일 구조. `tableMetadataJson` 은 **update 후의 새 metadata**.

#### 두 번째 envelope: `AfterUpdateTable`

```json
{
  "eventId": "55555555-6666-7777-8888-999999999999",
  "sequenceNumber": 10,
  "eventType": "AfterUpdateTable",
  "timestampMs": 1779763900000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_e2e_smoke_catalog",
  "namespace": ["e2e"],
  "table": "events",
  "tableMetadataJson": "{\"format-version\":2,\"table-uuid\":\"aeb93eab-...\",\"last-column-id\":3,\"current-schema-id\":1, ... (schema 변경 반영)}"
}
```

`AfterUpdateTable` envelope 의 `table` 필드는 `event.sourceTable()` (=업데이트 대상 테이블
이름). `tableMetadataJson` 은 `RequiresPostRenameTableMetadata` 와 같은 패턴으로 update 직후
load 한 결과 (commitTableRequest 적용 후 상태).

---

### 3.3 Delete Table

**트리거**: `DELETE /api/catalog/v1/{catalog}/namespaces/{ns}/tables/{table}`
**이벤트 타입**: `AfterDropTable`

```json
{
  "eventId": "66666666-7777-8888-9999-aaaaaaaaaaaa",
  "sequenceNumber": 15,
  "eventType": "AfterDropTable",
  "timestampMs": 1779764000000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_e2e_smoke_catalog",
  "namespace": ["e2e"],
  "table": "events",
  "purgeRequested": true
}
```

| 필드 | 출처 |
|---|---|
| `table` | `event.table()` (String) |
| `purgeRequested` | `event.purgeRequested()` — REST 요청의 `?purgeRequested=true` 파라미터. metadata + 데이터 파일도 함께 삭제하는지 |

> **`tableMetadataJson` 없음** — drop 은 단순 삭제 신호라 metadata 미동봉. receiver 는
> 이전 create/update/commit envelope 들로 알던 마지막 state 를 삭제.

---

## 부록 — 변형 이벤트

`create/update/delete` 의 9개 외에, 동일 카테고리에 속하는 **3개 변형 이벤트** 도 forward
됩니다 (`extensions/event-rest-forwarder/DESIGN.md §2.2` 참조).

### A. Register Table (= "외부 metadata 를 catalog 에 등록" — create 변형)

**트리거**: `POST /api/catalog/v1/{catalog}/namespaces/{ns}/register`
**이벤트 타입**: `AfterRegisterTable`

```json
{
  "eventId": "77777777-8888-9999-aaaa-bbbbbbbbbbbb",
  "sequenceNumber": 20,
  "eventType": "AfterRegisterTable",
  "timestampMs": 1779764100000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_e2e_smoke_catalog",
  "namespace": ["e2e"],
  "table": "imported_events",
  "tableMetadataJson": "{\"format-version\":2,\"table-uuid\":\"...\", ...}"
}
```

`AfterCreateTable` 과 동일 envelope 형태 (eventType 만 다름). receiver 는 두 가지를 같은
"새 table 등장" 으로 처리해도 무방.

---

### B. Rename Table (= 이름 변경 — update 변형, source + destination 동시 영향)

**트리거**: `POST /api/catalog/v1/{catalog}/tables/rename`
**이벤트 타입**: `AfterRenameTable`

```json
{
  "eventId": "88888888-9999-aaaa-bbbb-cccccccccccc",
  "sequenceNumber": 22,
  "eventType": "AfterRenameTable",
  "timestampMs": 1779764200000,
  "actor": "alice",
  "realm": "POLARIS",
  "catalog": "my_e2e_smoke_catalog",
  "namespace": ["e2e"],
  "table": "events_old",
  "tableMetadataJson": "{\"format-version\":2,\"table-uuid\":\"...\", ...}",
  "renameTo": {
    "namespace": ["e2e", "archive"],
    "name": "events_2024"
  }
}
```

| 필드 | 출처 |
|---|---|
| `namespace`, `table` | rename **source** identifier |
| `renameTo.namespace`, `renameTo.name` | rename **destination** identifier |
| `tableMetadataJson` | destination 의 post-rename metadata (`RequiresPostRenameTableMetadata` marker 로 추가 loadTable). post-load 실패 시 누락 |

> **Replay buffer 동작**: rename envelope 은 source 와 destination **두 resource 모두 영향**
> 으로 인식됨 (`EventEnvelope.resourceKeys()` 가 2 키 반환). receiver 가 buffered rename 을
> dedup 할 때, destination 에 들어온 newer commit 이 rename 을 evict 함.

---

### C. Commit Table (= Iceberg internal — update 의 일부, 단독 발생도 가능)

**트리거**: 위 3.1, 3.2 의 일부 (단일 commit 만 일으키는 API 호출은 드묾)
**이벤트 타입**: `AfterCommitTable`

3.1 의 첫 envelope 참조. 단독으로 보일 수 있는 경우:
- multi-table `commitTransaction` (n 개 테이블 변경) → atomic update 성공 시 `n` 개의
  `AfterCommitTable` 가 한꺼번에 drain (`TransactionalCommitTableDeferred` marker 동작).
- **rollback 시에는 단 하나도 fire 되지 않음** — receiver 는 트랜잭션이 실패한 것을
  보지 못함 (Polaris 가 적용 안 한 상태이므로 안 보여도 일관적).

---

## 응답 status 분류 (receiver → listener)

Listener 의 `HttpEventPoster.isRetryableFailure` 정책. receiver 가 반환할 HTTP status 별
listener 동작:

| Status | 분류 | Listener 동작 |
|---|---|---|
| `2xx` | success | 정상 — circuit close, same-resource older-seq buffered entry 즉시 purge |
| `5xx` (500/502/503/504) | retryable | 회로 failure 카운트 + replay buffer 적재 |
| `408 Request Timeout` | retryable | 동상 |
| `429 Too Many Requests` | retryable | 동상 (backpressure 신호) |
| 그 외 `4xx` (400/401/403/404/422 ...) | non-retryable | WARN 로그 + 즉시 drop (재시도 무의미). HALF_OPEN probe 시 도달 신호로 처리하여 circuit close |

Receiver 는 일시 장애 시 5xx, payload 영구 거절 시 4xx, 인증 실패 시 401/403 을 반환하면
listener 의 5 격리 가드가 정확히 동작합니다.

---

## 요약 표

| API | eventType | 발생 횟수 | 핵심 필드 |
|---|---|---|---|
| `POST /management/v1/catalogs` | `AfterCreateCatalog` | 1 | `catalog`, `properties` |
| `PUT /management/v1/catalogs/{c}` | `AfterUpdateCatalog` | 1 | `catalog`, `properties` |
| `DELETE /management/v1/catalogs/{c}` | `AfterDeleteCatalog` | 1 | `catalog` |
| `POST /catalog/v1/{c}/namespaces` | `AfterCreateNamespace` | 1 | `catalog`, `namespace[]`, `properties` |
| `POST /catalog/v1/{c}/namespaces/{ns}/properties` | `AfterUpdateNamespaceProperties` | 1 | `catalog`, `namespace[]`, `properties`, `propertyUpdates` |
| `DELETE /catalog/v1/{c}/namespaces/{ns}` | `AfterDropNamespace` | 1 | `catalog`, `namespaceRaw` |
| `POST /catalog/v1/{c}/namespaces/{ns}/tables` (create) | `AfterCommitTable` + `AfterCreateTable` | **2** | `catalog`, `namespace[]`, `table`, `tableMetadataJson` |
| `POST /catalog/v1/{c}/namespaces/{ns}/tables/{t}` (update) | `AfterCommitTable` + `AfterUpdateTable` | **2** | `catalog`, `namespace[]`, `table`, `tableMetadataJson` |
| `DELETE /catalog/v1/{c}/namespaces/{ns}/tables/{t}` | `AfterDropTable` | 1 | `catalog`, `namespace[]`, `table`, `purgeRequested` |
| `POST /catalog/v1/{c}/namespaces/{ns}/register` | `AfterRegisterTable` | 1 | `catalog`, `namespace[]`, `table`, `tableMetadataJson` |
| `POST /catalog/v1/{c}/tables/rename` | `AfterRenameTable` | 1 | `catalog`, `namespace[]`, `table`, `renameTo{}`, `tableMetadataJson` |
| `POST /catalog/v1/{c}/transactions/commit` (multi-table) | `AfterCommitTable` × N | N | per-table, transaction 성공 후에만 일괄 drain |
| **조회** (GET / List / CheckExists / Load / Refresh) | (없음) | 0 | 의도적 미override |
