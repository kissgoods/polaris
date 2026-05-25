# DataHub Event Listener — 요약

상세는 [`DESIGN.md`](datahub-listener-design.md). 본 문서는 **운영자가 알아야 할 핵심** 만 추렸다.

---

## 1. 무엇을 하는가

Polaris (Iceberg REST Catalog) 의 catalog / namespace / table CRUD 이벤트를 DataHub 의 Container / Dataset
엔티티로 실시간 동기화한다. listener 가 비활성이면 (`event-listener.type=no-op`) 아무 동작 없음.

```
Polaris HTTP API
   ↓
IcebergRestCatalogEventServiceDelegator (CDI Decorator)
   ↓
AbstractDataHubEventListener  (Identifier: "datahub-http")
   ↓
HttpEmitter → DataHub GMS  (POST /openapi/v3/entity/{type})
```

---

## 2. 3-tier 카탈로그 분류 (핵심 비즈니스 룰)

모든 catalog/namespace/dataset emit 에 **정확히 하나의 도메인 + 하나의 owner** 가 부착된다.

| Tier | 매칭 조건 | Domain URN | Owner |
|---|---|---|---|
| 1 | catalog 이름 `^my_([a-zA-Z0-9]+)_catalog$` | `urn:li:domain:<catalogName>` (자식 — parent: `user_catalog`) | `corpuser:<중간 segment>` |
| 2 | catalog 이름 `lake_catalog` | `urn:li:domain:lake_catalog_polaris` (flat) | `corpuser:datalake` |
| 3 | 그 외 모두 | `urn:li:domain:etc_polaris` (flat) | `corpuser:etc` |

**예시**:
- `my_x01100_catalog` → 자식 도메인 `urn:li:domain:my_x01100_catalog`, owner `x01100`
- `my_42_catalog` → 자식 도메인 `urn:li:domain:my_42_catalog`, owner `42`
- `lake_catalog` → `lake_catalog_polaris`, owner `datalake`
- `datalake_catalog`, `regular_cat` 등 → `etc_polaris`, owner `etc`

**Domain entity 자동 생성**: listener 가 모든 upsert 핸들러에서 `ensureCatalogDomain()` 으로 idempotent
emit. operator 사전 작업은 **`urn:li:domain:user_catalog` 부모 entity 한 번 생성** 뿐 (Tier 1 자식들의
`parentDomain` 이 가리키는 대상).

**Drop 동작**: Tier 1 catalog drop 시 자식 Domain 도 symmetric soft-delete. Tier 2/3 공유 Domain 은
미터치 (다른 catalog 가 쓸 수 있음).

**Iceberg-form 검증**: `create/register/update/commit` 경로 (catalog 데이터를 처음 보는 시점) 에서
`TableMetadata` 가 없으면 dataset emit 자체를 skip — Iceberg 형태가 아닌 데이터는 DataHub 에 등록하지
않는다. **rename 은 예외** — 이벤트 자체가 Iceberg REST API 에서 fire 되어 destination 이 Iceberg-form
임이 보증되므로 metadata 없어도 minimal aspect (schemaMetadata 만 omit) 로 emit. drop 은 단순 삭제
신호라 metadata 와 무관하게 항상 emit.

**Entity subType 명시**: DataHub UI 가 일관된 이름으로 표시되도록 모든 emit 에 subType aspect 부착.

| Entity | subType |
|---|---|
| Catalog Container | `Catalog` |
| Namespace Container | `Namespace` (이전: "Schema" — Iceberg/Polaris 용어와 일치하도록 변경) |
| Dataset | `Table` (없으면 후속 upsert 부터 generic "Dataset" 으로 fallback 되는 문제 회피) |

---

## 3. 각 도메인별 emit 방법

emit 은 **catalog 이름만으로 자동 분류** 된다 — 코드 수정·API 변경 없이 catalog 명명만으로 트리거.
catalog 가 만들어지는 순간 listener 가 해당 tier 의 Domain entity 를 자동 등록하고, 이후 그 catalog
아래의 모든 namespace / table 도 동일 tier 로 묶인다.

### 3.1 Tier 1 (user_catalog 자식) — 개인·팀 catalog

**조건**: catalog 이름이 `my_<영숫자>_catalog` 패턴 (예: `my_x01100_catalog`, `my_42_catalog`).

**선행 작업** (운영 시작 전 한 번만 — 부모 Domain entity 생성):
```bash
TOKEN=$DATAHUB_PAT
GMS=http://datahub-datahub-gms.datahub-hynix.svc.cluster.local:8080

curl -X POST "$GMS/openapi/v3/entity/domain" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '[{
    "urn": "urn:li:domain:user_catalog",
    "domainProperties": { "value": {
      "name": "user_catalog",
      "description": "Polaris per-user/team catalogs (my_<id>_catalog convention)."
    }}
  }]'
```

**Trigger 예시** (Polaris management API):
```bash
POLARIS_TOKEN=$(curl -s -X POST http://polaris:8181/api/catalog/v1/oauth/tokens \
  -d "grant_type=client_credentials&client_id=root&client_secret=$POLARIS_SECRET&scope=PRINCIPAL_ROLE:ALL" \
  | jq -r .access_token)

curl -X POST http://polaris:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $POLARIS_TOKEN" -H "Content-Type: application/json" \
  -d '{ "catalog": {
    "name": "my_x01100_catalog",
    "type": "INTERNAL",
    "properties": {"default-base-location": "s3://.../my_x01100_catalog"},
    "storageConfigInfo": { "storageType": "S3", "...": "..." }
  }}'
```

**자동 emit 결과**:
- Domain entity: `urn:li:domain:my_x01100_catalog` (parentDomain = `urn:li:domain:user_catalog`)
- Catalog/namespace/table 모두 `domains: [urn:li:domain:my_x01100_catalog]` + `ownership: [{owner: urn:li:corpuser:x01100, type: TECHNICAL_OWNER}]`

DataHub UI: `/domains/urn:li:domain:user_catalog` → 자식으로 `my_x01100_catalog` 카드 노출 → 클릭 시 그
아래의 catalog/namespace/dataset 트리.

**Drop 시**: catalog 삭제하면 자식 Domain 도 `status: removed=true` 로 자동 정리. 같은 이름 재생성 시
자동 부활.

### 3.2 Tier 2 (lake_catalog_polaris) — 데이터레이크 시스템 catalog

**조건**: catalog 이름이 정확히 `lake_catalog`.

**선행 작업**: 없음 — listener 가 첫 emit 시 `lake_catalog_polaris` Domain entity 를 자동 등록.

**Trigger 예시**:
```bash
curl -X POST http://polaris:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $POLARIS_TOKEN" -H "Content-Type: application/json" \
  -d '{ "catalog": {
    "name": "lake_catalog",
    "type": "INTERNAL",
    "properties": {"default-base-location": "s3://.../lake_catalog"},
    "storageConfigInfo": {...}
  }}'
```

**자동 emit 결과**:
- Domain entity: `urn:li:domain:lake_catalog_polaris` (flat, no parentDomain)
- Catalog/namespace/table 모두 `domains: [urn:li:domain:lake_catalog_polaris]` + `ownership: [{owner: urn:li:corpuser:datalake, type: TECHNICAL_OWNER}]`

**Drop 시**: 공유 Domain 이므로 catalog 삭제만으로 Domain entity 는 제거하지 않음 (다른 시스템 catalog 가
공유 가능). catalog container 만 soft-delete.

### 3.3 Tier 3 (etc_polaris) — 기타 일반 catalog

**조건**: Tier 1/2 에 해당하지 않는 모든 이름 (예: `datalake_catalog`, `regular_cat`, `analytics`, …).

**선행 작업**: 없음 — listener 가 첫 emit 시 `etc_polaris` Domain entity 를 자동 등록.

**Trigger 예시**: 평범한 이름으로 catalog 생성 — 별도 명명 규약 불필요.
```bash
curl -X POST http://polaris:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $POLARIS_TOKEN" -H "Content-Type: application/json" \
  -d '{ "catalog": { "name": "analytics", "type": "INTERNAL", "...": "..." }}'
```

**자동 emit 결과**:
- Domain entity: `urn:li:domain:etc_polaris` (flat)
- Catalog/namespace/table 모두 `domains: [urn:li:domain:etc_polaris]` + `ownership: [{owner: urn:li:corpuser:etc, type: TECHNICAL_OWNER}]`

**Drop 시**: Tier 2 와 동일 — 공유 Domain 미터치.

### 3.4 emit 결과 즉시 확인 방법

DataHub UI 가 비동기 indexing 으로 5~30 초 늦게 보이므로 빠른 검증이 필요하면 GMS REST 직접 조회:

```bash
# dataset 의 domains / ownership aspect 만 가져오기
URN='urn:li:dataset:(urn:li:dataPlatform:iceberg,my_x01100_catalog.ns.tbl,PROD)'
ENC=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$URN")
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$GMS/openapi/v3/entity/dataset/$ENC?aspects=domains&aspects=ownership"
```

기대 응답에 `domains.value.domains=["urn:li:domain:my_x01100_catalog"]` 와
`ownership.value.owners[0].owner="urn:li:corpuser:x01100"` 가 보이면 emit 성공.

### 3.5 Tier 강제 전환 (catalog 이름 변경 X)

같은 catalog 를 다른 tier 로 옮기려면 catalog 이름 자체를 바꿔야 한다 (분류는 이름 기반). 예: Tier 3 의
`temp_catalog` 를 Tier 1 로 승격하려면 새 이름 (`my_<id>_catalog`) 으로 catalog 를 재생성하고 데이터를
복사. **이름 변경만으로 자동 분류 갱신은 불가** — Polaris 자체에 catalog rename 이 없음.

---

## 4. 배포 전제 조건

1. **이미지**: `apache/polaris:1.3.0-incubating` 이 아닌 **listener 가 포함된 커스텀 이미지** 가 필수.
   Quarkus 가 CDI 빈을 빌드 타임에 인덱싱하므로 런타임 jar drop 으로는 동작 불가. 빌드 절차는
   `image-build.md` 참조.
2. **`user_catalog` 부모 Domain entity 사전 생성** (한 번만):
   ```bash
   curl -X POST "$GMS/openapi/v3/entity/domain" \
     -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
     -d '[{"urn":"urn:li:domain:user_catalog","domainProperties":{"value":{"name":"user_catalog","description":"..."}}}]'
   ```
3. **헬름 차트**: `helm/benchmarks-polaris/` 사용 (운영용). `helm/polaris/` 는 업스트림 참조용으로
   minio/opa/RSA hook 미배선이라 운영 배포 부적합.

---

## 5. 환경변수 (helm `extraEnv` 또는 properties)

helm 차트에서는 `extraEnv` 로, 로컬 실행에서는 `application.properties` 로 주입한다. 아래 표의 모든 환경
변수 prefix 는 `POLARIS_EVENT_LISTENER_DATAHUB_` (`..._` 는 prefix 생략 표시).

### 5.1 필수

| 환경변수                          | 예시 값                                                              | 비고                                              |
| --------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------- |
| `POLARIS_EVENT_LISTENER_TYPE`     | `datahub-http`                                                       | `no-op` 시 listener 자체 비활성                   |
| `..._GMS_URL`                     | `http://datahub-datahub-gms.datahub-hynix.svc.cluster.local:8080`    | 미설정 시 emit skip (catalog 차단 X)              |
| `..._TOKEN`                       | DataHub PAT                                                          | **Secret 으로 주입 권장** — values.yaml 평문 금지 |

### 5.2 선택 (env vars 누락 시 `@WithDefault` 값 적용)

| 환경변수                                  | Default   | 용도                                          |
| ----------------------------------------- | --------- | --------------------------------------------- |
| `..._PLATFORM_INSTANCE`                   | `polaris` | URN namespace                                 |
| `..._ENV`                                 | `PROD`    | dataset URN env 레이블                        |
| `..._SYNCHRONOUS_MODE`                    | `false`   | true 시 catalog op 에 emit blocking           |
| `..._CONNECT_TIMEOUT`                     | `5s`      | TCP connect cap                               |
| `..._REQUEST_TIMEOUT`                     | `10s`     | end-to-end cap                                |
| `..._MAX_INFLIGHT_EMITS`                  | `1000`    | backpressure cap                              |
| `..._CIRCUIT_BREAKER_FAILURE_THRESHOLD`   | `5`       | 연속 실패 N회 → 회로 OPEN                     |
| `..._CIRCUIT_BREAKER_OPEN_DURATION`       | `30s`     | cooldown                                      |
| `..._REPLAY_BUFFER_CAPACITY`              | `1000`    | 실패 emit 재전송 큐 (0=비활성)                |

### 5.3 DEPRECATED

| 환경변수                          | 상태                                                                          |
| --------------------------------- | ----------------------------------------------------------------------------- |
| `..._DOMAIN_MAPPING_<CATALOG>`    | 3-tier 분류로 대체됨. interface 만 유지되고 listener 가 무시. 설정해도 동작 안 함 |

---

## 6. 장애 격리 — Polaris 운영 영향 0 보장

DataHub 가 down/slow 해도 catalog op 는 정상 진행한다. 5단계 방어:

1. **Connect timeout** (`5s`): TCP 연결 black-hole 차단
2. **Request timeout** (`10s`): 단일 emit end-to-end cap
3. **Inflight semaphore** (`1000`): 동시 미완료 emit 한도 — 초과 시 drop + WARN
4. **Circuit breaker**: 5회 연속 실패 → 30s OPEN, HALF_OPEN 단일 probe (CAS 기반 thundering-herd 방지)
5. **Replay buffer** (peek-and-keep, cap 1000): 실패/skip emit 적재 → DataHub 회복 시 자동 재전송. 큐 가득
   시 drop-oldest

응답 분류:
- 2xx → success
- 5xx / 408 / 429 → retryable (circuit + replay)
- 기타 4xx (400/401/403/404 등) → non-retryable (WARN + drop, circuit 미영향)

**알려진 제약**: 큐는 in-memory — 서버 재시작 시 손실. 영구 outbox 미적용.

---

## 7. wire format (참고용)

`POST /openapi/v3/entity/{entityType}` — JSON 배열 + flat aspect key:

```json
[{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:iceberg,my_x01100_catalog.ns.tbl,PROD)",
  "datasetProperties": { "value": { "name": "tbl", "qualifiedName": "...", "customProperties": {...}, "lastModified": {...} } },
  "subTypes":          { "value": { "typeNames": ["Table"] } },
  "container":         { "value": { "container": "urn:li:container:iceberg.polaris.my_x01100_catalog.ns" } },
  "dataPlatformInstance": { "value": {...} },
  "schemaMetadata":    { "value": { "schemaName": "...", "fields": [...] } },
  "domains":           { "value": { "domains": ["urn:li:domain:my_x01100_catalog"] } },
  "ownership":         { "value": { "owners": [{ "owner": "urn:li:corpuser:x01100", "type": "TECHNICAL_OWNER" }] } },
  "status":            { "value": { "removed": false } }
}]
```

한 entity 의 모든 aspect 가 단일 POST. `emitUpsert` 가 `status: removed=false` 자동 주입 (drop→recreate
시 UI "removed" stick 자동 해제).

---

## 8. 빌드 & 배포

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21

# 1. 단위 테스트 (81 passing 기대)
./gradlew :polaris-extensions-datahub-listener:test

# 2. fast-jar 빌드
./gradlew :polaris-server:quarkusBuild
ls runtime/server/build/quarkus-app/lib/main/ | grep datahub  # 검증

# 3. 멀티아키 이미지 (같은 태그 재빌드 시 image-build.md §4a force clean 절차 사용)
cd runtime/server
docker buildx build --platform linux/amd64,linux/arm64 \
  -f src/main/docker/Dockerfile.jvm \
  -t skthynix/polaris:v1.3.0-integration-datahub --push .

# 4. helm 배포
helm upgrade benchmarks-polaris helm/benchmarks-polaris/ \
  --namespace datahub-hynix --atomic --timeout 5m
```

---

## 9. Polaris 내부 수정 (1.3.x 머지 시 주의)

DataHub listener 를 위해 `polaris-runtime-service` 모듈에 **3개 파일 추가/변경**:

| 파일 | 변경 |
|---|---|
| `IcebergRestCatalogEventServiceDelegator.java` | `renameTable` / `updateProperties` 후 marker 기반 추가 load 호출 |
| `IcebergRestCatalogEvents.java` | `AfterRenameTableEvent`/`AfterUpdateNamespacePropertiesEvent` 에 nullable 필드 + 하위호환 생성자 |
| `events/listeners/RequiresPost{RenameTable,UpdateNamespace}Metadata.java` | 신규 marker interface |

`polaris-core` / API 모듈은 무변경. 1.3.x 포인트 릴리스 업스트림 머지 부담 최소.

---

## 10. 알려진 제약

- **View 이벤트 미동기화**: `AfterCreateViewEvent` 등 view 관련 hook 미구현.
- **초기 일괄 sync 없음**: listener 활성화 이전 catalog/table 은 DataHub 에 안 보임. 필요 시 별도 backfill.
- **replay buffer in-memory**: 서버 재시작 시 큐 손실. 영구 보전 필요 시 outbox 별도 설계.
- **`user_catalog` 부모 Domain 수동 사전 생성**: listener 가 자동 만들지 않음 (의도적 — 잘못된 description 으로 들어가면 운영자 재정정 필요).
- **explicit `domain-mapping` 비활성**: 3-tier 분류로 대체. interface 만 호환성으로 잔존.
- **post-load 부수효과**: rename / updateProperties 후 추가 load 1회 (metadata fetch + 권한평가). hot path 아님.

---

## 11. 테스트 구조

총 **82 tests** (`./gradlew :polaris-extensions-datahub-listener:test`):

- `DataHubEventMapperTest` (45) — URN/escape/aspect 빌드, 3-tier 분류 (user_catalog 자식·lake·etc), schemaMetadata, customProperties, audit, dataset subType=Table, namespace subType=Namespace
- `AbstractDataHubEventListenerTest` (15) — 이벤트 → emitter 호출 검증, ensureCatalogDomain, drop symmetric soft-delete
- `HttpEmitterTest` (22) — wire format, circuit breaker, replay buffer (실제 JDK HttpServer 사용)
