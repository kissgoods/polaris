# Polaris Event REST Forwarder — 기술 설계 문서

## 1. 배경 및 목적

Apache Polaris는 Iceberg REST Catalog 서버로, 테이블·네임스페이스·카탈로그에 대한 CRUD 작업을 처리한다. 운영팀은 이 이벤트를 DataHub, Kafka, Atlas 같은 다양한 메타데이터 백엔드로 라우팅해야 한다.

이 확장은 **Polaris 에서 발생하는 카탈로그 이벤트를 raw JSON envelope 로 직렬화해 단일 REST 엔드포인트(receiver)에 POST 한다**. 백엔드별 변환 (DataHub URN/Aspect, Kafka topic 라우팅, Atlas type 매핑 등) 은 receiver 의 책임이고, Polaris 는 어떤 백엔드도 알지 않는다.

이전 버전은 `extensions/datahub-listener/` 에서 Polaris-side 에서 DataHub OpenAPI v3 payload 까지 모두 빌드했지만, 다음 이유로 raw-event 직송 구조로 교체했다:
- Polaris 가 DataHub 도메인 지식 (URN escape, 3-tier Domain 분류, schemaMetadata aspect, audit stamp 등) 을 무겁게 안고 있어 다른 백엔드 추가 시 코드 중복이 컸음
- 백엔드 변경/추가 시 Polaris 재빌드/재배포가 필요해 운영 유연성이 낮았음
- receiver 가 자체적으로 dedup/batch/fan-out 을 할 수 있어 listener 측이 단순해짐

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
   │  @Identifier("rest-forwarder") 빈 선택
   ▼
AbstractEventForwarderListener
   │  poster.post(envelope) 호출
   ▼
HttpEventPoster — JDK HttpClient
   ▼
Receiver (사용자 구현)
   │  DataHub / Kafka / Atlas / ... 로 fan-out
```

### 2.1 IcebergRestCatalogEventServiceDelegator

`runtime/service/.../catalog/iceberg/IcebergRestCatalogEventServiceDelegator.java`

Quarkus CDI Decorator 패턴으로 Iceberg REST Catalog 의 모든 API 메서드를 가로채고 전후로 `polarisEventListener.onBefore*` / `onAfter*` 를 호출한다.

이 forwarder 를 위해 delegator 에 **두 marker 기반 post-load 분기** 가 이전 PR 에서 추가되어 있다 — `renameTable` 과 `updateProperties`. 두 경로 모두 opt-in marker interface (`RequiresPostRenameTableMetadata`, `RequiresPostUpdateNamespaceMetadata`) 로 추가 비용을 격리한다. marker 미구현 listener (`no-op`, `aws-cloudwatch`, `persistence-in-memory-buffer`) 는 추가 호출을 전혀 부담하지 않는다 — 현재 두 marker 를 모두 구현한 listener 는 `AbstractEventForwarderListener` 하나뿐이다.

이 분기에 사용되는 두 이벤트 record 의 nullable 필드 (`LoadTableResponse loadTableResponse`, `Map<String,String> currentProperties`) 는 이전 PR 에서 이미 추가되어 있다 (하위호환 생성자 포함). forwarder 는 이 필드를 envelope 의 `tableMetadataJson` / `properties` 에 그대로 흘려보낸다.

**부수효과** (DataHub 이전 버전과 동일):
- 인가 분리 시나리오: 추가 `loadTable` / `loadNamespaceMetadata` 가 각각 `LOAD_TABLE` / `LOAD_NAMESPACE_METADATA` 권한을 별도 평가. principal 이 후속 권한을 안 가지면 listener 가 메타데이터를 잃음 (Polaris 동작 자체에는 영향 없음)
- Race condition: 1차 변경 (rename / updateProperties) 과 후속 load 사이에 같은 target 에 drop / 재 rename / 다른 update 가 끼어들면 stale 메타데이터 emit 가능
- 성능: 매 rename / updateProperties 마다 metadata fetch 1회 + 권한평가 1회 추가. hot path 가 아니라 운영 영향은 작음

### 2.2 이벤트 범위

12 종 핸들 (catalog 3 + namespace 3 + table 6). View / Principal / Role / Policy / Generic Table 등 그 외 이벤트는 의도적으로 미override — receiver 가 필요로 하면 별도 PR 로 확장.

| 이벤트 | 출처 record |
|---|---|
| `onAfterCreateCatalog` / `onAfterUpdateCatalog` / `onAfterDeleteCatalog` | `CatalogsServiceEvents.*` |
| `onAfterCreateNamespace` / `onAfterUpdateNamespaceProperties` / `onAfterDropNamespace` | `IcebergRestCatalogEvents.*` |
| `onAfterCreateTable` / `onAfterRegisterTable` / `onAfterUpdateTable` / `onAfterCommitTable` / `onAfterRenameTable` / `onAfterDropTable` | `IcebergRestCatalogEvents.*` |

**`onAfterCommitTransaction` 의도적 미override**: multi-table atomic commit 은 두 종류의 이벤트를 동시에 발생시킨다 — 테이블별 `onAfterCommitTable` (전체 `TableMetadata` 포함) 과 트랜잭션 전체의 `onAfterCommitTransaction` (메타데이터 없음). 전자 처리만으로 모든 테이블이 올바르게 sync 되며, 후자에서 한 번 더 emit 하면 receiver 가 `(catalog, table) → latest metadata` 로 keying 할 때 무메타 envelope 가 최신 상태를 덮어쓴다. 회귀 가드: `commitTransactionMustRemainNoOpToAvoidDuplicateEvents` (reflection 으로 override 부재 검증).

**Transactional commit table 이벤트 deferral (`TransactionalCommitTableDeferred` marker)**: `IcebergCatalogHandler.commitTransaction` 의 per-table `tableOps.commit()` 가 transaction workspace 에 queue 되는 시점에 `onAfterCommitTable` 을 fire 한다 — 그러나 최종 `metaStoreManager.updateEntitiesPropertiesIfNotChanged` 가 실패해 `CommitFailedException` 으로 rollback 될 수 있다. eager emit 하면 receiver 가 Polaris 가 한 번도 commit 하지 않은 state 를 적용함. 해결: marker 구현 listener 에 대해 delegator 가 `delegate.commitTransaction(...)` 양옆에 `beginTransaction()` / `endTransaction(committed)` handshake. listener 는 그 사이 동일 thread 의 `onAfterCommitTable` 호출을 ThreadLocal buffer 에 적재 → `endTransaction(true)` drain, `endTransaction(false)` discard. marker 미구현 listener 는 legacy eager-fire 유지.

---

## 3. 모듈 구조

```
extensions/event-rest-forwarder/
└── src/main/java/.../forwarder/
    ├── RestForwarderConfiguration.java     # SmallRye @ConfigMapping
    ├── AuditContext.java                   # (timestampMs, principal) record
    ├── EventEnvelope.java                  # Wire JSON DTO + nested RenameTarget / PropertyUpdateSummary
    ├── EventSerializer.java                # Polaris event → EventEnvelope
    ├── AbstractEventForwarderListener.java # 12 핸들러 + RequiresPost* marker 구현 + getAuditContext virtual hook
    └── http/
        ├── HttpEventPoster.java            # JDK HttpClient + 5 격리 가드
        └── RestForwarderEventListener.java # @Identifier("rest-forwarder") + SecurityContext audit
```

추가 런타임 의존성 없음 — Jackson, JDK HttpClient, Iceberg core 의 `TableMetadataParser` 만 사용 (모두 Polaris 기본 classpath).

---

## 4. 핵심 클래스 상세

### 4.1 RestForwarderConfiguration

```java
@ConfigMapping(prefix = "polaris.event-listener.rest-forwarder")
@ApplicationScoped
public interface RestForwarderConfiguration {
  @WithName("endpoint-url") Optional<String> endpointUrl();   // 미설정 시 emit skip
  @WithName("token")        Optional<String> token();         // Bearer (선택)
  @WithName("synchronous-mode")  @WithDefault("false")  boolean synchronousMode();

  // 5 격리 가드 (Section 4.4)
  @WithName("connect-timeout")   @WithDefault("5s")     Duration connectTimeout();
  @WithName("request-timeout")   @WithDefault("10s")    Duration requestTimeout();
  @WithName("max-inflight-emits") @WithDefault("1000")  int maxInflightEmits();
  @WithName("circuit-breaker-failure-threshold") @WithDefault("5")    int circuitBreakerFailureThreshold();
  @WithName("circuit-breaker-open-duration")     @WithDefault("30s")  Duration circuitBreakerOpenDuration();
  @WithName("replay-buffer-capacity")            @WithDefault("1000") int replayBufferCapacity();
}
```

`endpoint-url` 미설정 시 동작: emit 마다 에러 로그 후 skip — catalog 요청 차단 없음.

`endpoint-url` 이 `Optional` 인 이유: SmallRye Config 가 `@ConfigMapping` 인터페이스를 기동 시 eager-validate 한다. `no-op` listener 를 선택해도 `RestForwarderConfiguration` 빈은 클래스패스에 존재하므로, 필수 `String` 으로 두면 URL 미설정 시 no-op 서버 기동도 실패한다.

`synchronous-mode` 의 본 용도는 테스트·디버깅이지만, **strict per-event ordering** 이 필요한 운영에도 선택지로 두었다. async 모드에서는 HTTP/2 multiplexing / 커넥션 풀로 동일 (catalog, table) 의 두 emit 이 receiver 측에서 뒤바뀌어 도달할 수 있다. sync 모드로 두면 매 emit 이 catalog 요청 thread 에서 직렬화되어 race 가 사라진다 (지연 비용은 catalog op 마다 발생).

SmallRye Config 는 환경변수를 자동으로 config 속성에 매핑한다:

```
POLARIS_EVENT_LISTENER_REST_FORWARDER_ENDPOINT_URL    →  polaris.event-listener.rest-forwarder.endpoint-url
POLARIS_EVENT_LISTENER_REST_FORWARDER_TOKEN           →  polaris.event-listener.rest-forwarder.token
POLARIS_EVENT_LISTENER_REST_FORWARDER_REQUEST_TIMEOUT →  polaris.event-listener.rest-forwarder.request-timeout
```

### 4.2 EventEnvelope

Wire 포맷 DTO. `@JsonInclude(NON_NULL)` 로 null 필드는 직렬화에서 제외한다 (receiver 가 필드 presence 를 discriminator 로 사용할 수 있도록).

```json
{
  "eventId": "f5d3a7e2-...-uuid",
  "sequenceNumber": 4217,
  "eventType": "AfterCreateTable",
  "timestampMs": 1715760000000,
  "actor": "alice@example.com",
  "realm": "prod-realm",
  "catalog": "cat",
  "namespace": ["ns", "sub"],
  "namespaceRaw": "nssub",
  "table": "tbl",
  "properties": {"k": "v"},
  "tableMetadataJson": "<Iceberg canonical JSON>",
  "renameTo": {"namespace": ["ns"], "name": "new"},
  "purgeRequested": true,
  "propertyUpdates": {"updated": ["k1"], "removed": ["old"]}
}
```

- `eventId`: UUID minted at envelope creation, stable across the listener's own retries (replay buffer redelivery, sync-mode timeout-after-commit). **Receiver-side idempotency key** — see §8 #4.
- `realm`: Polaris realm identifier (from `RealmContext.getRealmIdentifier()`); null in single-realm deployments. Included in `resourceKey()` prefix so same-named catalogs in different realms can't collapse downstream — see §8 #6.
- `sequenceNumber`: per-listener-process monotonic counter (1+). Used by the listener internally for same-resource older-seq purge, and by the receiver as the natural ordering signal — see §8 #5.
- `eventType`: Polaris event record 의 simple class name (e.g. `AfterCreateTable`). receiver discriminator
- `actor`: JAX-RS `SecurityContext.getUserPrincipal().getName()`; 없으면 null
- `namespace`: levels 배열 — table / namespace 이벤트만, catalog 이벤트는 null
- `namespaceRaw`: `AfterDropNamespace` 만 채움 — Polaris 가 이 이벤트는 namespace 를 raw U+001F-separated string 으로 던지므로 split 하지 않고 그대로 보존 (receiver 가 split 여부 결정)
- `table`: table 이벤트만
- `properties`: create namespace / updateNamespaceProperties (post-load 성공 시) / catalog properties 시 채움. **`null` 은 "receiver 가 기존 상태를 유지하라"** 는 의미 — empty map (`{}`) 과 구별
- `tableMetadataJson`: `TableMetadataParser.toJson(TableMetadata)` 의 canonical 결과. receiver 는 `TableMetadataParser.fromJson` 으로 round-trip. post-load 실패 시 null
- `renameTo`: `AfterRenameTable` 의 destination
- `purgeRequested`: `AfterDropTable` 의 purge 플래그
- `propertyUpdates`: `AfterUpdateNamespaceProperties` 의 REST 응답에서 가져온 updated / removed 키 목록 — full `properties` 와 별개로 항상 surface (load 실패해도 diff 는 알려줌)

### 4.3 EventSerializer

`Polaris event record + AuditContext → EventEnvelope` 의 pure function 집합. 12 종 이벤트 각각에 대해 `forXxx(event, audit)` 메서드가 존재. CDI 의존성 없어 단위 테스트 용이.

핵심 헬퍼:
- `Namespace → List<String>`: `namespace.levels()` 그대로 `Arrays.asList`. empty namespace 는 null 로 변환 (receiver 의 "namespace 없는 catalog-level event" 와 구분되게 둘 다 null 인 것은 의도적 — receiver 는 `eventType` 으로 판별)
- `LoadTableResponse → tableMetadataJson`: `TableMetadataParser.toJson` (null-safe)
- `AfterDropNamespaceEvent`: namespace 를 `namespaceRaw` 에만 보존, `namespace` 는 null
- `AfterRenameTableEvent`: source 를 `namespace` + `table` 에, destination 을 `renameTo` 에. `loadTableResponse == null` 이면 `tableMetadataJson` 도 null (rename signal 자체는 그대로 보내 receiver 가 dataset 이동을 인지)
- `AfterUpdateNamespacePropertiesEvent`: `currentProperties` 그대로 `properties` 로. `null` 이면 `properties` 도 null (receiver 가 덮어쓰기 차단). REST 응답의 updated/removed 는 `propertyUpdates` 로 별도 surface

### 4.4 HttpEventPoster

JDK 11+ `HttpClient` 를 사용해 `EventEnvelope` 를 receiver 의 `endpoint-url` 에 POST. **per-event 1 POST**, batching 안 함 (receiver 책임).

**Receiver 장애 격리 (Polaris 운영 영향 0 보장)**

다섯 단계 방어:

| 메커니즘 | 동작 | 인터페이스 `@WithDefault` | 설정 키 |
|---|---|---|---|
| Connect timeout | TCP 연결 대기 한계. receiver black-hole / 미응답 시 OS-level timeout 으로 묶이는 것을 차단 | 5s | `connect-timeout` |
| Request timeout | 단일 emit 의 end-to-end 한계 | 10s | `request-timeout` |
| Inflight backpressure | 동시 미완료 emit 수에 cap (`Semaphore.tryAcquire`). 한도 초과 시 즉시 drop + WARN (100 건마다 로그) | 1000 | `max-inflight-emits` |
| Circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED\|OPEN) | 연속 실패 N 회 누적 시 회로 OPEN → cooldown 동안 모든 emit skip. cooldown 만료 후 **CAS 로 단일 caller 만 HALF_OPEN probe 진입** — 동시 emit 은 skip. probe 성공 → CLOSED, 실패 → OPEN + fresh cooldown. state 와 timestamp 는 immutable `CircuitSnapshot` 으로 묶여 단일 `AtomicReference` 에 CAS 갱신 → race window 제거 | 5회 / 30s | `circuit-breaker-failure-threshold`, `circuit-breaker-open-duration` |
| Replay buffer (peek-and-keep) | 실패·skip 된 emit 을 bounded `LinkedBlockingQueue` 에 적재 → daemon thread 가 cooldown 만료 후 재전송. **`peek()` 후 `forceSync=true` 로 전송한 결과 (성공 또는 permanent-rejection) 일 때만 `poll()` 로 head 제거** — failed probe 시 head 가 큐에 보존되어 다음 cooldown 후 재시도. 큐 가득 시 oldest drop (`bufferLock` 으로 race 보호) | 1000 events | `replay-buffer-capacity` (0 = 비활성) |

모든 request 빌드/스케줄링 경로 (`URI.create`, `HttpRequest.timeout`, `sendAsync`) 는 단일 try/finally 로 감싸 잘못된 설정값 (예: malformed URL, `Duration.ZERO`) 이 들어와도 예외가 caller 로 전파되지 않고 permit 도 누수되지 않는다.

**응답 분류 (`isRetryableFailure`)**

| 분류 | 매칭 | 동작 |
|---|---|---|
| Success | `err == null && 2xx` | `recordSuccess(entered)` — `entered == HALF_OPEN` 이면 CLOSED 전이 |
| Retryable failure | `err != null` ∪ 5xx ∪ 408 ∪ 429 | `recordFailure(entered)` — circuit threshold/HALF_OPEN probe 결정 + replay buffer 적재 |
| Non-retryable 4xx | 그 외 4xx (400/401/403/404 등) | WARN 로그 + drop. circuit failure 카운터 미증가, replay buffer 미적재. HALF_OPEN probe 시에는 receiver 도달 신호로 보고 `recordSuccess` → CLOSED 전이 |

핵심 의도: persistent client-side 오류 (잘못된 schema, 만료 token, 권한 미부여) 가 circuit 를 잘못 OPEN 시키거나 replay buffer 를 영구 미배달 항목으로 가득 채우는 것을 차단.

drop 된 이벤트는 catalog 트랜잭션에 영향을 주지 않는다 — listener 측 데이터 누락만 발생 (영구 outbox 미적용 — 알려진 제약).

**Circuit breaker / Replay buffer 동작 흐름 (10 분 outage 시나리오)**

```
t=0      receiver down 시작
t=0~50s  emit 시도 → 5번 연속 실패 (각 ~10s request-timeout) → 회로 OPEN
t=50s~   모든 emit skip (네트워크 시도 0). 실패한 emit 들은 replay buffer 적재 (cap 1000)
         buffer 가득 시 oldest drop (bounded memory)
t=80s    cooldown (30s) 경과 → 다음 emit = probe 시도 → 실패 → OPEN 재진입 (+ 30s)
...      약 20회 probe 반복 (cooldown 30s × ~20 ≈ 10분)
t=600s   receiver 회복 → probe 성공 → 회로 CLOSE → INFO 로그
         replayer thread 가 buffer 드레인 → 적재된 이벤트 (최근 cap 만큼) 재전송
```

요점: catalog operations 는 t=0 부터 끝까지 무중단. receiver 회복 후 buffer 안에 있던 이벤트는 자동 복구됨.

**비동기 발송 (기본)**

```java
httpClient.sendAsync(req, BodyHandlers.ofString())
    .whenComplete((resp, err) -> { /* recordSuccess/Failure + replay 적재 */ });
// synchronousMode=false: 여기서 반환 → Polaris 요청 처리 계속
// synchronousMode=true: future.join() 으로 receiver 응답 대기
```

receiver 장애 시 로그만 남기고 예외를 삼키므로 Polaris 카탈로그 연산에 영향을 주지 않는다.

### 4.5 AbstractEventForwarderListener

`PolarisEventListener` 구현체 + 세 marker (`RequiresPostRenameTableMetadata`, `RequiresPostUpdateNamespaceMetadata`, `TransactionalCommitTableDeferred`) 구현. 각 핸들러는 `poster.post(serializer.forXxx(event, getAuditContext()))` 한 줄 — 단 `onAfterCommitTable` 만은 thread-local 버퍼 검사를 거쳐 transaction 안에서는 적재 → `endTransaction(true)` 시 drain.

**`getAuditContext()` virtual hook**: abstract base 는 `(System.currentTimeMillis(), null)` 반환 — 테스트·offline 경로용. concrete HTTP listener 가 override 해서 `SecurityContext.getUserPrincipal()` 에서 principal 을 가져옴.

**Quarkus 프록시를 위한 no-args 생성자**: Quarkus 가 `@ApplicationScoped` 빈의 CDI 프록시 서브클래스를 생성할 때 `super()` 를 호출. no-args 생성자가 없으면 빌드 시 *"It's not possible to automatically add a synthetic no-args constructor"* 오류 발생.

```java
protected AbstractEventForwarderListener() {
    this.poster = null;     // 프록시 인스턴스용
    this.serializer = null;
}

@PreDestroy
void shutdown() {
    if (poster == null) return;   // 프록시 인스턴스 호출 시 NPE 방지
    poster.close();
}
```

### 4.6 RestForwarderEventListener

`@ApplicationScoped @Identifier("rest-forwarder")` — `polaris.event-listener.type=rest-forwarder` 설정 시 선택되는 concrete listener. `AbstractEventForwarderListener` 를 상속해서 poster/serializer 를 주입한다.

```java
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
```

`Clock` 은 `@Inject` 라서 테스트에서 fixed Clock 으로 시간 결정성을 확보할 수 있다.

---

## 5. 이벤트 흐름 예시: 테이블 생성

```
클라이언트                Polaris                     Receiver
    │                       │                            │
    │── POST /tables ───────►│                            │
    │                       │ onBeforeCreateTable()       │
    │                       │ delegate.createTable()      │
    │                       │ onAfterCreateTable()        │
    │                       │   poster.post(envelope)    │
    │                       │────────────────────────────►│ POST /polaris/events
    │◄── 200 OK ────────────│                            │ (비동기 — Polaris 응답과 독립)
    │                       │                            │◄── 200 OK
```

Envelope body 예시:
```json
{
  "eventId": "a1b2c3d4-...-uuid",
  "sequenceNumber": 4218,
  "eventType": "AfterCreateTable",
  "timestampMs": 1715760000000,
  "actor": "alice@example.com",
  "realm": "POLARIS",
  "catalog": "cat",
  "namespace": ["ns", "sub"],
  "table": "tbl",
  "tableMetadataJson": "{\"format-version\":2,\"table-uuid\":\"...\",\"location\":\"...\",\"schemas\":[...],\"properties\":{...},...}"
}
```

---

## 6. 설정 참조

### application.properties (로컬 실행)

```properties
polaris.event-listener.type=rest-forwarder
polaris.event-listener.rest-forwarder.endpoint-url=http://localhost:8080/polaris/events
polaris.event-listener.rest-forwarder.token=<bearer-token-optional>
polaris.event-listener.rest-forwarder.synchronous-mode=false

# 5 격리 가드 (실효 운영 default = @WithDefault). override 가 필요할 때만 키 활성화.
# polaris.event-listener.rest-forwarder.connect-timeout=5s
# polaris.event-listener.rest-forwarder.request-timeout=10s
# polaris.event-listener.rest-forwarder.max-inflight-emits=1000
# polaris.event-listener.rest-forwarder.circuit-breaker-failure-threshold=5
# polaris.event-listener.rest-forwarder.circuit-breaker-open-duration=30s
# polaris.event-listener.rest-forwarder.replay-buffer-capacity=1000
```

### Helm values.yaml (Kubernetes 배포)

```yaml
image:
  repository: skthynix/polaris
  tag: "v1.3.0-integration-datahub"   # event-rest-forwarder JAR 이 포함된 커스텀 이미지

eventListener:
  type: rest-forwarder

extraEnv:
  - name: POLARIS_EVENT_LISTENER_REST_FORWARDER_ENDPOINT_URL
    value: "http://polaris-event-receiver.<ns>.svc.cluster.local:8080/polaris/events"
  - name: POLARIS_EVENT_LISTENER_REST_FORWARDER_SYNCHRONOUS_MODE
    value: "false"
  # token 이 필요하면 Secret 으로 (인라인 평문 권장 안 함):
  # - name: POLARIS_EVENT_LISTENER_REST_FORWARDER_TOKEN
  #   valueFrom:
  #     secretKeyRef: { name: polaris-rest-forwarder-token, key: token, optional: true }
```

> **주의**: `image.repository / tag` 는 반드시 커스텀 빌드 이미지로 지정해야 한다. Quarkus 는 CDI 빈을 **빌드 타임** 에 인덱싱하므로, 공식 `apache/polaris` 이미지에는 이 forwarder 클래스가 존재하지 않는다.

**왜 env vars 만 쓰는가**: SmallRye Config 의 `validate-unknown=true` (baked-in default) 가 `runtimeOnly` 로 추가된 extension 의 `@ConfigMapping` 을 *file source 검증 단계에서 root 로 찾지 못해* `SRCFG00050` 으로 부팅 실패. env vars 로 주입하면 검증을 우회. `polaris.event-listener.type=rest-forwarder` (타입 선택) 만 ConfigMap 에 두는 건 안전 (등록된 root 에 매칭).

---

## 7. 빌드 및 등록

### Gradle 모듈 등록 (`gradle/projects.main.properties`)

```properties
polaris-extensions-event-rest-forwarder=extensions/event-rest-forwarder
```

### 서버 런타임 포함 (`runtime/server/build.gradle.kts`)

```gradle
runtimeOnly(project(":polaris-extensions-event-rest-forwarder"))
```

`runtimeOnly` 로 등록하는 이유: 서버 모듈은 이 확장 모듈에 컴파일 의존성이 없지만, 모듈 JAR 안에 포함된 Jandex 인덱스 (`META-INF/jandex.idx`) 를 Quarkus 가 런타임 classpath 에서 읽어야 CDI 빈으로 등록할 수 있기 때문이다.

---

## 8. Receiver (사용자 구현) 책임

새 REST 서비스가 구현해야 하는 endpoint:

```
POST /polaris/events           # path 는 endpoint-url 그대로 — listener 가 path 를 따로 붙이지 않음
Authorization: Bearer <token>  # config.token 설정 시
Content-Type: application/json
```

Body 는 단일 `EventEnvelope` JSON object (배열 아님 — listener 는 batching 안 함).

**응답 contract — `isRetryableFailure` 와 정렬**

| 상황 | 반환 status | listener 동작 |
|---|---|---|
| 정상 처리 | **2xx** | success, circuit close |
| 일시 장애 (DB 잠금, 백엔드 미응답) | **5xx / 408 / 429** | retryable: circuit failure 카운트 + replay buffer 적재 |
| 페이로드 거절 (schema 거절, malformed) | **400** | non-retryable: 즉시 drop |
| 인증 실패 | **401 / 403** | non-retryable: drop, 운영자가 secret 수정 |
| backpressure | **429** | retryable: cooldown 후 replay |

Receiver 가 자체적으로 처리할 것:
1. `eventType` discriminator 로 분기
2. `tableMetadataJson` 을 필요시 `TableMetadataParser.fromJson` 로 역직렬화 (Iceberg core 의존성 필요)
3. `namespaceRaw` 는 `String.split("")` 로 split 가능 (또는 그냥 보존)
4. **Idempotency**: `eventId` (UUID) 를 dedup 키로 사용. listener 는 이미 same-resource purge 로 buffered older-seq 를 정리하지만, **timeout-after-commit** (receiver 가 commit 후 응답 직전에 listener timeout → listener replay → receiver 가 같은 commit 2회 적용) 케이스만은 listener 가 막을 수 없음. `eventId` 를 receiver-side dedup table 에 보관 (TTL = listener `replay-buffer-capacity × 평균 emit 간격` 정도면 충분).
5. **Ordering 힌트**: 같은 resource 의 두 envelope 가 receiver 에 도착할 때 `sequenceNumber` 가 작은 쪽이 더 오래된 상태. async 모드 HTTP/2 multiplexing 으로 reorder 가능하므로, 동일 (catalog, namespace, table) 에 대해 늦게 도착한 lower-seq envelope 는 stale 로 보고 적용 보류/병합 권장.
6. **Multi-realm 분리**: `realm` 필드가 채워져 있다면 receiver 의 dedup / lookup 키에 prefix 로 포함시키세요 — 다른 realm 의 동명 catalog/table 이 충돌하지 않도록. listener 의 `resourceKey()` 도 같은 규칙을 적용하므로, receiver 가 listener-generated 키를 그대로 쓸 거라면 별도 조정 불필요.
7. dedup / batching / 백엔드별 변환 (DataHub URN 빌드, Kafka topic 라우팅 등) 책임

### 빠른 시작 (FastAPI ~20줄)

```python
from fastapi import FastAPI, Request, Response
import os
app = FastAPI()
EXPECTED = os.environ.get("POLARIS_FORWARDER_TOKEN")

@app.post("/polaris/events")
async def receive(req: Request):
    if EXPECTED:
        auth = req.headers.get("authorization", "")
        if auth != f"Bearer {EXPECTED}":
            return Response(status_code=401)
    event = await req.json()
    et = event["eventType"]
    # ... discriminator dispatch
    return Response(status_code=202)
```

---

## 9. 설계 결정 및 근거

| 결정 | 근거 |
|------|------|
| **HTTP 단일 POST per event** | receiver 가 batching 책임. listener 는 단순함 + 격리 가드 보장에 집중 |
| **비동기 발송 기본** | receiver 장애가 Polaris 카탈로그 연산의 지연·실패로 전파되어서는 안 됨 |
| **`updateNamespaceProperties` 후 `loadNamespaceMetadata` 추가 호출 (`RequiresPostUpdateNamespaceMetadata` opt-in)** | REST 응답은 변경/제거된 키 목록만 가지므로 receiver 가 전체 properties 를 알려면 후속 load 필요. listener 측 marker 로 opt-in — DataHub 외 listener 는 비용 0 |
| **post-load 실패 시 `properties` 를 null 로 surface** | receiver 가 "load 실패" 와 "empty map" 을 구별 가능. null 일 때 receiver 는 기존 상태 보존, `propertyUpdates` 의 diff 만 적용 가능 |
| **`renameTable` 후 `loadTable` 추가 호출 (`RequiresPostRenameTableMetadata` opt-in)** | rename REST 응답은 204 No Content. 목적지 테이블 metadata 를 envelope 에 담으려면 명시적 loadTable 필요. opt-in marker 로 forwarder 만 추가 비용 부담 |
| **`AfterDropNamespace` 의 raw U+001F string 보존** | 이벤트 자체가 namespace 를 `String` 형태로 던지므로 split 결정은 receiver 가. Polaris-side 에서 split 했다가 잘못된 levels 로 보내면 round-trip 불가 |
| **`tableMetadataJson` 으로 Iceberg canonical JSON 직송** | `TableMetadataParser` 의 wire 포맷은 Iceberg 가 공식 spec 으로 유지보수. 자체 schema mapper 를 들고 가지 않아도 receiver 가 `fromJson` 으로 round-trip 가능. Iceberg 버전 upgrade 시에도 호환 |
| **per-event 1 POST (배치 안 함)** | batching 은 receiver 가 traffic 패턴 / latency 요구 / dedup 정책에 맞춰 결정. listener 가 결정하면 모든 receiver 에 동일 trade-off 강요 |
| **same-resource older-seq purge 유지 (`HttpEventPoster.purgeBufferedOlderSameResource`)** | 이전 listener 의 same-URN purge 를 (catalog, namespace, table) 튜플 키로 일반화해서 보존. 직접 emit 이 2xx 로 성공하면 buffer 안의 **더 작은 sequenceNumber** 를 가진 same-resource entry 를 즉시 purge (+ stale 마킹). 없으면 drop-then-recreate race 가 발생 — outage 중 drop 이 buffer 에 들어가고, 회복 후 같은 이름 recreate 가 직접 emit 로 성공한 뒤, buffered drop 이 replay 되어 새로 만든 자원을 receiver 측에서 다시 삭제. 동등하거나 큰 seq 의 same-resource entry 는 보존 (그게 더 최신). receiver 측 dedup 책임은 **timeout-after-commit double delivery** 만 — listener 가 timeout 으로 보지만 receiver 는 이미 commit 한 경우 — `eventId` 로 idempotent 처리 |
| **`eventId` (UUID) + `sequenceNumber` (per-listener monotonic) 를 envelope 에 부착** | (1) `eventId` — receiver 의 idempotency key. 같은 envelope 가 retry 로 두 번 도착해도 (특히 sync-mode timeout-after-commit) receiver 가 중복 적용 안 함. (2) `sequenceNumber` — listener-side same-resource purge gate + receiver 의 out-of-order detection. async 모드에서 HTTP/2 multiplexing 으로 reorder 가능한 same-resource emit 들의 latest-state 판별에 사용. `EventSerializer` 가 envelope 생성 시점에 한 번만 할당 → 같은 envelope 의 모든 retry 가 동일 ID/seq 사용 |
| **no-args 생성자 (protected)** | Quarkus CDI 프록시가 `super()` 를 호출. 없으면 빌드 실패 |
| **5 격리 가드 (timeout × 2 + inflight + circuit + replay)** | receiver 가 down/hang 상태일 때 Polaris 카탈로그 운영에 영향이 없도록 보장. timeout 만으로는 동시 미완료 future 누적 가능 → semaphore 로 cap 후 drop. drop 은 listener 측 데이터 누락만 유발하고 catalog 트랜잭션은 정상 진행 |
| **HALF_OPEN 단일 probe (CAS-claimed)** | cooldown 직후 N 개 concurrent emit/replay 가 모두 "elapsed > cooldown" 이라 판단해 receiver 로 burst 하는 thundering-herd 차단 |
| **`CircuitSnapshot(state, openedAtMs)` immutable record + 단일 `AtomicReference`** | state 와 cooldown timestamp 가 별개 변수면 race window. 묶어서 한 번의 CAS 로 publish → race window 자체 제거 |
| **`recordSuccess(entered)` / `recordFailure(entered)`** | 트립 직전 발송된 async in-flight emit 의 callback 이 늦게 도착해 OPEN 을 강제 CLOSED 로 덮어쓰거나, 다른 emit 의 실패가 HALF_OPEN probe 의 결정을 가로채는 race 차단 |
| **Replay buffer peek-and-keep** | 단순 `poll()` 후 재전송이면 cooldown 직후 첫 probe 실패 시 head item 이 silently 손실. `peek()` 후 sync wait 결과로만 `poll()` → outage 동안 buffered 이벤트 무손실 보장 |
| **5xx/408/429 만 retryable 분류** | persistent client-side 오류 (잘못된 schema, 만료 token, 권한 미부여) 가 circuit 를 잘못 OPEN 시키거나 replay buffer 를 영구 미배달 항목으로 가득 채우는 것을 차단 |
| **caller principal 을 `actor` 로** | receiver 가 audit log 를 구축할 때 첫번째로 필요. `AuditContext(timestampMs, principal)` 를 `getAuditContext()` virtual 메서드로 주입. HTTP listener 가 `SecurityContext.getUserPrincipal()` 에서 가져옴 |
| **`onAfterCommitTransaction` 의도적 미override** | 위 Section 2.2 참조. 회귀 테스트: `commitTransactionMustRemainNoOpToAvoidDuplicateEvents` (reflection 으로 declaring class 검사) |
| **`TransactionalCommitTableDeferred` marker 로 transaction 안 commit table 이벤트 deferral** | `IcebergCatalogHandler.commitTransaction` 가 per-table commit 이벤트를 atomic update 전에 fire — eager emit 시 rollback 된 transaction 의 state 가 receiver 에 publish 됨. listener 가 marker 구현 + delegator 가 `beginTransaction` / `endTransaction(committed)` handshake 호출 → ThreadLocal buffer 로 deferral, 성공 시만 drain. marker 미구현 listener 는 legacy eager-fire. 회귀 테스트: `commitTableInsideTransactionIsBufferedUntilCommitSuccess` / `commitTableInsideTransactionIsDiscardedOnRollback` / `rolledBackTransactionDoesNotLeakBufferIntoNextRequest` |
| **`EventEnvelope.resourceKeys()` rename 시 dual-key (source + destination)** | rename envelope 가 source 만 키로 가지면 newer commit-on-destination 이 buffered rename 을 purge 못 함 → cooldown 후 replay 가 destination state 를 옛 rename 으로 덮어씀. `resourceKeys()` 가 Set 으로 source + destination 모두 반환, `purgeBufferedOlderSameResource` 는 set overlap 검사. 회귀: `successfulCommitOnRenameDestinationPurgesBufferedRename` |
| **`EventEnvelope.realm` 필드 + `resourceKey` prefix 에 realm 포함** | 다중-realm 배포에서 같은 catalog/table 이름이 다른 realm 에 있을 때 receiver 의 dedup/ordering 키가 충돌 — cross-tenant collapse. `RestForwarderEventListener` 가 `RealmContext.getRealmIdentifier()` 에서 가져와 `AuditContext.realmId` 로 흘림. 단일-realm / request scope 밖에서 null fallback 시 leading empty segment 로 키 shape 안정 유지 (legacy single-realm 호환). 회귀: `sameCatalogTableInDifferentRealmsProduceDifferentResourceKeys`, `nullRealmKeepsLegacySingleRealmKeyShape` |

---

## 10. 테스트 구조

```
event-rest-forwarder/src/test/
└── .../forwarder/
    ├── EventSerializerTest.java                (15 tests)
    │   ── 12 event type 별 envelope shape (eventType / catalog / namespace / table / properties),
    │      tableMetadataJson canonical round-trip via TableMetadataParser.fromJson,
    │      AfterDropNamespace 의 raw U+001F 보존,
    │      AfterRenameTable 의 renameTo + loadTableResponse=null 시 tableMetadataJson=null,
    │      AfterUpdateNamespaceProperties 의 currentProperties=null → properties=null,
    │      @JsonInclude(NON_NULL) wire 포맷 회귀
    ├── AbstractEventForwarderListenerTest.java (14 tests)
    │   ── 12 핸들러 각각 정확히 1 post 호출,
    │      currentProperties null fallback (propertyUpdates 는 surface),
    │      getAuditContext principal → envelope actor pass-through,
    │      onAfterCommitTransaction 의도적 미override 회귀 (reflection getMethod → declaringClass=PolarisEventListener)
    └── http/HttpEventPosterTest.java           (18 tests)
        ── 실제 JDK HttpServer 로 wire 포맷 (Authorization 헤더, JSON object 구조, endpoint-url 미설정 시 skip),
           inflight backpressure drop, sync 모드 timeout, malformed endpoint-url permit release,
           invalid request-timeout permit release;
           circuit breaker / replay buffer 회귀:
           ── circuitOpensAfterConsecutiveFailuresAndSkipsSubsequentEmits
           ── circuitClosesAfterCooldownAndSuccessfulProbe
           ── circuitReopensAfterCooldownAndFailedProbe
           ── halfOpenAllowsOnlyOneProbeUnderConcurrentEmits
           ── nonRetryable4xxDoesNotOpenCircuitOrBufferForReplay
           ── retryable429OpensCircuitAndBuffersForReplay
           ── replayBufferRedeliversAfterReceiverRecovers
           ── replayBufferSurvivesFailedProbeAndDeliversAfterEventualRecovery
           ── replayBufferDropsOldestWhenFull
```

총 **47 tests**. `./gradlew :polaris-extensions-event-rest-forwarder:test` 로 전수 실행된다.

**테스트 실행**

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./gradlew :polaris-extensions-event-rest-forwarder:test
```

---

## 11. 알려진 제약

- **이벤트 재전송 — 부분적 보전 (in-memory)**: replay buffer (default 1000 events) 가 일시 outage 동안의 이벤트를 보전한다. 단 (1) 큐 가득 시 drop-oldest, (2) 서버 재시작 시 큐 손실. 영구 보전이 필요하면 별도 outbox 설계가 필요하다.
- **View / Principal / Role / Policy / Generic Table 이벤트 미처리**: 현재 12 종 catalog/namespace/table 이벤트만 forward. 필요 시 핸들러 추가만 하면 되며 (delegator 변경 불필요), envelope 스키마는 확장 가능 (`@JsonInclude(NON_NULL)` 덕분에 새 필드 추가가 wire-compatible).
- **초기 일괄 동기화 없음**: 리스너 활성화 이전에 존재하던 카탈로그·테이블은 receiver 에 나타나지 않는다. 초기 sync 가 필요하면 별도 스크립트 (REST API 로 list → POST envelope) 가 필요.
- **중복 envelope**: 동일 (catalog, table) 에 대해 여러 번 emit 가능 (예: `onAfterUpdateTable` + `onAfterCommitTable`). receiver 가 자체 dedup 책임.
- **post-load 부수효과 (forwarder 활성 시)**: (Section 2.1 참조) rename / updateProperties 후 marker 기반 추가 load 호출 동반. 인가 분리 시 메타데이터 손실 가능 (DEBUG 로그만), race condition 시 stale 메타데이터 emit 가능, 매 호출마다 metadata fetch + 권한평가 1 회 추가 비용. marker 미구현 listener 활성 시에는 추가 호출 자체 없음.
- **4xx 분류의 단순화**: `isRetryableFailure` 는 5xx, 408, 429 만 retryable 로 분류. 그 외 4xx (422 Unprocessable Entity 등) 는 모두 non-retryable. receiver 가 일시적으로 422 를 반환한 후 곧 복구되는 환경에서는 이벤트 손실로 보일 수 있음.
- **strict ordering 미보장 (async 기본)**: HTTP/2 multiplexing 으로 동일 (catalog, table) 의 두 emit 이 receiver 측에서 뒤바뀌어 도달 가능. ordering 이 중요하면 `synchronous-mode=true` (latency trade-off) 또는 receiver 측에서 timestampMs 기반 정렬.
