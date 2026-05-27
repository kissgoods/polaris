# Polaris 1.3.0 + Event REST Forwarder — 작업 브랜치

## 이 폴더는 무엇인가

Apache Polaris **1.3.0-incubating** (정식 릴리스 태그 그대로) 에 generic event REST forwarder extension 을 더한 워크스페이스. **실제 작업·배포** 가 이루어지는 폴더입니다.

- 브랜치: `feature/datahub-listener-api` (`apache-polaris-1.3.0-incubating` 태그에서 분기)
- Base HEAD: `c940ded0` (GitHub 릴리스와 byte 단위 동일)
- `version.txt`: `1.3.0-incubating`
- 원격: `https://github.com/apache/polaris.git`

> **이전 구조와의 차이**: 이전 버전은 `extensions/datahub-listener/` 에서 Polaris 안에서 DataHub URN/Aspect/Domain payload 까지 직접 만들었지만, 이제는 raw event envelope 만 receiver REST 엔드포인트로 직송하고 DataHub 변환은 receiver 책임. Polaris 는 DataHub 지식을 전혀 갖지 않음. 자세한 사유는 `extensions/event-rest-forwarder/DESIGN.md` §1 참조.

## Event REST Forwarder

단일 Gradle 서브프로젝트로 HTTP 기반 forwarder 를 제공합니다:

| 모듈 | 경로 | 내용 |
|------|------|------|
| `polaris-extensions-event-rest-forwarder` | `extensions/event-rest-forwarder/` | Generic HTTP forwarder (`RestForwarderConfiguration`, `EventEnvelope`, `EventSerializer`, `AbstractEventForwarderListener`, `HttpEventPoster`, `RestForwarderEventListener`) |

`application.properties` (또는 Helm `advancedConfig`) 에서 활성화합니다:

```properties
polaris.event-listener.type=rest-forwarder
polaris.event-listener.rest-forwarder.endpoint-url=http://localhost:8080/polaris/events
polaris.event-listener.rest-forwarder.token=<bearer-token-optional>
polaris.event-listener.rest-forwarder.synchronous-mode=false
# 격리 가드 (RestForwarderConfiguration 의 @WithDefault 값. runtime override 가능)
polaris.event-listener.rest-forwarder.connect-timeout=5s
polaris.event-listener.rest-forwarder.request-timeout=10s
polaris.event-listener.rest-forwarder.max-inflight-emits=1000
```

기본값은 `no-op` 으로 유지됩니다. 빌드(`runtime/server/build.gradle.kts`)에는 forwarder 가 포함됩니다:

```gradle
runtimeOnly(project(":polaris-extensions-event-rest-forwarder"))
```

Wire 포맷 (1 event = 1 JSON object POST):
```json
{"eventType":"AfterCreateTable","timestampMs":...,"actor":"...","catalog":"cat",
 "namespace":["ns","sub"],"table":"tbl","tableMetadataJson":"<Iceberg canonical JSON>"}
```
`tableMetadataJson` 은 `TableMetadataParser.toJson` 결과 — receiver 가 `fromJson` 으로 round-trip. 자세한 envelope 스키마는 `extensions/event-rest-forwarder/DESIGN.md` §4.2.

**Helm 의 경우**: 운영 차트는 **`helm/benchmarks-polaris/`** (아래 "Helm chart 두 종류" 참조). `eventListener.type=rest-forwarder` 만 ConfigMap 에 두고, 나머지 forwarder 키와 token 은 **`extraEnv`** 에 `POLARIS_EVENT_LISTENER_REST_FORWARDER_*` env vars 로 주입합니다 (Quarkus 가 ConfigMapping 으로 자동 매핑). 파일 properties 로 주입하면 SmallRye `validate-unknown` 가드에 걸리니 주의 — 자세한 내용은 아래 컨벤션 항목 5.

## 운영 보존 사항

이전 DataHub listener 에서 검증된 다음 동작들이 새 forwarder 에도 그대로 보존되어 있습니다 — 리팩토링 시 회귀시키지 마세요:

1. **`onAfterUpdateNamespaceProperties` 가 `currentProperties == null` 일 때 envelope `properties` 도 null 로 surface** — receiver 가 "load 실패" 와 "empty map" 을 구별 가능하도록. receiver 측 기존 properties 보존 책임을 명시적으로 위임.
2. **`AfterDropNamespace` 의 raw U+001F string 보존** — `EventSerializer` 가 split 하지 않고 `namespaceRaw` 로 그대로 전송. receiver 가 분리 결정.
3. **`AbstractEventForwarderListener.shutdown` 이 null-safe** — Quarkus proxy 생성을 위한 no-args 생성자가 `poster = null` 로 셋업되므로, 가드 없이는 proxy 인스턴스의 `@PreDestroy` 에서 NPE 발생.
4. **`HttpEventPoster.close()` 가 내부 `HttpClient` 를 close** — JDK 21 의 connection pool / executor 를 깔끔하게 종료.
5. **`onAfterRenameTable` 이 destination metadata 를 보존** — `AfterRenameTableEvent` 의 `LoadTableResponse` 필드를 사용. `IcebergRestCatalogEventServiceDelegator.renameTable` 이 rename 직후 `delegate.loadTable()` 을 호출하여 목적지 테이블 메타데이터를 이벤트에 담음. 추가 `loadTable` 비용은 *필요한* listener 만 부담하도록 `RequiresPostRenameTableMetadata` marker interface 로 opt-in — `AbstractEventForwarderListener` 만 marker 를 구현. 마찬가지로 `onAfterUpdateNamespaceProperties` 는 `RequiresPostUpdateNamespaceMetadata` marker 로 후속 `loadNamespaceMetadata` opt-in. 두 marker 를 통해 `no-op`, `persistence-in-memory-buffer`, `aws-cloudwatch` 는 추가 로드 비용 0.
6. **`onAfterCommitTransaction` 의도적 미override** — multi-table commit 의 per-table `onAfterCommitTable` 로 이미 sync 되므로 transaction-level 재emit 은 중복 envelope 만 발생. 회귀 테스트 `commitTransactionMustRemainNoOpToAvoidDuplicateEvents` 가 reflection 으로 부재 검증.
7. **Transaction 안의 commit table 이벤트는 atomic update 성공 후에만 publish** — `IcebergCatalogHandler.commitTransaction` 은 per-table `tableOps.commit()` 가 transaction workspace 에 queue 되는 시점에 `onAfterCommitTable` 을 fire 하지만, 최종 `metaStoreManager.updateEntitiesPropertiesIfNotChanged` 가 실패해 `CommitFailedException` 으로 rollback 될 수 있음. 그 사이 listener 가 eager emit 하면 receiver 가 Polaris 가 한 번도 commit 하지 않은 state 를 적용함. `TransactionalCommitTableDeferred` marker 를 구현하면 delegator 가 `beginTransaction` → delegate 실행 → `endTransaction(committed)` handshake 를 호출하고, listener 는 그 사이 commit 이벤트를 ThreadLocal 에 buffer 했다가 `committed=true` 일 때만 drain (rollback 시 discard). `AbstractEventForwarderListener` 만 marker 구현 — 다른 listener (`no-op` 등) 는 legacy eager-fire 유지. 회귀 테스트: `commitTableInsideTransactionIsBufferedUntilCommitSuccess` / `commitTableInsideTransactionIsDiscardedOnRollback` / `rolledBackTransactionDoesNotLeakBufferIntoNextRequest`.
8. **Rename envelope 가 source + destination 양쪽 키 노출** — `EventEnvelope.resourceKeys()` 가 rename 시 `{ source, destination }` 두 키 반환. `HttpEventPoster.purgeBufferedOlderSameResource` 가 overlap 검사로 buffered rename 을 newer commit-on-destination 이 들어와도 evict. 회귀 테스트: `successfulCommitOnRenameDestinationPurgesBufferedRename` / `renameEnvelopeResourceKeysIncludesBothSourceAndDestination`.
9. **Envelope identity 에 realm 포함** — `EventEnvelope.realm` 필드 + `resourceKey()` prefix 에 realm 포함. 다중-realm 배포에서 같은 catalog/table 이름이 다른 realm 에 있어도 receiver 의 dedup/ordering 키 충돌 없음. `RestForwarderEventListener` 가 `RealmContext.getRealmIdentifier()` 에서 가져옴; 단일-realm 또는 request scope 밖에서는 null 로 fallback (legacy single-realm 키 shape 유지).

테스트 수: `EventSerializerTest` 15 + `AbstractEventForwarderListenerTest` 14 + `HttpEventPosterTest` 18 = **47** (receiver 다운/hang/잘못된 설정 격리, circuit breaker, replay buffer, 4xx 분류, commitTransaction no-op 회귀 등 포함).

## Helm chart 두 종류

| 경로 | 역할 | 수정 가능 여부 |
|---|---|---|
| `helm/polaris/` | **Apache 업스트림 1.3.0 차트 (참고용만)** — Polaris 업스트림 default 차트 그대로 보관. 운영 배포에 쓰지 **말 것** (templates 이 운영 default 들을 소비하지 않음 — minio/opa/topologySpread/realm-header/RSA-hook/secret-as-key 모두 미연결) | 1.3.x 포인트 릴리스 업스트림 머지 시 diff 추적용으로만 수정. 운영 친화 default 박지 말 것 |
| `helm/benchmarks-polaris/` | **운영 배포 차트** — S3/MinIO env, OPA hook, RSA secret pre-install hook, topologySpread, event-rest-forwarder env vars 패턴 등 운영 친화 기능 포함. 실제 `helm install`/`helm upgrade` 대상. release name 도 `benchmarks-polaris` | 운영 요구사항 변경 시 여기서 수정. 새 listener/extension 추가 시 이 차트에 통합 |

운영 배포는 **반드시** 다음과 같이 실행:

```bash
helm upgrade benchmarks-polaris \
  /Users/1113435/ai-data-platform/polaris-1.3/helm/benchmarks-polaris/ \
  -n datahub-hynix --atomic --timeout 5m
```

`helm/polaris/` 는 업스트림 변경사항을 우리 운영 차트로 portable 시킬 때 비교 참조용으로만 보관합니다.

## 빌드 환경

**JDK 21 필수.** `settings.gradle.kts` 가 JDK 17 을 거부합니다. 이 머신의 `/usr/libexec/java_home -V` 는 JDK 21 을 *나열하지 않으므로* Homebrew 설치 경로를 명시적으로 사용하세요:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
```

## 자주 쓰는 명령

```bash
# Event REST forwarder 테스트 (47 passing 기대)
./gradlew :polaris-extensions-event-rest-forwarder:test

# 전체 distribution — fast-jar 산출 위치: runtime/server/build/quarkus-app/
./gradlew :polaris-server:quarkusBuild

# 로컬 dev 실행 (시드 자격증명 POLARIS,root,s3cr3t)
./gradlew :polaris-server:quarkusRun

# fast-jar 에 forwarder jar 가 들어갔는지 확인
ls runtime/server/build/quarkus-app/lib/main/ | grep forwarder
# expected: org.apache.polaris.polaris-extensions-event-rest-forwarder-1.3.0-incubating.jar
```

## 컨벤션: 새로운 PolarisEventListener 추가 방법

1. 클래스: `@ApplicationScoped @Identifier("<name>")` + `org.apache.polaris.service.events.listeners.PolarisEventListener` 구현.
2. **부모 클래스에 명시적 no-args 생성자가 필요합니다.** Quarkus 가 빌드 타임에 proxy 서브클래스를 생성하면서 `super()` 를 호출합니다. 없으면 빌드가 *"It's not possible to automatically add a synthetic no-args constructor to an unproxyable bean class."* 메시지로 실패합니다. `AbstractEventForwarderListener` 가 그 패턴을 갖고 있으니 그대로 따라 하세요.
3. Config 인터페이스: `@ConfigMapping(prefix = "polaris.event-listener.<name>")` + `@ApplicationScoped`. `runtime/service` 외부에서는 `@StaticInitSafe` 를 피하세요 (해당 import 가 extension 클래스패스에 없음).
4. **반드시 두 곳 모두에** 등록하세요: `gradle/projects.main.properties` *와* `runtime/server/build.gradle.kts` (`runtimeOnly`). 후자를 빠뜨리면 Jandex bean 인덱싱이 조용히 깨집니다.
5. **헬름/배포 시 세부 설정은 env vars 로만 주입** — `polaris.event-listener.<name>.*` 키를 `application.properties` (또는 helm ConfigMap) 에 박지 **마세요**. SmallRye Config 의 `validate-unknown=true` (`runtime/defaults/application.properties` 의 baked-in default) 가 `runtimeOnly` 로 추가된 extension 의 `@ConfigMapping` 을 *file source 검증 단계에서 root 로 찾지 못해* `SRCFG00050: does not map to any root` 에러로 부팅이 실패합니다. 같은 키를 **환경변수** (`POLARIS_EVENT_LISTENER_<NAME>_<KEY>`) 로 주입하면 검증을 우회해서 정상 동작합니다. `helm/benchmarks-polaris/values.yaml` 의 `extraEnv` 가 forwarder 키를 env 로 넣는 패턴이 그래서입니다. `polaris.event-listener.type=<name>` (타입 선택) 만 ConfigMap 에 넣어도 됩니다 — 등록된 root (`PolarisEventListenerConfiguration`) 에 매칭되므로 안전.

## 피해야 할 것

- `main` (1.4.0-SNAPSHOT, 약 793 커밋 앞섬) 을 체크아웃하지 **마세요**. 이 브랜치는 1.3.0 릴리스 태그에 뿌리내려 있어야 합니다.
- 미리 빌드된 Polaris 이미지에 forwarder jar 만 런타임으로 떨어뜨리지 **마세요** — Quarkus 는 빌드 타임에 CDI 빈을 인덱싱합니다. 이미지를 다시 빌드하세요.
- `polaris-core/` 를 수정하지 **마세요**. 1.3.x 포인트 릴리스 업그레이드가 깨끗하게 가도록 forwarder 는 의도적으로 `extensions/event-rest-forwarder/` 에 격리되어 있습니다.
- `runtime/service/` 는 **원칙적으로** 수정하지 마세요. 단, post-load marker 흐름 (rename table / updateNamespaceProperties) 을 위해 다음 다섯 파일이 이미 추가/변경되어 있습니다 — 추가 변경 시에는 1.4.x 업스트림과의 diff 를 최소화하세요:
  - `runtime/service/src/main/java/org/apache/polaris/service/events/IcebergRestCatalogEvents.java` (`AfterRenameTableEvent` 에 `LoadTableResponse` 필드, `AfterUpdateNamespacePropertiesEvent` 에 `currentProperties` 필드 — 둘 다 하위호환 생성자 유지)
  - `runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/IcebergRestCatalogEventServiceDelegator.java` (rename 후 `delegate.loadTable()`, updateProperties 후 `delegate.loadNamespaceMetadata()` 추가 호출 — 각각 marker opt-in 만)
  - `runtime/service/src/main/java/org/apache/polaris/service/events/listeners/RequiresPostRenameTableMetadata.java` (marker interface)
  - `runtime/service/src/main/java/org/apache/polaris/service/events/listeners/RequiresPostUpdateNamespaceMetadata.java` (marker interface)
  - `runtime/service/src/main/java/org/apache/polaris/service/events/listeners/TransactionalCommitTableDeferred.java` (marker — listener buffers per-table commit events during a transaction and the delegator drains them only after the atomic commit succeeds; `IcebergCatalogHandler.commitTransaction` fires `onAfterCommitTable` BEFORE the final `updateEntitiesPropertiesIfNotChanged` so eager emission would publish state for a transaction Polaris might roll back)
- 위에 나열된 운영 보존 사항을 되돌리지 **마세요** — 우선 `AbstractEventForwarderListenerTest` / `EventSerializerTest` / `HttpEventPosterTest` 의 해당 테스트가 원하는 동작을 여전히 커버하는지 확인 후에만 진행하세요.

## `datahub-http` → `rest-forwarder` 마이그레이션

이 브랜치는 이전 `extensions/datahub-listener/` (`@Identifier("datahub-http")`) 를 제거하고 `extensions/event-rest-forwarder/` (`@Identifier("rest-forwarder")`) 로 대체합니다. **runtime config 의 breaking change** 이므로 운영 환경 업그레이드 시 다음 순서를 지키세요:

1. **이미지 태그를 분리** — 이전 datahub-http 이미지 (`v1.3.0-integration-datahub`) 와 새 forwarder 이미지 (`v1.3.0-rest-forwarder`) 를 **다른 immutable 태그** 로 publish. 같은 태그 재사용은 `IfNotPresent` 캐시 / split-brain 위험 (`image-build.md` 참조).
2. **Helm `values.yaml` 동시 갱신** — `image.tag` 와 `eventListener.type` 를 같은 helm upgrade 안에서 모두 새 값으로. `extraEnv` 의 `POLARIS_EVENT_LISTENER_DATAHUB_*` env vars 도 `POLARIS_EVENT_LISTENER_REST_FORWARDER_*` 로 일괄 교체. 둘 중 하나만 바꾸면 부팅 실패:
   - 옛 이미지 + 새 type → `@Identifier("rest-forwarder")` bean 없음 → SmallRye/CDI 가 `polaris.event-listener.type=rest-forwarder` 매핑 실패로 부팅 거부
   - 새 이미지 + 옛 type → `@Identifier("datahub-http")` bean 없음 → 동일 실패
   - 옛 env vars + 새 type → 새 `RestForwarderConfiguration` 이 `endpoint-url` 미설정이라 매 emit 마다 skip + 에러 로그 (catalog 차단은 아님)
3. **부팅 실패 시 신호** — pod 로그에 `Unsatisfied dependency expressed through @Identifier` 또는 `polaris.event-listener.type=... not found` 비슷한 메시지. helm `--atomic` 옵션을 사용하면 자동 롤백.
4. **수신자 배포 선행** — `endpoint-url` 가 가리키는 receiver 가 이미 떠 있어야 합니다. circuit breaker + replay buffer 가 일시 outage 는 흡수하지만, 초기 부팅 시점부터 receiver 미배치면 startup 직후 발생하는 catalog 이벤트들이 buffer cap 안에서만 보전됩니다.

## 자동 codex review 결과를 처리할 때

`.claude/settings.local.json` 의 Stop hook 으로 `codex review --uncommitted` 가 매 turn 후 자동 실행되고, 결과가 컨텍스트로 들어옵니다 (`~/.claude/scripts/codex-review-on-stop.sh`, `asyncRewake`). 이 결과를 받았을 때 다음 순서를 따르세요:

1. **라인 번호 / 인용 코드 검증** — review 가 가리키는 파일·라인·함수가 *현재* 코드와 매치하는지 먼저 확인합니다 (`Read` 또는 `awk 'NR==<line>'`). 라인 번호와 실제 코드가 어긋나면 stale 분석일 가능성이 높습니다.
2. **본문 검증** — 라인이 어긋나도 결함 설명이 일반화되어 있어 현재 코드에 여전히 적용될 수 있는지 본문 자체로 한 번 더 판단합니다.
3. **결정**:
   - **진짜 결함** → 분석 + 패치 + 회귀 테스트 추가 (가능하면).
   - **stale** (라인·본문 모두 현재 코드에 해당 없음) → 추가 변경 없이 "이 review 는 이전 상태 분석이라 현재 코드에는 해당되지 않습니다." 로 보고하고 마무리. 새 codex 실행을 사용자에게 권장.
4. **반복 의심** — 같은 결함이 두 라운드 연속 보고되면 (특히 라인 번호까지 동일) codex 의 환각이거나 hook dedup 가 깨졌을 가능성. 사용자에게 명시적으로 보고하세요.
5. **review 본문은 한국어로 응답** — 사용자가 한국어로 review 를 paste 하므로 응답도 한국어 유지.

## Plan 및 작업 이력

전체 구현 계획과 마이그레이션 로그: `/Users/1113435/.claude/plans/polaris-version-fizzy-stardust.md`.
