# Polaris 1.3.0 + DataHub Listener — 작업 브랜치

## 이 폴더는 무엇인가

Apache Polaris **1.3.0-incubating** (정식 릴리스 태그 그대로) 에 DataHub event listener extension 을 더한 워크스페이스. **실제 작업·배포** 가 이루어지는 폴더입니다.

- 브랜치: `polaris-1.3.0-datahub` (`apache-polaris-1.3.0-incubating` 태그에서 분기)
- Base HEAD: `c940ded0` (GitHub 릴리스와 byte 단위 동일)
- `version.txt`: `1.3.0-incubating`
- 원격: `https://github.com/apache/polaris.git`

`/Users/1113435/ai-data-platform/polaris-source` 에 있는 레퍼런스 워크스페이스도 동일한 DataHub listener 를 가지고 있지만 아래 마이그레이션 후 적용된 버그 픽스는 **포함되어 있지 않습니다**. 이 폴더가 더 정확한 정식 버전입니다.

## DataHub Event Listener

단일 Gradle 서브프로젝트로 HTTP 기반 listener 를 제공합니다:

| 모듈 | 경로 | 내용 |
|------|------|------|
| `polaris-extensions-datahub-listener` | `extensions/datahub-listener/` | HTTP listener (`DataHubConfiguration`, `DataHubEventMapper`, `AbstractDataHubEventListener`, `HttpEmitter`, `DataHubHttpEventListener`) |

`application.properties` (또는 Helm `advancedConfig`) 에서 활성화합니다:

```properties
polaris.event-listener.type=datahub-http
polaris.event-listener.datahub.gms-url=http://localhost:8080
polaris.event-listener.datahub.token=<PAT>
polaris.event-listener.datahub.platform-instance=polaris
polaris.event-listener.datahub.env=PROD
polaris.event-listener.datahub.synchronous-mode=false
# 격리 가드 (DataHubConfiguration 의 @WithDefault 값. runtime override 가능)
polaris.event-listener.datahub.connect-timeout=5s
polaris.event-listener.datahub.request-timeout=10s
polaris.event-listener.datahub.max-inflight-emits=1000
# Optional: catalog → DataHub Domain URN (dataset-level only, no fallback)
polaris.event-listener.datahub.domain-mapping.<catalog>=urn:li:domain:<id>
```

기본값은 `no-op` 으로 유지됩니다. 빌드(`runtime/server/build.gradle.kts`)에는 listener 가 포함됩니다:

```gradle
runtimeOnly(project(":polaris-extensions-datahub-listener"))
```

> **참고**: 초기에는 SDK 기반 모듈(`datahub-listener-sdk`, `io.acryl:datahub-client`)도 함께 보관했으나
> Avro 1.11.4 ↔ 1.12.0 충돌 위험이 있고 동일 wire 결과를 HTTP 모드로 더 가볍게 달성할 수 있어 제거되었습니다.

**Helm 의 경우**: 운영 차트는 **`helm/benchmarks-polaris/`** (아래 "Helm chart 두 종류" 참조). `eventListener.type=datahub-http` 만 ConfigMap 에 두고, 나머지 DataHub 키와 token 은 **`extraEnv`** 에 `POLARIS_EVENT_LISTENER_DATAHUB_*` env vars 로 주입합니다 (Quarkus 가 ConfigMapping 으로 자동 매핑). 파일 properties 로 주입하면 SmallRye `validate-unknown` 가드에 걸리니 주의 — 자세한 내용은 아래 컨벤션 항목 5.

## 이 브랜치에 적용된 버그 픽스 (`/polaris-source` 와의 차이)

진짜 의미 있는 정확성 개선들입니다 — 리팩토링 시 절대로 회귀시키지 마세요:

1. **`onAfterUpdateNamespaceProperties` 가 더 이상 DataHub 의 custom properties 를 덮어쓰지 않음** — 이 이벤트는 *변경된* 키 목록만 가지고 있으므로, `AbstractDataHubEventListener` 가 `Map.of()` 대신 `null` 을 전달하고 `DataHubEventMapper.namespaceContainerAspects` 는 properties 가 unknown 일 때 `containerProperties` aspect 를 emit 하지 않도록 합니다.
2. **DataHub UI 에서 nested namespace 계층 구조 표시** — `namespaceContainerAspects` 가 이제 `container.container` 를 항상 catalog 가 아닌 *직접 부모* namespace 로 (`parentContainerUrn` 헬퍼 사용) 연결합니다. 즉 `a.b.c` 의 부모는 `a.b` container 가 됩니다.
3. **`namespaceContainerUrnFromRaw` 가 빈 입력을 처리** — trailing-dot URN 을 만들지 않고 `catalogContainerUrn` 을 그대로 반환합니다.
4. **`AbstractDataHubEventListener.shutdown` 이 null-safe** — Quarkus proxy 생성을 위한 no-args 생성자가 `emitter = null` 로 셋업되므로, 가드 없이는 proxy 인스턴스의 `@PreDestroy` 에서 NPE 가 발생합니다.
5. **`HttpEmitter.close()` 가 내부 `HttpClient` 를 close** — JDK 21 의 connection pool / executor 를 깔끔하게 종료하기 위해 필요합니다.
6. **`onAfterRenameTable` 이 이름 변경 후 테이블 properties 를 보존** — `AfterRenameTableEvent` 에 `LoadTableResponse` 필드를 추가하고, `IcebergRestCatalogEventServiceDelegator.renameTable` 이 rename 직후 `delegate.loadTable()` 을 호출하여 목적지 테이블 메타데이터를 이벤트에 담습니다. 추가 `loadTable` 비용은 *필요한* listener 만 부담하도록 `RequiresPostRenameTableMetadata` marker interface 로 opt-in 합니다 — `AbstractDataHubEventListener` 만 marker 를 구현하고, 그 외 (`no-op`, `persistence-in-memory-buffer`, `aws-cloudwatch`) 는 추가 로드 없이 `null` payload 를 받습니다. 이를 위해 `runtime/service/` 세 파일이 추가/변경되었습니다 (아래 "피해야 할 것" 참조).

테스트 수: `DataHubEventMapperTest` 19 + `AbstractDataHubEventListenerTest` 8 + `HttpEmitterTest` 22 = **49** (DataHub 다운/hang/잘못된 설정 격리, URN escape, replay buffer stale-replay 가드, commitTransaction no-op 보호 등 회귀 테스트 포함).

## Helm chart 두 종류

| 경로 | 역할 | 수정 가능 여부 |
|---|---|---|
| `helm/polaris/` | **Apache 업스트림 1.3.0 차트 (참고용만)** — Polaris 업스트림 default 차트 그대로 보관. 운영 배포에 쓰지 **말 것** (templates 이 운영 default 들을 소비하지 않음 — minio/opa/topologySpread/realm-header/RSA-hook/secret-as-key 모두 미연결) | 1.3.x 포인트 릴리스 업스트림 머지 시 diff 추적용으로만 수정. 운영 친화 default 박지 말 것 |
| `helm/benchmarks-polaris/` | **운영 배포 차트** — S3/MinIO env, OPA hook, RSA secret pre-install hook, topologySpread, DataHub listener env vars 패턴 등 운영 친화 기능 포함. 실제 `helm install`/`helm upgrade` 대상. release name 도 `benchmarks-polaris` | 운영 요구사항 변경 시 여기서 수정. 새 listener/extension 추가 시 이 차트에 통합 |

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
# DataHub extension 테스트 (49 passing 기대)
./gradlew :polaris-extensions-datahub-listener:test

# 전체 distribution — fast-jar 산출 위치: runtime/server/build/quarkus-app/
./gradlew :polaris-server:quarkusBuild

# 로컬 dev 실행 (시드 자격증명 POLARIS,root,s3cr3t)
./gradlew :polaris-server:quarkusRun

# fast-jar 에 listener jar 가 들어갔는지 확인
ls runtime/server/build/quarkus-app/lib/main/ | grep datahub
# expected: org.apache.polaris.polaris-extensions-datahub-listener-1.3.0-incubating.jar
```

## 컨벤션: 새로운 PolarisEventListener 추가 방법

1. 클래스: `@ApplicationScoped @Identifier("<name>")` + `org.apache.polaris.service.events.listeners.PolarisEventListener` 구현.
2. **부모 클래스에 명시적 no-args 생성자가 필요합니다.** Quarkus 가 빌드 타임에 proxy 서브클래스를 생성하면서 `super()` 를 호출합니다. 없으면 빌드가 *"It's not possible to automatically add a synthetic no-args constructor to an unproxyable bean class."* 메시지로 실패합니다. `AbstractDataHubEventListener` 가 그 패턴을 갖고 있으니 그대로 따라 하세요.
3. Config 인터페이스: `@ConfigMapping(prefix = "polaris.event-listener.<name>")` + `@ApplicationScoped`. `runtime/service` 외부에서는 `@StaticInitSafe` 를 피하세요 (해당 import 가 extension 클래스패스에 없음).
4. **반드시 두 곳 모두에** 등록하세요: `gradle/projects.main.properties` *와* `runtime/server/build.gradle.kts` (`runtimeOnly`). 후자를 빠뜨리면 Jandex bean 인덱싱이 조용히 깨집니다.
5. **헬름/배포 시 세부 설정은 env vars 로만 주입** — `polaris.event-listener.<name>.*` 키를 `application.properties` (또는 helm ConfigMap) 에 박지 **마세요**. SmallRye Config 의 `validate-unknown=true` (`runtime/defaults/application.properties` 의 baked-in default) 가 `runtimeOnly` 로 추가된 extension 의 `@ConfigMapping` 을 *file source 검증 단계에서 root 로 찾지 못해* `SRCFG00050: does not map to any root` 에러로 부팅이 실패합니다. 같은 키를 **환경변수** (`POLARIS_EVENT_LISTENER_<NAME>_<KEY>`) 로 주입하면 검증을 우회해서 정상 동작합니다. `helm/benchmarks-polaris/values.yaml` 의 `extraEnv` 가 DataHub 키 7개를 env 로 넣는 패턴이 그래서입니다. `polaris.event-listener.type=<name>` (타입 선택) 만 ConfigMap 에 넣어도 됩니다 — 등록된 root (`PolarisEventListenerConfiguration`) 에 매칭되므로 안전.

## 피해야 할 것

- `main` (1.4.0-SNAPSHOT, 약 793 커밋 앞섬) 을 체크아웃하지 **마세요**. 이 브랜치는 1.3.0 릴리스 태그에 뿌리내려 있어야 합니다.
- 미리 빌드된 Polaris 이미지에 listener jar 만 런타임으로 떨어뜨리지 **마세요** — Quarkus 는 빌드 타임에 CDI 빈을 인덱싱합니다. 이미지를 다시 빌드하세요.
- `polaris-core/` 를 수정하지 **마세요**. 1.3.x 포인트 릴리스 업그레이드가 깨끗하게 가도록 listener 는 의도적으로 `extensions/datahub-listener/` 에 격리되어 있습니다.
- `runtime/service/` 는 **원칙적으로** 수정하지 마세요. 단, 버그 픽스 #6 (rename table) 을 위해 다음 세 파일이 이미 추가/변경되었습니다 — 추가 변경 시에는 1.4.x 업스트림과의 diff 를 최소화하세요:
  - `runtime/service/src/main/java/org/apache/polaris/service/events/IcebergRestCatalogEvents.java` (`AfterRenameTableEvent` 에 `LoadTableResponse` 필드 추가)
  - `runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/IcebergRestCatalogEventServiceDelegator.java` (rename 후 `delegate.loadTable()` 추가 호출 — `RequiresPostRenameTableMetadata` opt-in 만)
  - `runtime/service/src/main/java/org/apache/polaris/service/events/listeners/RequiresPostRenameTableMetadata.java` (신규 marker interface)
- 위에 나열된 버그 픽스를 되돌리지 **마세요** — 우선 `AbstractDataHubEventListenerTest` / `DataHubEventMapperTest` 의 해당 테스트가 원하는 동작을 여전히 커버하는지 확인 후에만 진행하세요.

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
