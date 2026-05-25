# Polaris 1.3.0 + DataHub Listener — Docker 이미지 빌드 & Push

DataHub event listener 를 포함한 **커스텀 이미지** 를 빌드해서 Docker Hub `skthynix/polaris:datahub-v1.3.0` 으로 push 하는 절차입니다.

> **왜 공식 이미지를 못 쓰나**: `apache/polaris:1.3.0-incubating` 에는 `extensions/datahub-listener/` 의 jar 가 없습니다. Quarkus 가 빌드 타임에 CDI 빈을 인덱싱하므로 이미 빌드된 이미지에 listener jar 만 떨궈도 동작하지 않습니다 → 이미지를 다시 빌드해야 합니다.

대상 이미지: `skthynix/polaris:v1.3.0-integration-datahub`

---

## 0. 전제 조건

| 항목 | 값 | 확인 |
|---|---|---|
| JDK | **21** (1.3.x 빌드 요구사항) | `java -version` |
| Docker daemon | 실행 중 | `docker info` |
| Docker Hub 로그인 | `skthynix` 계정 | `docker login` |
| 작업 디렉터리 | `/Users/1113435/ai-data-platform/polaris-1.3` | `pwd` |
| 브랜치 | `polaris-1.3.0-datahub` | `git branch --show-current` |

> Apple Silicon 머신에서 빌드해도 운영 클러스터(amd64) 에 push 하려면 **`linux/amd64` 멀티플랫폼 빌드** 가 필요합니다. `docker buildx` 가 설치돼 있어야 합니다 (`docker buildx version`).

---

## 1. JDK 21 환경 설정

이 머신의 `/usr/libexec/java_home -V` 는 JDK 21 을 *나열하지 않습니다*. Homebrew 경로를 명시적으로 지정하세요.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
java -version
# openjdk version "21.x.x" ...
```

---
## 2. DataHub Listener 테스트 (선택, 권장)

49개 테스트가 통과하는지 먼저 확인합니다.

```bash
cd /Users/1113435/ai-data-platform/polaris-1.3-github/
./gradlew :polaris-extensions-datahub-listener:test
# BUILD SUCCESSFUL — DataHubEventMapperTest 19 + AbstractDataHubEventListenerTest 8 + HttpEmitterTest 22
```

---

## 3. Quarkus fast-jar 빌드

산출물은 `runtime/server/build/quarkus-app/` 에 실행 가능한 fast-jar 디렉터리를 생성합니다.

```bash
./gradlew :polaris-server:quarkusBuild
```

빌드가 끝나면 **listener jar 포함 여부** 를 검증합니다:

```bash
ls runtime/server/build/quarkus-app/lib/main/ | grep datahub
# 기대 결과:
# org.apache.polaris.polaris-extensions-datahub-listener-1.3.0-incubating.jar
```

> 위 jar 가 없으면 `runtime/server/build.gradle.kts` 에 `runtimeOnly(project(":polaris-extensions-datahub-listener"))` 가 누락된 것입니다.

---

## 4. Docker 이미지 빌드 (arm64 + amd64 멀티아키)

`runtime/server/src/main/docker/Dockerfile.jvm` 을 사용합니다. 베이스 이미지는 `registry.access.redhat.com/ubi9/openjdk-21-runtime`.

> ⚠️ **같은 태그로 재빌드 시 캐시 함정**
> - `docker buildx --push` 는 멀티아키 매니페스트를 registry 로 push 하지만 **로컬 docker 데몬에는 적재하지 않습니다** (`--load` 와 `--push` 동시 사용 불가).
> - 그래서 빌드 직후 `docker run skthynix/polaris:datahub-v1.3.0` 을 하면 docker 가 **이전에 캐시된 로컬 이미지** 를 그대로 씁니다 (코드 수정이 반영 안 됨).
> - 더해서 buildx 가 이전 빌드의 레이어 캐시를 재사용해 *변경된 파일* 마저 옛 레이어를 가져올 수 있습니다.

같은 태그로 강제 재빌드할 때는 아래 **§4a "Force clean rebuild"** 스크립트를 쓰세요.

### 4a. Force clean rebuild (같은 태그 재빌드 권장)

```bash
TAG=skthynix/polaris:v1.3.2-integration-datahub

# 1) 로컬 이미지 + dangling 잔존물 정리
docker image rm -f $TAG 2>/dev/null || true
docker image prune -f

# 2) buildx 빌더 캐시 비우기 (이 머신 전체 buildx 캐시 — 다른 프로젝트도 영향 받음)
docker buildx prune -af

# 3) Quarkus fast-jar 재빌드 (코드/설정 변경 반영)
cd /Users/1113435/ai-data-platform/polaris-1.3-github
./gradlew :polaris-server:quarkusBuild

# 4) 멀티아키 빌드 + push (--no-cache --pull 로 모든 캐시 우회)
# --platform linux/amd64 ,linux/arm64 \
cd runtime/server
docker buildx build \
  --platform linux/amd64 \
  --no-cache \
  --pull \
  -f src/main/docker/Dockerfile.jvm \
  -t skthynix/polaris:v1.3.2-integration-datahub \
  --push \
  .
cd ../..

# 5) 새로 push 된 매니페스트를 로컬로 강제 pull (manifest 만 받음)
docker pull $TAG
```

옵션 의미:
- `--no-cache`: Dockerfile 의 모든 step 을 캐시 무시하고 새로 실행
- `--pull`: 베이스 이미지 (`ubi9/openjdk-21-runtime`) 도 매번 최신 다이제스트로 재확인
- `docker pull` (마지막 단계): registry 에 갓 push 된 매니페스트를 받아 로컬 데몬에 적재 → 이후 `docker run` 이 새 이미지를 사용

### 4b. 일반 빌드 (새 태그일 때)

캐시 함정이 없으니 단순합니다.

```bash
cd /Users/1113435/ai-data-platform/polaris-1.3-github/runtime/server
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f src/main/docker/Dockerfile.jvm \
  -t skthynix/polaris:v1.3.2-integration-datahub \
  --push \
  .
```

---

## 5. 이미지 검증 docker buildx rm multiplatform

이미지 안에 listener jar 가 실제로 들어있는지 최종 확인합니다.

```bash
docker run --rm --entrypoint sh skthynix/polaris:v1.3.2-integration-datahub \
  -c "ls /deployments/lib/main/ | grep datahub"
# 기대: org.apache.polaris.polaris-extensions-datahub-listener-1.3.0-incubating.jar
```

빌드 시각을 빠르게 확인해 *로컬에 옛 이미지가 캐시된 게 아닌지* 검증:

```bash
docker image inspect skthynix/polaris:v1.3.2-integration-datahub --format '{{.Created}}'
# 방금 빌드한 시각이 나와야 함. 옛 시각이면 §4a 의 docker pull 이 안 됐다는 뜻
```

부팅 smoke test (시드 자격증명, 즉시 종료):

```bash
docker run --rm -p 8181:8181 -p 8182:8182 \
  -e POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t \
  -e POLARIS_PERSISTENCE_TYPE=in-memory \
  skthynix/polaris:datahub-v1.3.2
# Ctrl-C 로 종료. "Polaris Server started" 가 보이면 OK
```

---

## 6. Helm 으로 배포

> ⚠️ **운영 배포는 반드시 `helm/benchmarks-polaris/` 사용**. `helm/polaris/` 는 Apache 업스트림 참고용이라 운영 default (MinIO env, OPA, RSA hook, datahub env vars 등) 가 wire 되어 있지 않습니다. 자세한 내용은 `CLAUDE.md` 의 "Helm chart 두 종류" 참조.

이미지가 Docker Hub 에 올라간 뒤 `helm/benchmarks-polaris/values.yaml` 의 `image.repository` / `image.tag` 가 이 태그를 가리키도록 유지되어 있는지 확인합니다.

```yaml
image:
  repository: skthynix/polaris
  pullPolicy: IfNotPresent
  tag: "datahub-v1.3.2"
```

배포 (네임스페이스 `datahub-hynix`, release name `benchmarks-polaris`):

```bash
cd /Users/1113435/ai-data-platform/polaris-1.3-github

# 렌더링만 미리 확인
helm template benchmarks-polaris helm/benchmarks-polaris/ | less

# 실제 설치 또는 업그레이드 (이미 release 가 있으면 upgrade, 없으면 install)
helm upgrade --install benchmarks-polaris helm/benchmarks-polaris/ \
  --namespace datahub-hynix --create-namespace \
  --atomic --timeout 5m
```

배포 후 listener 활성화 확인:

```bash
kubectl -n datahub-hynix logs deploy/benchmarks-polaris --tail=200 \
  | grep -iE "Polaris Server started|datahub|event-listener"
# 기대: "Polaris Server started in X.Xs", datahub env vars 적용 흔적
```

---

## 7. 새 태그로 재배포

코드 수정 후 새 태그로 갱신할 때의 표준 흐름 (멀티아키):

```bash
NEW_TAG=v1.3.2-integration-datahub

# 1) Fast-jar 재빌드
cd /Users/1113435/ai-data-platform/polaris-1.3-github
./gradlew :polaris-server:quarkusBuild

# 2) 멀티아키 빌드 + push (새 태그라 캐시 함정 없음)
cd runtime/server
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f src/main/docker/Dockerfile.jvm \
  -t skthynix/polaris:v1.3.1-integration-datahub \
  --push \
  .
cd ../..

# 3) Helm 업그레이드 (values.yaml 수정 없이 image.tag 만 override)
helm upgrade benchmarks-polaris helm/benchmarks-polaris/ \
  --namespace datahub-hynix \
  --set image.tag=${NEW_TAG} \
  --atomic --timeout 5m
```

> **참고**: `latest` 태그는 의도적으로 쓰지 않습니다. `pullPolicy: IfNotPresent` 상태에서 `latest` 를 쓰면 노드 캐시 때문에 새 빌드가 반영되지 않을 수 있습니다.
> 같은 태그를 유지하면서 코드만 갱신해야 한다면 §4a 의 force clean rebuild 절차를 따르세요.

---

## 자주 발생하는 문제

| 증상 | 원인 | 해결 |
|---|---|---|
| `quarkusBuild` 가 JDK 17 거부 메시지로 실패 | `JAVA_HOME` 이 17 을 가리킴 | 위 1번 단계 재실행 |
| 빌드 후 `lib/main/` 에 datahub jar 없음 | `runtime/server/build.gradle.kts` 변경 누락 | `runtimeOnly(project(":polaris-extensions-datahub-listener"))` 확인 |
| `buildx: command not found` | docker desktop 의 buildx 비활성 | `docker buildx install` 또는 Docker Desktop 재시작 |
| **같은 태그로 재빌드했는데 옛 동작 그대로** | `--push` 가 로컬 docker 에 적재 안 함 → `docker run` 이 캐시된 옛 이미지 사용 | §4a force clean rebuild (특히 `docker pull $TAG` 마지막 단계) 실행. `docker image inspect ... --format '{{.Created}}'` 로 시각 확인 |
| pod 가 `ImagePullBackOff` | private 레지스트리인 경우 imagePullSecrets 없음 | Docker Hub public 이미지면 무관, private 이면 `imagePullSecrets` 설정 |
| pod 가 `SRCFG00050: polaris.event-listener.datahub.X does not map to any root` 로 CrashLoopBackOff | `runtimeOnly` extension 의 `@ConfigMapping` 이 file source 검증 단계에 등록 안 됨 — `validate-unknown=true` 가 거부. env vars 로는 통과 | helm chart 의 `eventListener.datahub.*` 키를 ConfigMap 에 두지 말고 deployment 의 `extraEnv` 로 `POLARIS_EVENT_LISTENER_DATAHUB_<KEY>` 형태로 주입. `helm/benchmarks-polaris/values.yaml` 의 패턴 참조. `polaris.event-listener.type` 만 ConfigMap 에 두는 건 OK |
| 부팅 후 DataHub 로 emit 가 안 됨 | `eventListener.type` 이 `datahub-http` 가 아니거나 GMS URL 오타 | ConfigMap + env vars 둘 다 확인: `kubectl -n datahub-hynix get deploy benchmarks-polaris -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="POLARIS_EVENT_LISTENER_DATAHUB_GMS_URL")].value}'` |
