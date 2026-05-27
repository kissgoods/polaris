**# Polaris 1.3.0 Hive Metastore Federation 테스트 계획

## Context

공식 문서 (https://polaris.apache.org/releases/1.3.0/federation/hive-metastore-federation/) 에 기술된 **HMS federation** 기능을 현 워크스페이스 (`polaris-1.3-git`, branch `main`, base tag `apache-polaris-1.3.0-incubating`) 에서 실제로 동작시켜 검증한다.

**검증 목표**: 기존 운영중인 HMS (`datahub-hynix/hms-hms-minio-spark-hive-metastore:9083`) 에 등록되어 있는 Iceberg 테이블이 Polaris 의 federated catalog 로 노출되고, 클라이언트가 Iceberg REST API 를 통해 그 테이블을 list/read 할 수 있는지 확인한다.

**테스트 경로**: 로컬 quarkusRun (smoke) → Helm 배포 (운영 동치) 2단계.

## 환경 사실 (확인 완료)

| 항목 | 값 |
|---|---|
| HMS 서비스 | `hms-hms-minio-spark-hive-metastore.datahub-hynix.svc.cluster.local:9083` (thrift, **인증 없음 / NONE**) |
| MinIO endpoint | `http://benchmarks-minio.datahub-hynix.svc.cluster.local:9000` |
| 기본 warehouse | `s3://data-catalog-bucket/` (helm `values.yaml:381`) |
| MinIO creds | `minioadmin / minioadmin` (Secret `benchmarks-minio-credentials`) |
| Polaris bootstrap | `POLARIS,root,s3cr3t` (로컬), helm 은 Secret `polaris-persistence-secret` |
| HMS federation 모듈 | `extensions/federation/hive/` (gradle: `polaris-extensions-federation-hive`) |
| 빌드 플래그 | `-PNonRESTCatalogs=HIVE` (없으면 listener jar 가 fat-jar 에 안 들어감 — `runtime/server/build.gradle.kts:50-52`) |

**중요 제약** — `extensions/federation/hive/src/main/java/.../HiveFederatedCatalogFactory.java:51-53` 가 IMPLICIT 외 인증은 명시적으로 거부 (`IllegalStateException`). 다행히 운영 HMS 가 인증 없음이라 해당 없음.

## Phase 0: 사전 준비

1. **`runtime/server/` 의 nested `.git` 문제 정리** (이전 대화에서 발견된 별개 이슈, push 막힘의 원인) — 본 테스트와 직교하지만 빌드/커밋 흐름에 영향 있으면 먼저 해결.
2. **JDK 21 활성화** — `export JAVA_HOME=/opt/homebrew/opt/openjdk@21; export PATH="$JAVA_HOME/bin:$PATH"` (CLAUDE.md 참고).
3. **port-forward 두 개** (로컬 테스트 한정):
   ```bash
   kubectl -n datahub-hynix port-forward svc/hms-hms-minio-spark-hive-metastore 9083:9083 &
   kubectl -n datahub-hynix port-forward svc/benchmarks-minio 9000:9000 &
   ```
4. **`hive-site.xml` 작성** — `~/polaris-hms-test/hive-conf/hive-site.xml` 같은 임시 경로에:
   ```xml
   <?xml version="1.0"?>
   <configuration>
     <property><name>hive.metastore.uris</name><value>thrift://localhost:9083</value></property>
     <property><name>hive.metastore.sasl.enabled</name><value>false</value></property>
     <!-- MinIO S3A 설정 — Polaris JVM 이 metadata.json 을 읽기 위해 필요 -->
     <property><name>fs.s3a.endpoint</name><value>http://localhost:9000</value></property>
     <property><name>fs.s3a.access.key</name><value>minioadmin</value></property>
     <property><name>fs.s3a.secret.key</name><value>minioadmin</value></property>
     <property><name>fs.s3a.path.style.access</name><value>true</value></property>
     <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
   </configuration>
   ```

## Phase A: 로컬 quarkusRun smoke test

### A-1. HIVE 모듈 포함하여 빌드

```bash
./gradlew :polaris-server:quarkusBuild -PNonRESTCatalogs=HIVE
# fast-jar 확인 — hive federation jar 존재 여부
ls runtime/server/build/quarkus-app/lib/main/ | grep -E 'federation-hive|iceberg-hive'
# expected: polaris-extensions-federation-hive-*.jar, iceberg-hive-metastore-*.jar, hive-metastore-*.jar
```

빠진 jar 가 있으면 build 가 HIVE 플래그를 안 받은 것 — `runtime/server/build.gradle.kts:50` 조건문 확인.

### A-2. Feature flag 와 함께 quarkusRun 기동

`runtime/defaults/src/main/resources/application.properties:117` 의 default 는 `["ICEBERG_REST"]` 뿐이라 HIVE 가 비활성. 파일 수정 대신 **system properties / env vars 로 오버라이드**:

```bash
HIVE_CONF_DIR=~/polaris-hms-test/hive-conf \
HADOOP_CONF_DIR=~/polaris-hms-test/hive-conf \
./gradlew :polaris-server:quarkusRun -PNonRESTCatalogs=HIVE \
  -Dpolaris.features.SUPPORTED_CATALOG_CONNECTION_TYPES='["ICEBERG_REST","HIVE"]' \
  -Dpolaris.features.SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES='["OAUTH","IMPLICIT"]' \
  -Dpolaris.features.ENABLE_CATALOG_FEDERATION=true
```

기동 후 `http://localhost:8181` 가 listen, 시드 자격증명은 `POLARIS / root / s3cr3t` (realm POLARIS).

### A-3. Bearer token 발급

```bash
TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials' -d 'client_id=root' -d 'client_secret=s3cr3t' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
echo $TOKEN
```

### A-4. HMS federated catalog 생성

```bash
curl -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "type": "EXTERNAL",
      "name": "hms_test",
      "properties": { "default-base-location": "s3://data-catalog-bucket/hms_test/" },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://data-catalog-bucket/"],
        "endpoint": "http://localhost:9000",
        "pathStyleAccess": true,
        "region": "us-east-1"
      },
      "connectionConfigInfo": {
        "connectionType": "HIVE",
        "uri": "thrift://localhost:9083",
        "warehouse": "s3://data-catalog-bucket/",
        "authenticationParameters": { "authenticationType": "IMPLICIT" }
      }
    }
  }'
```

응답 201 / 200 이면 성공.

### A-5. Federation 동작 검증

```bash
# 1) catalog 등록 확인
curl -s http://localhost:8181/api/management/v1/catalogs/hms_test -H "Authorization: Bearer $TOKEN" | jq

# 2) namespace 목록 — HMS 의 database 들이 보여야 함
curl -s http://localhost:8181/api/catalog/v1/hms_test/namespaces -H "Authorization: Bearer $TOKEN" | jq

# 3) 특정 namespace 의 table 목록 — HMS 에 등록된 Iceberg 테이블만 노출됨 (Hive 일반 테이블은 안 나옴)
curl -s "http://localhost:8181/api/catalog/v1/hms_test/namespaces/<ns>/tables" -H "Authorization: Bearer $TOKEN" | jq

# 4) 테이블 metadata 로드 — metadata.json 이 MinIO 에서 정상 fetch 되는지
curl -s "http://localhost:8181/api/catalog/v1/hms_test/namespaces/<ns>/tables/<table>" -H "Authorization: Bearer $TOKEN" | jq .metadata
```

성공 기준: 4) 가 200 + Iceberg metadata JSON 반환.
실패 시 흔한 원인:
- `ClassNotFoundException: o.a.i.hive.HiveCatalog` → 빌드 플래그 누락 (A-1)
- `Connection refused thrift://localhost:9083` → port-forward 끊김
- `AccessDenied s3://...` → hive-site.xml 의 fs.s3a.* 누락 또는 endpoint 오타
- `Hive federation only supports IMPLICIT authentication` → 요청 본문의 authenticationType 확인

## Phase B: Helm 배포 테스트

Phase A 가 통과한 뒤 진행.

### B-1. 이미지 재빌드

```bash
# HIVE 포함 컨테이너 이미지 생성
./gradlew :polaris-server:imageBuild -PNonRESTCatalogs=HIVE \
  -Dquarkus.container-image.tag=1.3.0-incubating-hive
# 클러스터 registry 로 push (운영 절차에 맞춰)
```

### B-2. hive-site.xml 을 ConfigMap 으로 마운트

새 manifest (helm chart 외부 또는 chart 의 `extraVolumes` 활용):

```yaml
# kubectl apply 로 별도 적용
apiVersion: v1
kind: ConfigMap
metadata:
  name: polaris-hive-conf
  namespace: datahub-hynix
data:
  hive-site.xml: |
    <?xml version="1.0"?>
    <configuration>
      <property><name>hive.metastore.uris</name><value>thrift://hms-hms-minio-spark-hive-metastore.datahub-hynix.svc.cluster.local:9083</value></property>
      <property><name>hive.metastore.sasl.enabled</name><value>false</value></property>
      <property><name>fs.s3a.endpoint</name><value>http://benchmarks-minio.datahub-hynix.svc.cluster.local:9000</value></property>
      <property><name>fs.s3a.access.key</name><value>minioadmin</value></property>
      <property><name>fs.s3a.secret.key</name><value>minioadmin</value></property>
      <property><name>fs.s3a.path.style.access</name><value>true</value></property>
      <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
    </configuration>
```

### B-3. `helm/benchmarks-polaris/values.yaml` 패치

**핵심 원칙** (CLAUDE.md 컨벤션 #5 와 동일) — feature flag 는 `extraEnv` 에 env vars 로:

```yaml
# values.yaml 의 extraEnv 섹션에 추가
extraEnv:
  # ... 기존 env vars 유지 ...
  - name: POLARIS_FEATURES__ENABLE_CATALOG_FEDERATION
    value: "true"
  - name: POLARIS_FEATURES__SUPPORTED_CATALOG_CONNECTION_TYPES
    value: '["ICEBERG_REST","HIVE"]'
  - name: POLARIS_FEATURES__SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES
    value: '["OAUTH","IMPLICIT"]'
  - name: HIVE_CONF_DIR
    value: /etc/polaris/hive-conf
  - name: HADOOP_CONF_DIR
    value: /etc/polaris/hive-conf

extraVolumes:
  - name: hive-conf
    configMap:
      name: polaris-hive-conf

extraVolumeMounts:
  - name: hive-conf
    mountPath: /etc/polaris/hive-conf
    readOnly: true

image:
  tag: 1.3.0-incubating-hive  # B-1 에서 만든 이미지
```

### B-4. 재배포

```bash
helm upgrade benchmarks-polaris \
  /Users/1113435/ai-data-platform/polaris-1.3-git/helm/benchmarks-polaris/ \
  -n datahub-hynix --atomic --timeout 5m
```

### B-5. 클러스터 내 검증

```bash
# 로그에서 feature flag 활성 확인
kubectl -n datahub-hynix logs -l app.kubernetes.io/name=polaris | grep -iE 'connection.types|catalog.federation'

# in-cluster 에서 catalog 생성 + list (테스트용 curl pod)
kubectl -n datahub-hynix run -it --rm curl-test --image=curlimages/curl --restart=Never -- sh
# pod 안에서 A-3/A-4/A-5 를 service DNS 로 재실행
# Polaris service name: benchmarks-polaris.datahub-hynix.svc.cluster.local:8181
```

## 변경되는 파일 (최종)

- (조건부) `runtime/server/.git/` — Phase 0 에서 제거 시
- `helm/benchmarks-polaris/values.yaml` — B-3 의 extraEnv / extraVolumes 패치
- 신규 manifest: `polaris-hive-conf` ConfigMap (chart 외부 적용 권장 — secret 이 아니라서 별도 관리 무방)

**수정하지 말 것** — `runtime/defaults/src/main/resources/application.properties`. 기본값을 바꾸는 대신 system properties / env vars 로 오버라이드 (A-2, B-3). 1.3.x 포인트 업그레이드 시 머지 충돌 회피.

## 회귀 / 안전 체크

- DataHub listener 동작 영향 — federation catalog 의 이벤트도 동일 listener 가 받음. 기존 49 테스트 그대로 통과해야 함:
  ```bash
  ./gradlew :polaris-extensions-datahub-listener:test
  ```
- HIVE 플래그 없이 빌드된 기존 이미지가 깨지지 않는지 — B-1 의 새 이미지는 별도 tag 로 push (`1.3.0-incubating-hive`) 해서 롤백 가능하게.

## 결과 보고 형식

각 단계 완료 시: ✅/❌ + 출력 발췌 + (실패 시) 원인. Phase A 통과 후 사용자 confirm 받고 Phase B 진행.

---

## 실행 시도 결과 (2026-05-21)

### 진행 완료
- ✅ Phase 0: JDK 21.0.10 확인, kubectl 컨텍스트 `docker-desktop`, HMS/MinIO 서비스 가동중
- ✅ Phase 0-3: port-forward 기존 가동 확인 — HMS `localhost:9083`, MinIO **`localhost:19000`** (9000 아님 — chart 가 19000 으로 매핑)
- ✅ Phase 0-4: `~/polaris-hms-test/hive-conf/hive-site.xml` 작성 (MinIO endpoint `localhost:19000` 으로 수정)
- ✅ 환경 sanity: HMS thrift 9083 listen 확인, MinIO HTTP 19000 응답 (HTTP 403, 인증 안 거치면 정상)

### 발견된 환경 차이 (계획 대비)
- MinIO 로컬 포트는 **19000** (계획에 9000 으로 적혀 있던 부분 수정 필요 — Phase A-2/A-4 의 endpoint 인자도 `http://localhost:19000` 으로 정정)
- 포트 8181 은 helm 배포된 Polaris (`benchmarks-polaris`) port-forward 가 점유 중 → 로컬 quarkusRun 은 **`-Dquarkus.http.port=8282`** 등 다른 포트 사용 필요. A-3/A-4/A-5 의 curl URL 도 그에 맞춰 조정.

### 차단된 단계 — Phase A-1 빌드 실패
```
./gradlew :polaris-server:quarkusBuild -PNonRESTCatalogs=HIVE
→ BUILD FAILED in 1m 18s
  Task :polaris-server:quarkusAppPartsBuild FAILED
  [error]: Build step ResteasyReactiveProcessor#setupEndpoints threw java.lang.IllegalArgumentException
    at org.objectweb.asm.ClassReader.<init>(Unknown Source)
    at FilterClassIntrospector.usesGetResourceMethod(FilterClassIntrospector.java:31)
    at ResteasyReactiveProcessor$7.visitPreMatchRequestFilter(...)
```
- `:polaris-extensions-federation-hive:jar` 까지는 성공 (extension 자체 컴파일/패키징 OK)
- Quarkus augmentation 단계에서 ASM `ClassReader` 가 어떤 클래스 파일을 거부 (예외 메시지 없음)
- 기본 빌드 (HIVE 플래그 없이) 는 통과 → HIVE 추가만의 transitive 의존성 이슈

### 진단 진행 상황 (codex:rescue 4시간+ 가동, 종료 권장)
- **환경 사실**: Quarkus 3.29.4, ASM 9.9 — 버전 호환 문제 아님 (ASM 9.9 는 Java 25 까지 지원)
- **유력 가설**: Hive/Hadoop transitive 에 legacy `asm:asm` artifact (구버전 3.x) 가 포함되어 modern `org.ow2.asm:asm` 와 클래스명 충돌 가능성. `extensions/federation/hive/build.gradle.kts` 에서 미제외.
- 단, Codex 가 sandbox 환경에서 Gradle 실행에 막혀 dependency tree 수집에 4시간 이상 소비 후 finalize 못함 → 작업 cancel 권장 (`/codex:cancel task-mpf70krb-tjpu0m`)

### 다음 시도 시 우선 시도할 fix 후보
`extensions/federation/hive/build.gradle.kts` 의 `hive.metastore` block 에 jersey + legacy asm 제외 추가:
```kotlin
implementation(libs.hive.metastore) {
  exclude("org.slf4j", "slf4j-reload4j")
  exclude("org.slf4j", "slf4j-log4j12")
  exclude("ch.qos.reload4j", "reload4j")
  exclude("log4j", "log4j")
  exclude("org.apache.zookeeper", "zookeeper")
  // 추가 제외 후보 — Quarkus ASM 충돌 우회용
  exclude("asm", "asm")
  exclude("com.sun.jersey", "jersey-core")
  exclude("com.sun.jersey", "jersey-server")
  exclude("com.sun.jersey", "jersey-servlet")
  exclude("com.sun.jersey", "jersey-json")
  exclude("com.sun.jersey.contribs", "jersey-guice")
  exclude("org.glassfish.jersey.core", "jersey-server")
  exclude("org.glassfish.jersey.containers", "jersey-container-servlet")
}
```
검증 순서: 위 패치 → `./gradlew :polaris-server:quarkusBuild -PNonRESTCatalogs=HIVE` → 성공 시 fast-jar 의 `lib/main/` 에 federation-hive / iceberg-hive / hive-metastore jar 존재 확인 → A-2 이어서 진행.

### 회귀 확인 필요
패치 후 다음 둘 다 통과해야 함:
1. HIVE 없는 기본 빌드: `./gradlew :polaris-server:quarkusBuild`
2. DataHub listener: `./gradlew :polaris-extensions-datahub-listener:test` (49 tests)

---

## 최종 실행 결과 (2026-05-22 KST)

### ✅ Phase A (local quarkusRun) 와 Phase B (helm 배포) 모두 성공

`sim_00329` 테이블 metadata 로드 (HTTP 200) — 완전한 Iceberg metadata (schema, snapshots, manifest-list) 반환 확인. 양쪽 환경 동일.

### 빌드/배포 시 실제로 발견한 함정과 fix

1. **transitive 의존성 폭탄** — `hive-metastore:4.1.0` 이 `hive-standalone-metastore:3.1.3` → HBase 2.0.0-alpha4 → Tephra → Twill → `org.ow2.asm:asm-all:5.0.2` (legacy ASM) 까지 끌어와 Quarkus `FilterClassIntrospector` 가 `IllegalArgumentException` (메시지 없음) 으로 부팅 실패. `extensions/federation/hive/build.gradle.kts` 의 `libs.hive.metastore` block 에 다음 제외 추가:
   - hive: hive-exec, hive-upgrade-acid
   - hbase: hbase-client/common/hadoop-compat/hadoop2-compat/metrics/metrics-api/protocol/protocol-shaded/server
   - tephra: tephra-api/core/hbase-compat-1.0
   - twill: twill-api/common/core/discovery-api/discovery-core/zookeeper
   - asm: org.ow2.asm:asm-all
   - datanucleus: datanucleus-api-jdo/core/rdbms/javax.jdo
   - log4j (버전 충돌): log4j-core/slf4j-impl/1.2-api/web (`log4j-api` 만 2.25.2 로 유지)
   - **주의**: `hive-standalone-metastore` 자체는 *포함* (Thrift API 클래스 — `NoSuchObjectException` 등 — 이 들어있어서 빠뜨리면 런타임 `ClassNotFoundException`). 위 exclusion 들이 graph cascade 로 standalone-metastore 의 toxic transitive 만 자름.

2. **S3AFileSystem 누락** — HMS warehouse 가 `s3://...` 이면 Iceberg HiveCatalog 가 metadata.json 을 Hadoop `FileSystem.get(s3a://...)` 으로 읽음. `hadoop-aws:3.4.2` 와 `s3-transfer-manager` 명시 추가:
   ```kotlin
   implementation("org.apache.hadoop:hadoop-aws:3.4.2") {
     exclude("org.slf4j", "slf4j-reload4j"); ...
     exclude("software.amazon.awssdk", "bundle")  // 구 all-in-one bundle:2.29.52 가
                                                   // BusinessMetricFeatureId.CREDENTIALS_ENV_VARS 누락 →
                                                   // EnvironmentVariableCredentialsProvider NoSuchFieldError
   }
   implementation("software.amazon.awssdk:s3-transfer-manager:2.39.2")
   ```

3. **Quarkus 빌드 OOM** — AWS SDK v2 의 ~260 jars (~840MB) 를 Jandex 가 인덱싱하다 default 4G heap 초과. `runtime/server/build.gradle.kts` 에 추가:
   ```kotlin
   quarkus { buildForkOptions { maxHeapSize = "12g" } }
   ```
   (`org.gradle.jvmargs` 만 키워도 안 됨 — Quarkus 의 build worker 는 별도 fork 라 `buildForkOptions` 가 정답.)

4. **Hadoop Configuration 이 `core-site.xml` 을 classpath 에서만 찾음** — `HADOOP_CONF_DIR` env var 의 파일시스템 경로는 Quarkus fast-jar 의 `RunnerClassLoader` 가 자동으로 잡지 않음. 해결: `HiveFederatedCatalogFactory` 가 직접 로드:
   ```java
   Configuration conf = new Configuration();
   String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
   if (hadoopConfDir != null) {
     for (File xml : new File(hadoopConfDir).listFiles((d, n) -> n.endsWith("-site.xml"))) {
       conf.addResource(new Path(xml.getAbsolutePath()));
     }
   }
   hiveCatalog.setConf(conf);
   ```
   로컬 (`HADOOP_CONF_DIR=~/polaris-hms-test/hive-conf`) 과 K8s (`HADOOP_CONF_DIR=/etc/polaris/hadoop-conf` + ConfigMap 마운트) 가 동일 메커니즘으로 동작.

5. **`SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` 은 절대 전역으로 켜지 말 것 (회귀 주의)** — 초기엔
   federation loadTable 의 "Failed to get subscoped credentials" 를 우회하려고 helm `features:`
   블록에 `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` 를 넣었으나, 이 키는 **realm 단위 전역**
   설정이라 federation 뿐 아니라 *모든* 카탈로그에 적용된다. 켜지면 `StorageAccessConfigProvider`
   (`runtime/service/.../catalog/io/StorageAccessConfigProvider.java:98-104`) 가 모든 테이블
   로드에서 **빈 `StorageAccessConfig`** 를 반환하고, `IcebergCatalogHandler.java:834` 가 그
   `extraProperties()` (= `s3.endpoint`, `s3.path-style-access`, `s3.region`) 를 비운 채
   응답/FileIO 설정에 넣는다. 결과적으로 S3FileIO 가 `s3.path-style-access` 를 못 받아
   virtual-host 주소(`<버킷>.<엔드포인트>`)를 쓰고, MinIO 를 쓰는 **내부 카탈로그 전체**가
   `UnknownHostException` 으로 깨졌다. → helm 차트에서 이 키를 **주입하지 않는다**.
   federation 의 "subscoped credentials" 에러는 외부(EXTERNAL) 카탈로그가
   `ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING` 가 꺼져 있으면 credential vending 블록
   (`IcebergCatalogHandler.java:798`) 자체를 건너뛰므로, 플래그 없이도 HMS federation loadTable
   은 정상 동작한다 (helm pod 는 S3A metadata 읽기용 자격증명을 env var 로 이미 보유).

6. **`polaris.features.*` 는 application.properties 에 적어야 함** — env var (`POLARIS_FEATURES__*`) 는 SmallRye Config 의 `@WithParentName Map<String,String>` 이 대소문자/underscore 키를 보존 못 해서 적용 안 됨. helm 차트의 `features:` 블록을 통해 application.properties 로 baked.

7. **helm 차트의 `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY` 는 이미 Secret 에서 inject 됨** — `extraEnv` 에 중복 정의하면 strategic merge 충돌. CLAUDE.md 컨벤션과 일치 (`benchmarks-minio-credentials` Secret 사용).

8. **helm release 의 user-supplied values 가 chart values 를 override** — 이전 `--set image.tag=...` 가 release storage 에 남아있어서 chart 의 새 `image.tag` 가 무시됨. `helm upgrade --set image.tag=v1.3.0-hive-test-20260521 --set image.pullPolicy=Never` 로 명시 override.

### 변경된 파일 (커밋 대상)
- `extensions/federation/hive/build.gradle.kts` — 의존성 정리 (Fix #1, #2)
- `extensions/federation/hive/src/main/java/org/apache/polaris/extensions/federation/hive/HiveFederatedCatalogFactory.java` — HADOOP_CONF_DIR 로딩 (Fix #4)
- `runtime/server/build.gradle.kts` — `buildForkOptions { maxHeapSize = "12g" }` (Fix #3)
- `helm/benchmarks-polaris/values.yaml` — `hiveFederation` 블록 (enabled flag + metastoreUri + s3.endpoint)
- `helm/benchmarks-polaris/templates/hive-federation-configmap.yaml` — (신규) hive-site.xml + core-site.xml ConfigMap, `hiveFederation.enabled` 게이트
- `helm/benchmarks-polaris/templates/configmap.yaml` — `hiveFederation.enabled` 시 federation feature 키 3개 (ENABLE_CATALOG_FEDERATION, SUPPORTED_CATALOG_CONNECTION_TYPES, SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES) 머지. `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` 은 주입하지 않음 (Fix #5 회귀)
- `helm/benchmarks-polaris/templates/deployment.yaml` — `hiveFederation.enabled` 시 volume/volumeMount/env 주입

### Helm 차트 설계 (chart-native, 외부 manifest 불필요)
- `hiveFederation.enabled` (default `true`) 하나로 federation 전체를 토글. 켜면 차트가:
  1. `{release}-hive-conf` ConfigMap 생성 (`hive-site.xml` + `core-site.xml`)
  2. `/etc/polaris/hadoop-conf` 에 마운트 + `HADOOP_CONF_DIR` env
  3. federation feature 키 3개를 application.properties 에 머지 (ENABLE_CATALOG_FEDERATION,
     SUPPORTED_CATALOG_CONNECTION_TYPES 에 HIVE, SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES 에 IMPLICIT)
  4. `POLARIS_READINESS_IGNORE_SEVERE_ISSUES` / `QUARKUS_CONFIG_MAPPING_VALIDATE_UNKNOWN` env
  - **주입하지 않는 것**: `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` — 전역 켜면 내부 카탈로그가
    깨진다 (Fix #5 참조).
- 끄면(`enabled: false`) **어떤 federation 리소스도 만들지 않음** → HMS 없는 환경에서도 `helm install` 정상.
- **자격증명은 ConfigMap 에 두지 않음** — 차트가 이미 `benchmarks-minio-credentials` Secret 에서 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` 를 주입하므로 S3AFileSystem 의 `EnvironmentVariableCredentialsProvider` 가 사용. core-site.xml 에는 endpoint/path-style 만.
- `SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES` 는 Polaris 기본값 `[OAUTH, BEARER, SIGV4]` 을 보존한 채 `IMPLICIT` 만 추가 — 기본값을 통째로 덮으면 BEARER/SIGV4 쓰는 ICEBERG_REST federated catalog 가 거부됨.

### 운영 체크리스트
- [ ] 이미지 빌드: `./gradlew :polaris-server:quarkusBuild -PNonRESTCatalogs=HIVE` → `docker buildx build` → push to skthynix/polaris:HIVE-TAG
- [ ] `helm/benchmarks-polaris/values.yaml` 의 `hiveFederation.metastoreUri` / `s3.endpoint` 가 운영 cluster DNS 와 맞는지 확인
- [ ] `helm upgrade --set image.tag=<HIVE-TAG> --set image.pullPolicy=<...>` (HIVE 모듈 포함 이미지 필수)
- [ ] 통신 검증: `curl -H "Polaris-Realm: POLARIS" -H "Authorization: Bearer ..." http://polaris/api/catalog/v1/hms_test/namespaces`
- [ ] HMS 없는 환경에 배포할 땐 `--set hiveFederation.enabled=false`

### 회귀 영향 (변경 후 재검증)
- ✅ HIVE 없는 기본 빌드 (`./gradlew :polaris-server:quarkusBuild`): 통과
- ✅ DataHub listener 테스트 (`./gradlew :polaris-extensions-datahub-listener:test`): 82 tests 모두 통과 (49 → 82 로 늘었으나 그대로 통과)

### 알려진 제약 (Known limitation)
- **S3A assumed-role 경로 미지원** — `extensions/federation/hive/build.gradle.kts` 가 `hadoop-aws` 에서
  `software.amazon.awssdk:bundle` (구 2.29.52, `BusinessMetricFeatureId.CREDENTIALS_ENV_VARS` 누락)
  을 제외한다. 그 결과 S3A 를 `fs.s3a.assumed.role.*` 로 구성해 custom STS endpoint 로 role 을
  assume 하는 경우 `hadoop-aws` 의 `STSClientFactory` 가 참조하는 shaded 클래스
  (`software.amazon.awssdk.thirdparty...URIBuilder`) 가 없어 `NoClassDefFoundError` 가 난다.
  현재 federation 의 S3A metadata 읽기는 env-var 자격증명
  (`EnvironmentVariableCredentialsProvider` — helm 차트가 Secret 에서 주입) 만 쓰고 assumed-role
  은 안 쓰므로 이 경로를 타지 않아 영향 없음.
  S3A assumed-role 이 필요해지면 `bundle` 을 제외하는 대신 `2.39.2` 로 force 할 것
  (이미지 +~500MB. 2.39.2 bundle 은 `CREDENTIALS_ENV_VARS` 를 포함하므로 원래 NoSuchFieldError 도 해소).

