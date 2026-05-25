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

plugins {
  id("polaris-client")
  id("org.kordamp.gradle.jandex")
}

dependencies {
  // Polaris dependencies
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-common")
  // Use iceberg-hive-metastore but exclude conflicting hive dependencies
  implementation("org.apache.iceberg:iceberg-hive-metastore") { exclude(group = "org.apache.hive") }
  // Add our own Hive 4.1.0 dependencies
  implementation(libs.hive.metastore) {
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    // HMS server-side bloat — Polaris uses HMS as a CLIENT but needs Thrift API classes
    // (NoSuchObjectException, etc.) from hive-standalone-metastore, so we keep
    // hive-standalone-metastore itself. The toxic transitives (HBase 2.0.0-alpha4 + Tephra +
    // Twill + asm-all:5.0.2 legacy ASM that breaks Quarkus's FilterClassIntrospector) are
    // excluded at the hive-metastore level — gradle cascades exclusions through the entire
    // dependency subtree, so we don't need to repeat them on hive-standalone-metastore.
    exclude("org.apache.hive", "hive-exec")
    exclude("org.apache.hive", "hive-upgrade-acid")
    exclude("org.apache.hbase", "hbase-client")
    exclude("org.apache.hbase", "hbase-common")
    exclude("org.apache.hbase", "hbase-hadoop-compat")
    exclude("org.apache.hbase", "hbase-hadoop2-compat")
    exclude("org.apache.hbase", "hbase-metrics")
    exclude("org.apache.hbase", "hbase-metrics-api")
    exclude("org.apache.hbase", "hbase-protocol")
    exclude("org.apache.hbase", "hbase-protocol-shaded")
    exclude("org.apache.hbase", "hbase-server")
    exclude("co.cask.tephra", "tephra-api")
    exclude("co.cask.tephra", "tephra-core")
    exclude("co.cask.tephra", "tephra-hbase-compat-1.0")
    exclude("org.apache.twill", "twill-api")
    exclude("org.apache.twill", "twill-common")
    exclude("org.apache.twill", "twill-core")
    exclude("org.apache.twill", "twill-discovery-api")
    exclude("org.apache.twill", "twill-discovery-core")
    exclude("org.apache.twill", "twill-zookeeper")
    exclude("org.ow2.asm", "asm-all")
    exclude("org.datanucleus", "datanucleus-api-jdo")
    exclude("org.datanucleus", "datanucleus-core")
    exclude("org.datanucleus", "datanucleus-rdbms")
    exclude("org.datanucleus", "javax.jdo")
    // Hive transitive pins log4j-{core,slf4j-impl,1.2-api,web} at 2.17.1, which is binary-
    // incompatible with log4j-api 2.25.2 (Quarkus pulls newer api but Hive's older core/impl
    // calls LoaderUtil#getClassLoaders() that no longer exists on the new api → NoSuchMethodError
    // on AWS SDK init). Polaris runtime uses slf4j + jboss-logmanager (Quarkus standard) and
    // does not need log4j-core, so drop the old core/impl/web/1.2-api jars. log4j-api itself
    // is pulled at 2.25.2 from another path and is fine.
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.apache.logging.log4j", "log4j-web")
  }

  // Hadoop dependencies
  implementation(libs.hadoop.common) {
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.apache.hadoop.thirdparty", "hadoop-shaded-protobuf_3_25")
    exclude("com.github.pjfanning", "jersey-json")
    exclude("com.sun.jersey", "jersey-core")
    exclude("com.sun.jersey", "jersey-server")
    exclude("com.sun.jersey", "jersey-servlet")
    exclude("io.dropwizard.metrics", "metrics-core")
  }

  // S3AFileSystem — required for Hive metastore federation when warehouse path is s3://...
  // (HiveCatalog uses Hadoop FileSystem to fetch metadata.json from object storage).
  implementation("org.apache.hadoop:hadoop-aws:3.4.2") {
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    // hadoop-aws 3.4.2 pulls software.amazon.awssdk:bundle:2.29.52 (an older all-in-one jar)
    // that shadows individual AWS SDK modules at 2.39.2 from iceberg-bom. The older bundle's
    // BusinessMetricFeatureId lacks CREDENTIALS_ENV_VARS, causing NoSuchFieldError when
    // EnvironmentVariableCredentialsProvider initializes. Drop the bundle; the individual
    // 2.39.2 modules (s3, sts, auth, etc.) on the classpath cover what S3AFileSystem needs.
    exclude("software.amazon.awssdk", "bundle")
  }
  // S3AFileSystem additionally requires the Transfer Manager; this is NOT in the iceberg-bom's
  // default module set. Explicit version (2.39.2) matches the awssdk-bom that polaris-core uses.
  implementation("software.amazon.awssdk:s3-transfer-manager:2.39.2")

  // CDI dependencies for runtime discovery
  implementation(libs.jakarta.enterprise.cdi.api)
  implementation(libs.smallrye.common.annotation)

  // Logging
  implementation(libs.slf4j.api)
}
