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
package org.apache.polaris.extensions.federation.hive;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.hive.HiveConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class for creating a Hive catalog handle based on connection configuration. */
@ApplicationScoped
@Identifier(ConnectionType.HIVE_FACTORY_IDENTIFIER)
public class HiveFederatedCatalogFactory implements ExternalCatalogFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveFederatedCatalogFactory.class);

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo,
      PolarisCredentialManager polarisCredentialManager) {
    // Currently, Polaris supports Hive federation only via IMPLICIT authentication.
    // Hence, prior to initializing the configuration, ensure that the catalog uses
    // IMPLICIT authentication.
    AuthenticationParametersDpo authenticationParametersDpo =
        connectionConfigInfoDpo.getAuthenticationParameters();
    if (authenticationParametersDpo.getAuthenticationTypeCode()
        != AuthenticationType.IMPLICIT.getCode()) {
      throw new IllegalStateException("Hive federation only supports IMPLICIT authentication.");
    }
    String warehouse = ((HiveConnectionConfigInfoDpo) connectionConfigInfoDpo).getWarehouse();
    // Hadoop's Configuration only loads core-default.xml and core-site.xml from the JVM classpath
    // (resource lookup, not filesystem). In a Quarkus fast-jar (java -jar quarkus-run.jar),
    // mounting a config dir or pointing HADOOP_CONF_DIR at a filesystem path is not enough — the
    // RunnerClassLoader does not pick those files up. So if HADOOP_CONF_DIR is set, explicitly
    // load every *-site.xml from it as a Configuration resource via Path. This is what Hadoop's
    // command-line launcher does behind the scenes; we replicate it here.
    Configuration conf = new Configuration();
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (hadoopConfDir != null && !hadoopConfDir.isEmpty()) {
      File dir = new File(hadoopConfDir);
      File[] siteXmls = dir.listFiles((d, name) -> name.endsWith("-site.xml"));
      if (siteXmls != null) {
        for (File xml : siteXmls) {
          conf.addResource(new Path(xml.getAbsolutePath()));
          LOGGER.info("HiveFederatedCatalogFactory: loaded {}", xml.getAbsolutePath());
        }
      }
    }

    HiveCatalog hiveCatalog = new HiveCatalog();
    hiveCatalog.setConf(conf);
    hiveCatalog.initialize(
        warehouse, connectionConfigInfoDpo.asIcebergCatalogProperties(polarisCredentialManager));
    return hiveCatalog;
  }

  @Override
  public GenericTableCatalog createGenericCatalog(
      ConnectionConfigInfoDpo connectionConfig, PolarisCredentialManager polarisCredentialManager) {
    // TODO implement
    throw new UnsupportedOperationException(
        "Generic table federation to this catalog is not supported.");
  }
}
