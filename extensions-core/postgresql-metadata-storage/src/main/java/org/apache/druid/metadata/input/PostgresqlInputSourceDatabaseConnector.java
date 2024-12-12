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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLInputSourceDatabaseConnector;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.utils.ConnectionUriUtils;
import org.skife.jdbi.v2.DBI;

import java.util.Objects;
import java.util.Set;


@JsonTypeName("postgresql")
public class PostgresqlInputSourceDatabaseConnector extends SQLInputSourceDatabaseConnector
{
  private final DBI dbi;
  private final MetadataStorageConnectorConfig connectorConfig;

  @JsonCreator
  public PostgresqlInputSourceDatabaseConnector(
      @JsonProperty("connectorConfig") MetadataStorageConnectorConfig connectorConfig,
      @JacksonInject JdbcAccessSecurityConfig securityConfig
  )
  {
    this.connectorConfig = connectorConfig;
    final BasicDataSource datasource = getDatasource(connectorConfig, securityConfig);
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.postgresql.Driver");
    this.dbi = new DBI(datasource);
  }

  @JsonProperty
  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  @Override
  public Set<String> findPropertyKeysFromConnectURL(String connectUri, boolean allowUnknown)
  {
    return ConnectionUriUtils.tryParseJdbcUriParameters(connectUri, allowUnknown);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PostgresqlInputSourceDatabaseConnector that = (PostgresqlInputSourceDatabaseConnector) o;
    return connectorConfig.equals(that.connectorConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connectorConfig);
  }

  @Override
  public String toString()
  {
    return "PostgresqlInputSourceDatabaseConnector{" +
           "connectorConfig=" + connectorConfig +
           '}';
  }
}
