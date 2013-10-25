/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.hbase.avro.GenericAvroDao;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.manager.DefaultSchemaManager;
import com.cloudera.cdk.data.spi.AbstractDatasetRepository;
import com.google.common.base.Supplier;
import java.util.Collection;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

public class HBaseDatasetRepository extends AbstractDatasetRepository {

  private HTablePool tablePool;
  private SchemaManager schemaManager;
  private HBaseMetadataProvider metadataProvider;

  HBaseDatasetRepository(HBaseAdmin hBaseAdmin, HTablePool tablePool) {
    this.tablePool = tablePool;
    this.schemaManager = new DefaultSchemaManager(tablePool);
    this.metadataProvider = new HBaseMetadataProvider(hBaseAdmin, schemaManager);
  }

  @Override
  public <E> Dataset<E> create(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor newDescriptor = metadataProvider.create(name, descriptor);
    return newDataset(name, newDescriptor);
  }

  @Override
  public <E> Dataset<E> update(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor newDescriptor = metadataProvider.update(name, descriptor);
    return newDataset(name, newDescriptor);
  }

  @Override
  public <E> Dataset<E> load(String name) {
    DatasetDescriptor descriptor = metadataProvider.load(name);
    return newDataset(name, descriptor);
  }

  @SuppressWarnings("unchecked")
  private <E> Dataset<E> newDataset(String name, DatasetDescriptor descriptor) {
    // TODO: use descriptor.getFormat() to decide type of DAO (Avro vs. other)
    String tableName = HBaseMetadataProvider.getTableName(name);
    String entityName = HBaseMetadataProvider.getEntityName(name);
    Dao dao;
    if (isComposite(descriptor)) {
      try {
        Class<SpecificRecord> entityClass = (Class<SpecificRecord>)
            Class.forName(descriptor.getSchema().getFullName());
        dao = SpecificAvroDao.buildCompositeDaoWithEntityManager(tablePool, tableName,
                entityClass, schemaManager);
      } catch (ClassNotFoundException e) {
        throw new DatasetRepositoryException(e);
      }
    } else if (isSpecific(descriptor)) {
      dao = new SpecificAvroDao(tablePool, tableName, entityName, schemaManager);
    } else {
      dao = new GenericAvroDao(tablePool, tableName, entityName, schemaManager);
    }
    return new DaoDataset(name, dao, descriptor);
  }

  private static boolean isSpecific(DatasetDescriptor descriptor) {
    try {
      Class.forName(descriptor.getSchema().getFullName());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static boolean isComposite(DatasetDescriptor descriptor) {
    // TODO: have a better way of detecting a composite class (currently just checks if
    // there are field mappings)
    String entitySchema = descriptor.getSchema().toString();
    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    return parser.parseEntitySchema(entitySchema).getFieldMappings().isEmpty();
  }

  @Override
  public boolean delete(String name) {
    return metadataProvider.delete(name);
  }

  @Override
  public boolean exists(String name) {
    return metadataProvider.exists(name);
  }

  @Override
  public Collection<String> list() {
    throw new UnsupportedOperationException();
  }

  public static class Builder implements Supplier<HBaseDatasetRepository> {

    private Configuration configuration;

    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    @Override
    public HBaseDatasetRepository get() {
      HTablePool pool = new HTablePool(configuration, 10);
      HBaseAdmin admin;
      try {
        admin = new HBaseAdmin(configuration);
      } catch (MasterNotRunningException e) {
        throw new DatasetRepositoryException("Problem creating HBaseDatasetRepository.", e);
      } catch (ZooKeeperConnectionException e) {
        throw new DatasetRepositoryException("Problem creating HBaseDatasetRepository.", e);
      }
      return new HBaseDatasetRepository(admin, pool);
    }
  }
}
