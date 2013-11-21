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

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.hbase.avro.GenericAvroDao;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.hbase.manager.DefaultSchemaManager;
import com.cloudera.cdk.data.spi.AbstractDatasetRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
  public <E> RandomAccessDataset<E> create(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor newDescriptor = metadataProvider.create(name, descriptor);
    return newDataset(name, newDescriptor);
  }

  @Override
  public <E> RandomAccessDataset<E> update(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor newDescriptor = metadataProvider.update(name, descriptor);
    return newDataset(name, newDescriptor);
  }

  @Override
  public <E> RandomAccessDataset<E> load(String name) {
    String tableName = HBaseMetadataProvider.getTableName(name);
    String entityName = HBaseMetadataProvider.getEntityName(name);
    if (entityName.contains(".")) {
      List<DatasetDescriptor> descriptors = new ArrayList<DatasetDescriptor>();
      for (String subEntityName : entityName.split("\\.")) {
        descriptors.add(metadataProvider.load(tableName + "." + subEntityName));
      }
      return newCompositeDataset(name, tableName, descriptors);
    } else {
      DatasetDescriptor descriptor = metadataProvider.load(name);
      return newDataset(name, descriptor);
    }
  }

  @SuppressWarnings("unchecked")
  private <E> RandomAccessDataset<E> newCompositeDataset(String name, String tableName,
      List<DatasetDescriptor> descriptors) {
    List<Class<SpecificRecord>> subEntityClasses = new ArrayList<Class<SpecificRecord>>();
    for (DatasetDescriptor descriptor : descriptors) {
      try {
        Class<SpecificRecord> subEntityClass = (Class<SpecificRecord>) Class
            .forName(descriptor.getSchema().getFullName());
        subEntityClasses.add(subEntityClass);
      } catch (ClassNotFoundException e) {
        throw new DatasetRepositoryException(e);
      }
    }
    Dao dao = SpecificAvroDao.buildCompositeDaoWithEntityManager(tablePool,
        tableName, subEntityClasses, schemaManager);
    return new DaoDataset<E>(name, dao, descriptors.get(0));
  }

  @SuppressWarnings("unchecked")
  private <E> RandomAccessDataset<E> newDataset(String name, DatasetDescriptor descriptor) {
    // TODO: use descriptor.getFormat() to decide type of DAO (Avro vs. other)
    String tableName = HBaseMetadataProvider.getTableName(name);
    String entityName = HBaseMetadataProvider.getEntityName(name);
    Dao dao;
    if (isSpecific(descriptor)) {
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

  public static class Builder { // implements Supplier<HBaseDatasetRepository> {

    private Configuration configuration;

    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    public HBaseDatasetRepository build() {
      HTablePool pool = new HTablePool(configuration, 10);
      HBaseAdmin admin;
      try {
        admin = new HBaseAdmin(configuration);
      } catch (MasterNotRunningException e) {
        throw new DatasetRepositoryException(
            "Problem creating HBaseDatasetRepository.", e);
      } catch (ZooKeeperConnectionException e) {
        throw new DatasetRepositoryException(
            "Problem creating HBaseDatasetRepository.", e);
      }
      return new HBaseDatasetRepository(admin, pool);
    }
  }
}
