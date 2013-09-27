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
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

public class HBaseDatasetRepository extends AbstractDatasetRepository {

  private HTablePool tablePool;
  private SchemaManager schemaManager;
  private HBaseMetadataProvider metadataProvider;

  public HBaseDatasetRepository(HBaseAdmin hBaseAdmin, HTablePool tablePool) {
    this.tablePool = tablePool;
    this.schemaManager = new DefaultSchemaManager(tablePool);
    this.metadataProvider = new HBaseMetadataProvider(hBaseAdmin, schemaManager);
  }

  @Override
  public Dataset create(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor newDescriptor = metadataProvider.create(name, descriptor);
    return newDataset(name, newDescriptor);
  }

  @Override
  public Dataset update(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor newDescriptor = metadataProvider.update(name, descriptor);
    return newDataset(name, newDescriptor);
  }

  @Override
  public Dataset load(String name) {
    DatasetDescriptor descriptor = metadataProvider.load(name);
    return newDataset(name, descriptor);
  }

  private Dataset newDataset(String name,
      DatasetDescriptor descriptor) {
    // TODO: use descriptor.getFormat() to decide type of DAO (Avro vs. other)
    String tableName = HBaseMetadataProvider.getTableName(name);
    String entityName = HBaseMetadataProvider.getEntityName(name);
    if (isComposite(descriptor)) {
      try {
        Class<SpecificRecord> entityClass = (Class<SpecificRecord>)
            Class.forName(descriptor.getSchema().getFullName());
        Dao<SpecificRecord> dao =
            SpecificAvroDao.buildCompositeDaoWithEntityManager(tablePool, tableName,
                entityClass, schemaManager);
        return new CompositeAvroDaoDataset(dao, descriptor);
      } catch (ClassNotFoundException e) {
        throw new DatasetRepositoryException(e);
      }
    } else if (isSpecific(descriptor)) {
      SpecificAvroDao dao = new SpecificAvroDao(tablePool, tableName, entityName, schemaManager);
      return new SpecificAvroDaoDataset(dao, descriptor);
    } else {
      GenericAvroDao dao = new GenericAvroDao(tablePool, tableName, entityName, schemaManager);
      return new GenericAvroDaoDataset(dao, descriptor);
    }
  }

  private static boolean isSpecific(DatasetDescriptor descriptor) {
    try {
      Class.forName(descriptor.getSchema().getFullName());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  static boolean isComposite(DatasetDescriptor descriptor) {
    // TODO: have a better way of detecting a specific class (currently just checks if
    // there are field mappings)
    String entitySchema = descriptor.getSchema().toString();
    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    return parser.parseEntitySchema(entitySchema).getFieldMappings().isEmpty();
  }

  @Override
  public boolean delete(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(String name) {
    return metadataProvider.exists(name);
  }

  @Override
  public Collection<String> list() {
    throw new UnsupportedOperationException();
  }
}
