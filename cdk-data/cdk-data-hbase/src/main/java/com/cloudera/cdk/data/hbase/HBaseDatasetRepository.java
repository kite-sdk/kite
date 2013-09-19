package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.hbase.avro.GenericAvroDao;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.hbase.manager.DefaultSchemaManager;
import com.cloudera.cdk.data.spi.AbstractDatasetRepository;
import java.util.Collection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

public class HBaseDatasetRepository extends AbstractDatasetRepository {

  private HTablePool tablePool;
  private SchemaManager schemaManager;
  private HBaseMetadataProvider metadataProvider;

  public HBaseDatasetRepository(HBaseAdmin hBaseAdmin, HTablePool tablePool) {
    this.tablePool = tablePool;
    this.schemaManager = new DefaultSchemaManager(tablePool);
    this.metadataProvider = new HBaseMetadataProvider(hBaseAdmin, tablePool);
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

  private Dataset newDataset(String name, DatasetDescriptor descriptor) {
    // TODO: use descriptor.getFormat() to decide type of DAO (Avro vs. other)
    String entityName = HBaseMetadataProvider.getEntityName(descriptor);
    if (isSpecific(descriptor)) {
      SpecificAvroDao dao = new SpecificAvroDao(tablePool, name, entityName, schemaManager);
      return new SpecificAvroDaoDataset(dao, descriptor);
    } else {
      GenericAvroDao dao = new GenericAvroDao(tablePool, name, entityName, schemaManager);
      return new GenericAvroDaoDataset(dao, descriptor);
    }
  }

  private boolean isSpecific(DatasetDescriptor descriptor) {
    try {
      Class.forName(descriptor.getSchema().getFullName());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
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
