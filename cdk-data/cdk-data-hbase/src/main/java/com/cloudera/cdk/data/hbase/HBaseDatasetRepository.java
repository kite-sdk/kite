package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.hbase.avro.GenericAvroDao;
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
    metadataProvider.create(name, descriptor);
    // TODO: use descriptor.getFormat() to decide type of DAO (Avro vs. other)
    GenericAvroDao dao = new GenericAvroDao(tablePool, name, HBaseMetadataProvider.ENTITY_NAME, schemaManager);
    return new GenericAvroDaoDataset(dao, descriptor);
  }

  @Override
  public Dataset update(String name, DatasetDescriptor descriptor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Dataset load(String name) {
    DatasetDescriptor descriptor = metadataProvider.load(name);
    GenericAvroDao dao = new GenericAvroDao(tablePool, name, HBaseMetadataProvider.ENTITY_NAME, schemaManager);
    return new GenericAvroDaoDataset(dao, descriptor);
  }

  @Override
  public boolean delete(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<String> list() {
    throw new UnsupportedOperationException();
  }
}
