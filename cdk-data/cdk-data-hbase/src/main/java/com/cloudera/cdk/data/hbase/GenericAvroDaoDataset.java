package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.hbase.avro.GenericAvroDao;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GenericAvroDaoDataset implements Dataset {

  private final GenericAvroDao dao;
  private DatasetDescriptor descriptor;
  private Schema keySchema;
  private Schema entitySchema;

  public GenericAvroDaoDataset(GenericAvroDao dao, DatasetDescriptor descriptor) {
    this.dao = dao;
    this.descriptor = descriptor;
    Schema[] keyAndEntitySchemas = HBaseMetadataProvider.getKeyAndEntitySchemas(descriptor);
    this.keySchema = keyAndEntitySchemas[0];
    this.entitySchema = keyAndEntitySchemas[1];
  }

  @Override
  public String getName() {
    return dao.getTableName();
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public Dataset getPartition(PartitionKey key, boolean autoCreate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(PartitionKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <E> DatasetWriter<E> getWriter() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <E> DatasetReader<E> getReader() {
    return new GenericAvroDaoDatasetReader(dao.getScanner());
  }

  @Override
  public Iterable<Dataset> getPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <E> DatasetAccessor<E> newAccessor() {
    return new DatasetAccessor<E>() {
      @Override
      public E get(PartitionKey key) {
        GenericRecord keyRecord = new GenericData.Record(keySchema);
        int i = 0;
        for (FieldPartitioner fp : descriptor.getPartitionStrategy().getFieldPartitioners()) {
          keyRecord.put(fp.getName(), key.get(i++));
        }
        GenericRecord entityRecord = dao.get(keyRecord);
        return merge(keyRecord, entityRecord);
      }

      @Override
      public boolean put(E e) {
        GenericRecord record = (GenericRecord) e;
        // split into key and entity records
        GenericRecord key = new GenericData.Record(keySchema);
        for (Schema.Field f : keySchema.getFields()) {
          key.put(f.name(), record.get(f.name()));
        }
        GenericRecord entity = new GenericData.Record(entitySchema);
        for (Schema.Field f : entitySchema.getFields()) {
          entity.put(f.name(), record.get(f.name()));
        }
        return dao.put(key, entity);
      }
    };
  }

  private <E> E merge(GenericRecord keyRecord, GenericRecord entityRecord) {
    if (entityRecord == null) {
      return null;
    }
    GenericRecord mergedEntity = new GenericData.Record(descriptor.getSchema());
    for (Schema.Field f : keySchema.getFields()) {
      mergedEntity.put(f.name(), keyRecord.get(f.name()));
    }
    for (Schema.Field f : entitySchema.getFields()) {
      mergedEntity.put(f.name(), entityRecord.get(f.name()));
    }
    return (E) mergedEntity;
  }

  private class GenericAvroDaoDatasetReader extends AbstractDatasetReader {

    private EntityScanner<GenericRecord, GenericRecord> scanner;
    private Iterator<KeyEntity<GenericRecord, GenericRecord>> iterator;

    public GenericAvroDaoDatasetReader(EntityScanner<GenericRecord, GenericRecord> scanner) {
      this.scanner = scanner;
    }

    @Override
    public void open() {
      scanner.open();
      iterator = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public GenericRecord next() {
      KeyEntity<GenericRecord, GenericRecord> next = iterator.next();
      return merge(next.getKey(), next.getEntity());
    }

    @Override
    public void close() {
      scanner.close();
    }

    @Override
    public boolean isOpen() {
      return true; // TODO
    }
  }
}
