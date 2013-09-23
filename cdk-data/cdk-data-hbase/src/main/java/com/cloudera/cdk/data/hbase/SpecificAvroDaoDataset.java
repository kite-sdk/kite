package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntityBatch;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

class SpecificAvroDaoDataset implements Dataset {
  private SpecificAvroDao dao;
  private DatasetDescriptor descriptor;
  private Schema keySchema;


  public SpecificAvroDaoDataset(SpecificAvroDao dao, DatasetDescriptor descriptor) {
    this.dao = dao;
    this.descriptor = descriptor;
    this.keySchema = HBaseMetadataProvider.getKeySchema(descriptor);
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
    return new SpecificAvroDaoDatasetWriter(dao.newBatch());
  }

  @Override
  public <E> DatasetReader<E> getReader() {
    return new SpecificAvroDaoDatasetReader(dao.getScanner());
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
        return (E) dao.get(key);
      }

      @Override
      public boolean put(E e) {
        return dao.put(e);
      }

      @Override
      public long increment(PartitionKey key, String fieldName, long amount) {
        return dao.increment(key, fieldName, amount);
      }

      @Override
      public void delete(PartitionKey key) {
        dao.delete(key);
      }

      @Override
      public boolean delete(PartitionKey key, E entity) {
        return dao.delete(key, entity);
      }

    };
  }

  private class SpecificAvroDaoDatasetReader<E> extends AbstractDatasetReader {

    private EntityScanner<E> scanner;
    private Iterator<E> iterator;

    public SpecificAvroDaoDatasetReader(EntityScanner<E> scanner) {
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
    public E next() {
      return iterator.next();
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

  private class SpecificAvroDaoDatasetWriter<E> implements DatasetWriter<E> {

    private EntityBatch batch;

    public SpecificAvroDaoDatasetWriter(EntityBatch batch) {
      this.batch = batch;
    }

    @Override
    public void open() {
      // noop
    }

    @Override
    public void write(E e) {
      batch.put(e);
    }

    @Override
    public void flush() {
      batch.flush();
    }

    @Override
    public void close() {
      batch.close();
    }

    @Override
    public boolean isOpen() {
      return true; // TODO
    }
  }
}
