package com.cloudera.data.hdfs;

import java.io.DataInputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

public class TestDataset {

  private static final Logger logger = LoggerFactory
      .getLogger(TestDataset.class);

  @Test
  public void test() {
    Dataset ds = new Dataset();

    ds.setSchema(new Schema.Parser()
        .parse("{ \"type\": \"record\", \"name\": \"user\", \"fields\": [ { \"type\": \"string\", \"name\": \"username\" } ] }"));

    Schema schema = ds.getSchema();
    Record record = new Record(schema);

    logger.debug("schema:{} record:{}", schema, record);

    record.put("username", "test");

    logger.debug("record:{}", record);

    DataInputStream inputStream = ds.open();

    Closeables.closeQuietly(inputStream);
    Closeables.closeQuietly(ds);
  }

}
