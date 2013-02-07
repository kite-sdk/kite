package com.cloudera.data;

import java.io.IOException;

import org.apache.avro.Schema;

import com.cloudera.data.hdfs.HDFSDataset;

public interface DatasetRepository<DS extends Dataset> {

  /**
   * Get the latest version of a named dataset.
   * 
   * @param name
   * @throws IOException
   */
  HDFSDataset get(String name) throws IOException;

  DS create(String name, Schema schema) throws IOException;

}
