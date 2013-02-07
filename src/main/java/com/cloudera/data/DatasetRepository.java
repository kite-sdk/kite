package com.cloudera.data;

import java.io.IOException;

import com.cloudera.data.hdfs.HDFSDataset;

public interface DatasetRepository {

  /**
   * Get the latest version of a named dataset.
   * 
   * @param name
   * @throws IOException 
   */
  public HDFSDataset get(String name) throws IOException;

}
