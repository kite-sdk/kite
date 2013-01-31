package com.cloudera.data.hdfs;

import java.io.IOException;

public interface DatasetRepository {

  /**
   * Get the latest version of a named dataset.
   * 
   * @param name
   * @throws IOException 
   */
  public Dataset get(String name) throws IOException;

  /**
   * Get a specific version of a named dataset.
   * 
   * @param name
   * @param version
   * @return
   */
  public Dataset get(String name, long version);

}
