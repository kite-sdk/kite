package com.cloudera.data.hdfs;

public interface DatasetRepository {

  /**
   * Get the latest version of a named dataset.
   * 
   * @param name
   */
  public Dataset get(String name);

  /**
   * Get a specific version of a named dataset.
   * 
   * @param name
   * @param version
   * @return
   */
  public Dataset get(String name, long version);

}
