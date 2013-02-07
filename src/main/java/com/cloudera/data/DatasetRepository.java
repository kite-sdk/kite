package com.cloudera.data;

import java.io.IOException;

import org.apache.avro.Schema;

public interface DatasetRepository<DS extends Dataset> {

  /**
   * Get the latest version of a named dataset.
   * 
   * @param name
   * @throws IOException
   */
  DS get(String name) throws IOException;

  /**
   * Create a dataset with the supplied schema. Depending on the underlying
   * dataset storage, some schemas types or configurations may not be supported.
   * If an illegal schema is supplied, an exception will be thrown by the
   * implementing class.
   * 
   * @param name
   *          The fully qualified dataset name
   * @param schema
   *          A schema that describes entities of this dataset
   * @return The newly created dataset
   * @throws IOException
   */
  DS create(String name, Schema schema) throws IOException;

}
