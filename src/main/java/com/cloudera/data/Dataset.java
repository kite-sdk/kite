package com.cloudera.data;

import org.apache.avro.Schema;

/**
 * A logical representation of a set of data entities.
 * 
 * Logically, all datasets have two generic properties: a name, and a schema
 * that describes an entity of that dataset. Concrete implementations of
 * {@code Dataset} may support additional properties, mandatory or otherwise, as
 * needed. {@code Dataset}s are not normally instantiated directly, but managed
 * by a repository (also implementation-specific).
 * 
 * @see DatasetRepository
 */
public interface Dataset {

  /**
   * Get the name of a {@code Dataset}. No guarantees about the format of this
   * name are made.
   */
  String getName();

  /**
   * Get the {@code Dataset}'s associated {@link Schema}. Depending on the
   * underlying storage system, this schema may be simple (i.e. records made up
   * of only scalar types) or complex (i.e. containing other records, lists, and
   * so on). Validation of the supported schemas is performed by the managing
   * repository, not the {@code Dataset} itself.
   * 
   * @return
   */
  Schema getSchema();

}
