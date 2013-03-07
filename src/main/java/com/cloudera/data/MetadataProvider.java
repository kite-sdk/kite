package com.cloudera.data;

import java.io.IOException;

/**
 * <p>
 * Defines a service provider interface for metadata system plugins.
 * </p>
 * <p>
 * Implementations of {@link DatasetRepository} are written against this
 * interface and, by extension, can store dataset metadata information contained
 * in the {@link DatasetDescriptor} in a system of the user's choosing.
 * </p>
 */
public interface MetadataProvider {

  /**
   * Load the dataset descriptor for the dataset {@code name}.
   * 
   * @param name
   *          The fully qualified name of an existing dataset.
   * @return A dataset descriptor.
   * @throws IOException
   *           If the dataset doesn't exist or the descriptor can not be loaded.
   */
  DatasetDescriptor load(String name) throws IOException;

  /**
   * Save the dataset descriptor for the dataset {@code name}.
   * 
   * @param name
   *          The fully qualified name of a dataset.
   * @param descriptor
   *          A dataset descriptor.
   * @throws IOException
   *           If the dataset descriptor can not be saved.
   */
  void save(String name, DatasetDescriptor descriptor) throws IOException;

}
