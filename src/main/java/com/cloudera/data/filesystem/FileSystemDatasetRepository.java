/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.data.filesystem;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetRepository;
import com.cloudera.data.MetadataProvider;
import com.cloudera.data.PartitionStrategy;
import com.cloudera.data.impl.Accessor;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * <p>
 * A {@link DatasetRepository} that stores data in a Hadoop {@link FileSystem}.
 * </p>
 * <h1>Overview</h1>
 * <p>
 * Given a {@link FileSystem}, a root directory, and a {@link MetadataProvider},
 * this {@link DatasetRepository} implementation can load and store
 * {@link Dataset}s on both local filesystems as well as the Hadoop Distributed
 * FileSystem (HDFS). Users may directly instantiate this class with the three
 * dependencies above and then perform dataset-related operations using any of
 * the provided methods. The primary methods of interest will be
 * {@link #create(String, DatasetDescriptor)} and {@link #get(String)} which
 * create a new dataset, or load an existing dataset, respectively. Once a
 * dataset has been created or loaded, users can invoke the appropriate
 * {@link Dataset} methods to get a reader or writer as needed.
 * </p>
 * <h1>Directory Naming and Structure</h1>
 * <p>
 * Other than the root directory, all paths and file layout is completely
 * controlled by the underlying implementation. Each dataset is represented by a
 * directory of the same name, with data files in its {@code data} directory.
 * Partitioned datasets follow the conventions of Apache Hive and Cloudera
 * Impala, with the dataset {@code data} directory containing subdirectories for
 * each partition. Partition directories use the attribute name, followed by an
 * equals sign ("="), and the value of that attribute as the directory name. For
 * example, a dataset partitioned by the {@code date} attribute of an entity has
 * the directory name {@code date=20130101} for the partition containing
 * entities whose {@code date} attribute was set to the value {@code 20130101} .
 * </p>
 * <p>
 * Here are some examples of API use and the resultant directory structures.
 * </p>
 * 
 * <pre>
 * DatasetRepository repo = new HDFSDatasetRepository(
 *   fileSystem,
 *   new Path("/data"),
 *   metadataProvider
 * );
 * 
 * Dataset userEvents = repo.create("UserEvents",
 *   new DatasetDescriptor.Builder()
 *     .schema(userEventSchema)
 *     .get()
 * );
 * 
 * DatasetWriter&gt;GenericRecord&lt; writer = userEvents.getWriter();
 * 
 * try {
 *   writer.open();
 *   writer.write(new GenericRecordBuilder(userEventSchema)
 *     .set("username", "foo")
 *     .set("...", "...")
 *     .build()
 *   );
 * 
 *   writer.flush();
 * } finally {
 *   writer.close();
 * }
 * </pre>
 * <p>
 * This example writes a single user event entity (using the Avro
 * {@code GenericRecordBuilder} to build an entity represented by the
 * {@code GenericRecord} type) to a simple (non-partitioned) dataset. The
 * resultant HDFS directory structure, assuming the instance of
 * {@code FileSystem} above was configured to connect to an HDFS cluster, would
 * look as follows.
 * </p>
 * 
 * <pre>
 * /data/UserEvents/data/*.avro
 * </pre>
 * <p>
 * If we instead create the {@code UserEvent} dataset with the following
 * {@code DatasetDescriptor}:
 * </p>
 * 
 * <pre>
 * Dataset userEvents = repo.create(
 *     &quot;UserEvents&quot;,
 *     new DatasetDescriptor.Builder()
 *         .schema(userEventSchema)
 *         .partitionStrategy(
 *             new PartitionStrategy.Builder().identity(&quot;date&quot;, 365).get()).get());
 * </pre>
 * <p>
 * the resultant dataset would be partitioned by the identity function applied
 * to the {@code date} attribute of the entity when it is written. The directory
 * structure will now be:
 * </p>
 * 
 * <pre>
 * /data/UserEvents/data/date=20130101/*.avro
 * /data/UserEvents/data/date=20130102/*.avro
 * ...
 * </pre>
 * 
 * @see DatasetRepository
 * @see Dataset
 * @see DatasetDescriptor
 * @see PartitionStrategy
 * @see MetadataProvider
 */
public class FileSystemDatasetRepository implements DatasetRepository {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDatasetRepository.class);

  private final MetadataProvider metadataProvider;

  private final Path rootDirectory;
  private final FileSystem fileSystem;

  public FileSystemDatasetRepository(FileSystem fileSystem, Path rootDirectory,
      MetadataProvider metadataProvider) {

    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
    this.metadataProvider = metadataProvider;
  }

  @Override
  public Dataset create(String name, DatasetDescriptor descriptor)
      throws IOException {

    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor can not be null");
    Preconditions.checkState(metadataProvider != null,
        "Metadata provider can not be null");

    Schema schema = descriptor.getSchema();

    Path datasetPath = pathForDataset(name);
    Path datasetDataPath = pathForDatasetData(name);

    if (fileSystem.exists(datasetPath)) {
      throw new IOException("Attempt to create an existing dataset:" + name);
    }

    logger.debug("Creating dataset:{} schema:{} datasetPath:{}", new Object[] {
        name, schema, datasetDataPath });

    if (!fileSystem.mkdirs(datasetDataPath)) {
      throw new IOException("Failed to make dataset diectories:"
          + datasetDataPath);
    }

    metadataProvider.save(name, descriptor);

    return new FileSystemDataset.Builder()
        .name(name)
        .fileSystem(fileSystem)
        .descriptor(descriptor)
        .directory(pathForDataset(name))
        .dataDirectory(pathForDatasetData(name))
        .partitionKey(
            descriptor.isPartitioned() ? Accessor.getDefault()
                .newPartitionKey() : null).get();
  }

  @Override
  public Dataset get(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkState(rootDirectory != null,
        "Root directory can not be null");
    Preconditions.checkState(fileSystem != null,
        "FileSystem implementation can not be null");
    Preconditions.checkState(metadataProvider != null,
        "Metadata provider can not be null");

    logger.debug("Loading dataset:{}", name);

    Path datasetDirectory = pathForDataset(name);
    Path datasetDataPath = pathForDatasetData(name);

    DatasetDescriptor descriptor = metadataProvider.load(name);

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .fileSystem(fileSystem).descriptor(descriptor)
        .directory(datasetDirectory).dataDirectory(datasetDataPath).name(name)
        .get();

    logger.debug("Loaded dataset:{}", ds);

    return ds;
  }

  @Override
  public boolean drop(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkState(rootDirectory != null,
        "Root directory can not be null");
    Preconditions.checkState(fileSystem != null,
        "FileSystem implementation can not be null");
    Preconditions.checkState(metadataProvider != null,
        "Metadata provider can not be null");

    logger.debug("Dropping dataset:{}", name);

    Path datasetPath = pathForDataset(name);

    if (metadataProvider.delete(name) && fileSystem.exists(datasetPath)) {
      if (fileSystem.delete(datasetPath, true)) {
        return true;
      } else {
        throw new IOException("Failed to delete dataset name:" + name
            + " data path:" + datasetPath);
      }
    } else {
      return false;
    }
  }

  private Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
        "Dataset repository root directory can not be null");

    return new Path(rootDirectory, name.replace('.', '/'));
  }

  private Path pathForDatasetData(String name) {
    return pathForDataset(name);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("rootDirectory", rootDirectory)
        .add("metadataProvider", metadataProvider)
        .add("fileSystem", fileSystem).toString();
  }

  public Path getRootDirectory() {
    return rootDirectory;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

}
