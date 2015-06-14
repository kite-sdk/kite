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
package org.kitesdk.data.spi.filesystem;

import java.util.Iterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.PartitionListener;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.PartitionedDataset;
import org.kitesdk.data.spi.Replaceable;
import org.kitesdk.data.spi.SizeAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.IndexedRecord;
import org.kitesdk.data.Formats;

@SuppressWarnings("deprecation")
public class FileSystemDataset<E> extends AbstractDataset<E> implements
    Mergeable<FileSystemDataset<E>>, InputFormatAccessor<E>, LastModifiedAccessor,
    PartitionedDataset<E>, SizeAccessor, Signalable<E>, Replaceable<View<E>> {

  private static final Logger LOG = LoggerFactory
    .getLogger(FileSystemDataset.class);

  private final FileSystem fileSystem;
  private final Path directory;
  private final String namespace;
  private final String name;
  private final DatasetDescriptor descriptor;
  private PartitionKey partitionKey;
  private final URI uri;

  private static final String SIGNALS_DIRECTORY_NAME = ".signals";

  private final PartitionStrategy partitionStrategy;
  private final PartitionListener partitionListener;

  final FileSystemPartitionView<E> unbounded;

  // reusable path converter, has no relevant state
  private final PathConversion convert;
  private final SignalManager signalManager;

  FileSystemDataset(FileSystem fileSystem, Path directory,
                    String namespace, String name,
                    DatasetDescriptor descriptor, URI uri,
                    @Nullable PartitionListener partitionListener,
                    Class<E> type) {
    super(type, descriptor.getSchema());
    if (Formats.PARQUET.equals(descriptor.getFormat())) {
      Preconditions.checkArgument(IndexedRecord.class.isAssignableFrom(type) ||
          type == Object.class,
          "Parquet only supports generic and specific data models, type"
          + " parameter must implement IndexedRecord");
    }

    this.fileSystem = fileSystem;
    this.directory = directory;
    this.namespace = namespace;
    this.name = name;
    this.descriptor = descriptor;
    this.partitionStrategy =
        descriptor.isPartitioned() ? descriptor.getPartitionStrategy() : null;
    this.partitionListener = partitionListener;
    this.convert = new PathConversion(descriptor.getSchema());
    this.uri = uri;

    Path signalsPath = new Path(directory, SIGNALS_DIRECTORY_NAME);
    this.signalManager = new SignalManager(fileSystem, signalsPath);
    this.unbounded = new FileSystemPartitionView<E>(
        this, partitionListener, signalManager, type);

    // remove this.partitionKey for 0.14.0
    this.partitionKey = null;
  }

  FileSystemDataset(FileSystem fileSystem, Path directory,
                    String namespace, String name,
                    DatasetDescriptor descriptor, URI uri,
                    @Nullable PartitionKey partitionKey,
                    @Nullable PartitionListener partitionListener,
                    Class<E> type) {
    this(fileSystem, directory, namespace, name, descriptor, uri,
        partitionListener, type);
    this.partitionKey = partitionKey;
  }

  private FileSystemDataset(FileSystemDataset<?> toCopy, Class<E> type) {
    super(type, toCopy.descriptor.getSchema());
    this.fileSystem = toCopy.fileSystem;
    this.directory = toCopy.directory;
    this.namespace = toCopy.namespace;
    this.name = toCopy.name;
    this.descriptor = toCopy.descriptor;
    this.partitionStrategy = toCopy.partitionStrategy;
    this.partitionListener = toCopy.partitionListener;
    this.convert = toCopy.convert;
    this.uri = toCopy.uri;
    this.signalManager = toCopy.signalManager;
    this.unbounded = new FileSystemPartitionView<E>(
        this, partitionListener, signalManager, type);
    this.partitionKey = null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Dataset<T> asType(Class<T> type) {
    if (getType().equals(type)) {
      return (Dataset<T>) this;
    }
    return new FileSystemDataset<T>(this, type);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  // needed to preserve the behavior of FileSystemDatasets
  View<E> viewForUri(URI location) {
    Preconditions.checkNotNull(location, "Partition location cannot be null");
    PartitionView<E> view = getPartitionView(location);
    if (view == unbounded) {
      return this;
    }
    return view;
  }

  FileSystemPartitionView<E> getPartitionView(URI location) {
    return FileSystemPartitionView.getPartition(unbounded, location);
  }

  FileSystemPartitionView<E> getPartitionView(Path location) {
    return FileSystemPartitionView.getPartition(unbounded, location);
  }

  @Override
  public Iterable<PartitionView<E>> getCoveringPartitions() {
    return unbounded.getCoveringPartitions();
  }

  PartitionKey getPartitionKey() {
    return partitionKey;
  }

  FileSystem getFileSystem() {
    return fileSystem;
  }

  Path getDirectory() {
    return directory;
  }

  PartitionListener getPartitionListener() {
    return partitionListener;
  }

  public boolean deleteAll() {
    // no constraints, so delete is always aligned to partition boundaries
    return unbounded.deleteAllUnsafe();
  }

  public PathIterator pathIterator() {
    return unbounded.pathIterator();
  }

  /**
   * Returns an iterator that provides all leaf-level directories in this view.
   *
   * @return leaf-directory iterator
   */
  public Iterator<Path> dirIterator() {
    return unbounded.dirIterator();
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return unbounded;
  }

  @Override
  public FileSystemView<E> filter(Constraints c) {
    return unbounded.filter(c);
  }

  @Override
  @Nullable
  @SuppressWarnings("deprecation")
  public PartitionedDataset<E> getPartition(PartitionKey key, boolean allowCreate) {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get a partition on a non-partitioned dataset (name:%s)",
      name);

    LOG.debug("Loading partition for key {}, allowCreate:{}", new Object[]{
        key, allowCreate});

    Path partitionDirectory = fileSystem.makeQualified(
        toDirectoryName(directory, key));

    try {
      if (!fileSystem.exists(partitionDirectory)) {
        if (allowCreate) {
          if (partitionListener != null) {
            partitionListener.partitionAdded(namespace, name,
                toRelativeDirectory(key).toString());
          }

          // ensure that the directory exists, it may or may not have been
          // created by the partitionListener
          fileSystem.mkdirs(partitionDirectory);
        } else {
          return null;
        }
      }
    } catch (IOException e) {
      throw new DatasetIOException("Unable to locate or create dataset partition directory " + partitionDirectory, e);
    }

    int partitionDepth = key.getLength();
    PartitionStrategy subpartitionStrategy = Accessor.getDefault()
        .getSubpartitionStrategy(partitionStrategy, partitionDepth);

    return new FileSystemDataset.Builder<E>()
        .namespace(namespace)
        .name(name)
        .fileSystem(fileSystem)
        .uri(uri)
        .descriptor(new DatasetDescriptor.Builder(descriptor)
            .location(partitionDirectory)
            .partitionStrategy(subpartitionStrategy)
            .build())
        .type(type)
        .partitionKey(key)
        .partitionListener(partitionListener)
        .build();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void dropPartition(PartitionKey key) {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to drop a partition on a non-partitioned dataset (name:%s)",
      name);
    Preconditions.checkNotNull(key, "Partition key may not be null");

    LOG.debug("Dropping partition with key:{} dataset:{}", key, name);

    Path partitionDirectory = toDirectoryName(directory, key);

    try {
      if (!fileSystem.delete(partitionDirectory, true)) {
        throw new IOException("Partition directory " + partitionDirectory
          + " for key " + key + " does not exist");
      }
    } catch (IOException e) {
      throw new DatasetIOException("Unable to locate or drop dataset partition directory " + partitionDirectory, e);
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public Iterable<PartitionedDataset<E>> getPartitions() {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get partitions on a non-partitioned dataset (name:%s)",
      name);

    List<PartitionedDataset<E>> partitions = Lists.newArrayList();

    FileStatus[] fileStatuses;

    try {
      fileStatuses = fileSystem.listStatus(directory,
        PathFilters.notHidden());
    } catch (IOException e) {
      throw new DatasetIOException("Unable to list partition directory for directory " + directory, e);
    }

    for (FileStatus stat : fileStatuses) {
      Path p = fileSystem.makeQualified(stat.getPath());
      PartitionKey key = keyFromDirectory(p.getName());
      PartitionStrategy subPartitionStrategy = Accessor.getDefault()
          .getSubpartitionStrategy(partitionStrategy, 1);
      Builder<E> builder = new FileSystemDataset.Builder<E>()
          .namespace(namespace)
          .name(name)
          .fileSystem(fileSystem)
          .uri(uri)
          .descriptor(new DatasetDescriptor.Builder(descriptor)
              .location(p)
              .partitionStrategy(subPartitionStrategy)
              .build())
          .type(type)
          .partitionKey(key)
          .partitionListener(partitionListener);

      partitions.add(builder.build());
    }

    return partitions;
  }

  public void addExistingPartitions() {
    if (partitionListener != null && descriptor.isPartitioned()) {
      for (Path partition : pathIterator()) {
        partitionListener.partitionAdded(namespace, name, partition.toString());
      }
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
      .add("descriptor", descriptor).add("directory", directory)
      .add("dataDirectory", directory).add("partitionKey", partitionKey)
      .toString();
  }

  @Override
  public void merge(FileSystemDataset<E> update) {
    DatasetDescriptor updateDescriptor = update.getDescriptor();

    // check that the dataset's descriptor can read the update
    Compatibility.checkCompatible(updateDescriptor, descriptor);

    for (PartitionView<E> src : update.getCoveringPartitions()) {
      if (src instanceof FileSystemPartitionView) {
        URI relative = ((FileSystemPartitionView<E>) src).getRelativeLocation();
        PartitionView<E> dest = relative != null ?
            getPartitionView(relative) : unbounded;

        // We call this listener before we attempt to create any partition
        // directories. This lets the listener decide how to create the
        // directory, if desired. Hive managed datasets let the Hive
        // metastore create them while external datasets create it
        // locally
        if (descriptor.isPartitioned() && partitionListener != null &&
            relative != null) {
          partitionListener.partitionAdded(namespace, name, relative.toString());
        }

        List<Pair<Path, Path>> staged = FileSystemUtil.stageMove(fileSystem,
            new Path(src.getLocation()), new Path(dest.getLocation()),
            "tmp" /* data should be added to recover from a failure */ );
        FileSystemUtil.finishMove(fileSystem, staged);

      } else {
        throw new IllegalArgumentException(
            "Incompatible PartitionView: " + src.getClass().getName());
      }
    }
  }

  @Override
  public boolean canReplace(View<E> part) {
    if (part instanceof FileSystemView) {
      return equals(part.getDataset()) &&
          ((FileSystemView) part).getConstraints().alignedWithBoundaries();
    } else if (part instanceof FileSystemDataset) {
      return equals(part);
    }
    return false;
  }

  @Override
  public void replace(View<E> target, View<E> replacement) {
    DatasetDescriptor updateDescriptor = replacement.getDataset().getDescriptor();

    // check that the dataset's descriptor can read the update
    Compatibility.checkCompatible(updateDescriptor, descriptor);

    if (descriptor.isPartitioned()) {
      // track current partitions: either replace or delete
      Set<PartitionView<E>> notReplaced = Sets.newHashSet(
          target.getCoveringPartitions());

      // replace leaf partitions one at a time
      for (PartitionView<E> src : replacement.getCoveringPartitions()) {
        if (src instanceof FileSystemPartitionView) {
          FileSystemPartitionView<E> dest = getPartitionView(
              ((FileSystemPartitionView<E>) src).getRelativeLocation());

          // The destination partition view may not exist, if the source data was
          // stored in directories with non-standard names. To account for this,
          // find all of the directories that should be removed: those that match
          // the partition constraints.
          List<Path> removals = Lists.newArrayList();
          Iterable<PartitionView<E>> existingPartitions = dest
              .toConstraintsView()
              .getCoveringPartitions();
          for (PartitionView<E> partition : existingPartitions) {
            FileSystemPartitionView<E> toReplace =
                (FileSystemPartitionView<E>) partition;
            Path path = new Path(toReplace.getLocation());
            removals.add(path);
            notReplaced.remove(toReplace);
            if (partitionListener != null && descriptor.isPartitioned()) {
              partitionListener.partitionDeleted(
                  namespace, name, toReplace.getRelativeLocation().toString());
            }
          }

          // replace the directory all at once
          FileSystemUtil.replace(fileSystem, directory,
              new Path(dest.getLocation()), new Path(src.getLocation()),
              removals);

          if (partitionListener != null && descriptor.isPartitioned()) {
            partitionListener.partitionAdded(
                namespace, name, dest.getRelativeLocation().toString());
          }

        } else {
          throw new IllegalArgumentException(
              "Incompatible PartitionView: " + src.getClass().getName());
        }
      }

      // remove the original partitions that were not individually replaced
      for (PartitionView<E> toRemove : notReplaced) {
        toRemove.deleteAll();
      }

    } else {
      // stage data in the dataset, remove existing data, then finish the move
      PartitionView<E> srcPartition = Iterables.getOnlyElement(
          replacement.getCoveringPartitions());
      List<Pair<Path, Path>> staged = FileSystemUtil.stageMove(fileSystem,
          new Path(srcPartition.getLocation()), new Path(unbounded.getLocation()),
          "replace" /* data should replace to recover from a failure */ );
      deleteAll(); // remove all existing files
      FileSystemUtil.finishMove(fileSystem, staged);
    }
  }

  @Override
  public InputFormat<E, Void> getInputFormat(Configuration conf) {
    return new FileSystemViewKeyInputFormat<E>(this, conf);
  }

  @SuppressWarnings("unchecked")
  private Path toDirectoryName(@Nullable Path dir, PartitionKey key) {
    Path result = dir;
    for (int i = 0; i < key.getLength(); i++) {
      final FieldPartitioner fp = Accessor.getDefault().getFieldPartitioners(partitionStrategy).get(i);
      if (result != null) {
        result = new Path(result, PathConversion.dirnameForValue(fp, key.get(i)));
      } else {
        result = new Path(PathConversion.dirnameForValue(fp, key.get(i)));
      }
    }
    return result;
  }

  private Path toRelativeDirectory(PartitionKey key) {
    return toDirectoryName(null, key);
  }

  @SuppressWarnings("unchecked")
  private PartitionKey keyFromDirectory(String name) {
    final FieldPartitioner fp = Accessor.getDefault().getFieldPartitioners(partitionStrategy).get(0);
    final List<Object> values = Lists.newArrayList();

    if (partitionKey != null) {
      values.addAll(partitionKey.getValues());
    }

    values.add(convert.valueForDirname(fp, name));

    return new PartitionKey(values.toArray());
  }

  @SuppressWarnings("unchecked")
  public PartitionKey keyFromDirectory(Path dir) {

    Path relDir = null;
    URI relUri = directory.toUri().relativize(dir.toUri());

    if (!relUri.toString().isEmpty()) {
      relDir = new Path(relUri);
      Preconditions.checkState(!relDir.equals(dir), "Partition directory %s is not " +
          "relative to dataset directory %s", dir, directory);
    }

    List<String> pathComponents = Lists.newArrayList();
    while (relDir != null && !relDir.getName().equals("")) {
      pathComponents.add(0, relDir.getName());
      relDir = relDir.getParent();
    }

    List<FieldPartitioner> fps = Accessor.getDefault().getFieldPartitioners(partitionStrategy);
    Preconditions.checkState(pathComponents.size() <= fps.size(),
        "Number of components in partition directory %s (%s) exceeds number of field " +
            "partitioners %s", dir, pathComponents, partitionStrategy);

    List<Object> values = Lists.newArrayList();
    for (int i = 0; i < pathComponents.size(); i++) {
      values.add(convert.valueForDirname(fps.get(i), pathComponents.get(i)));
    }

    if (partitionKey != null) {
      values.addAll(0, partitionKey.getValues());
    }

    return new PartitionKey(values.toArray());
  }

  @Override
  public long getSize() {
    long size = 0;
    for (Iterator<Path> i = dirIterator(); i.hasNext(); ) {
      Path dir = i.next();
      try {
        for (FileStatus st : fileSystem.listStatus(dir)) {
          size += st.getLen();
        }
      } catch (IOException e) {
        throw new DatasetIOException("Cannot find size of " + dir, e);
      }
    }
    return size;
  }

  @Override
  public long getLastModified() {
    return unbounded.getLastModified();
  }

  @Override
  public boolean isEmpty() {
    return unbounded.isEmpty();
  }

  @Override
  public void signalReady() {
    unbounded.signalReady();
  }

  @Override
  public boolean isReady() {
    return unbounded.isReady();
  }

  public static class Builder<E> {

    private Configuration conf;
    private FileSystem fileSystem;
    private Path directory;
    private String namespace;
    private String name;
    private DatasetDescriptor descriptor;
    private Class<E> type;
    private URI uri;
    private PartitionKey partitionKey;
    private PartitionListener partitionListener;

    public Builder<E> namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder<E> name(String name) {
      this.name = name;
      return this;
    }

    protected Builder<E> fileSystem(FileSystem fs) {
      this.fileSystem = fs;
      return this;
    }

    public Builder<E> configuration(Configuration conf) {
      this.conf = conf;
      return this;
    }

     public Builder<E> descriptor(DatasetDescriptor descriptor) {
      Preconditions.checkArgument(descriptor.getLocation() != null,
          "Dataset location cannot be null");

      this.descriptor = descriptor;

      return this;
    }

    public Builder<E> type(Class<E> type) {
      Preconditions.checkNotNull(type, "Type cannot be null");

      this.type = type;

      return this;
    }

    public Builder<E> uri(URI uri) {
      this.uri = uri;
      return this;
    }

    Builder<E> partitionKey(@Nullable PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    Builder<E> partitionListener(@Nullable PartitionListener partitionListener) {
      this.partitionListener = partitionListener;
      return this;
    }

    public FileSystemDataset<E> build() {
      Preconditions.checkState(this.namespace != null, "No namespace defined");
      Preconditions.checkState(this.name != null, "No dataset name defined");
      Preconditions.checkState(this.descriptor != null,
        "No dataset descriptor defined");
      Preconditions.checkState((conf != null) || (fileSystem != null),
          "Configuration or FileSystem must be set");
      Preconditions.checkState(type != null, "No type specified");

      this.directory = new Path(descriptor.getLocation());

      if (fileSystem == null) {
        try {
          this.fileSystem = directory.getFileSystem(conf);
        } catch (IOException ex) {
          throw new DatasetIOException("Cannot access FileSystem", ex);
        }
      }

      Path absoluteDirectory = fileSystem.makeQualified(directory);
      return new FileSystemDataset<E>(
          fileSystem, absoluteDirectory, namespace, name, descriptor, uri,
          partitionKey, partitionListener, type);
    }
  }

}
