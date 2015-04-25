/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.util.Iterator;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionListener;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.StorageKey;

/**
 * A {@link PartitionView} that is backed by a directory in a file system.
 */
@Immutable
class FileSystemPartitionView<E> extends FileSystemView<E>
    implements PartitionView<E> {

  private static final Splitter PATH_SPLITTER = Splitter.on('/');
  private static final Splitter KV_SPLITTER = Splitter.on('=').limit(2);

  private final Path location;
  private final URI relativeLocation;

  FileSystemPartitionView(FileSystemDataset<E> dataset,
                          @Nullable PartitionListener partitionListener,
                          @Nullable SignalManager signalManager,
                          Class<E> type) {
    super(dataset, partitionListener, signalManager, type);
    this.location = dataset.getDirectory();
    this.relativeLocation = null;
  }

  static <E> FileSystemPartitionView<E> getPartition(
      FileSystemPartitionView<E> base, URI location) {
    URI relative = relativize(base.root.toUri(), location);

    if (relative == null) {
      return base;
    }

    return new FileSystemPartitionView<E>(base, relative);
  }

  static <E> FileSystemPartitionView<E> getPartition(
      FileSystemPartitionView<E> base, Path location) {
    URI relative = relativize(
        base.root.toUri(), (location == null ? null : location.toUri()));

    if (relative == null) {
      return base;
    }

    return new FileSystemPartitionView<E>(base, relative);
  }

  private FileSystemPartitionView(FileSystemPartitionView<E> view,
                                  URI relativeLocation) {
    super(view, constraints(view, relativeLocation));
    this.location = new Path(view.root, relativeLocation.toString());
    this.relativeLocation = relativeLocation;
  }

  @Override
  public URI getLocation() {
    return location.toUri();
  }

  URI getRelativeLocation() {
    return relativeLocation;
  }

  FileSystemView<E> toConstraintsView() {
    return filter(constraints);
  }

  @Override
  public Iterable<PartitionView<E>> getCoveringPartitions() {
    // filter the matching partitions to return those contained by this one
    return Iterables.filter(super.getCoveringPartitions(),
        new Predicate<PartitionView<E>>() {
          @Override
          public boolean apply(@Nullable PartitionView<E> input) {
            return input != null &&
                contains(location.toUri(), root, input.getLocation());
          }
        });
  }

  @Override
  public boolean deleteAll() {
    return deleteAllUnsafe();
  }

  @Override
  protected Predicate<StorageKey> getKeyPredicate() {
    if (relativeLocation == null) {
      return Predicates.alwaysTrue();
    }
    return new PartitionKeyPredicate(root, location);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!super.equals(other)) {
      return false;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    FileSystemPartitionView that = (FileSystemPartitionView) other;
    return Objects.equal(this.location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), location);
  }

  private static boolean contains(URI location, Path root, URI relative) {
    URI full = new Path(root, relative.getPath()).toUri();
    return !location.relativize(full).isAbsolute();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
      justification="Null value checked by precondition")
  private static URI relativize(@Nullable URI root, @Nullable URI location) {
    Preconditions.checkNotNull(root, "Cannot find location relative to null");

    if (location == null) {
      return null;
    }

    String scheme = location.getScheme();
    String path = location.getPath();
    URI relative;
    if (scheme == null && !path.startsWith("/")) {
      // already a relative location
      relative = location;

    } else {
      String rootScheme = root.getScheme();
      Preconditions.checkArgument(
          (scheme == null || rootScheme == null) || scheme.equals(rootScheme),
          "%s is not contained in %s", location, root);

      // use just the paths to avoid authority mismatch errors
      URI rootPath = URI.create(root.getPath());
      relative = rootPath.relativize(URI.create(path));
      if (relative.getPath().isEmpty()) {
        return null;
      }
    }

    // remove a trailing slash
    String relativePath = relative.getPath();
    if (relativePath.endsWith("/")) {
      relative = URI.create(relativePath.substring(0, relativePath.length()-1));
    }

    Preconditions.checkArgument(!relative.getPath().startsWith("/"),
        "%s is not contained in %s", location, root);

    return relative;
  }

  /**
   * Build partition constraints for the partition URI location.
   *
   * @param view a {@code FileSystemPartitionView} containing {@code location}
   * @param relative a relative URI for a partition within the given view
   * @return a set of constraints that match the location
   */
  private static Constraints constraints(FileSystemPartitionView<?> view,
                                         @Nullable URI relative) {
    DatasetDescriptor descriptor = view.dataset.getDescriptor();

    if (relative == null) {
      // no partitions are selected, so no additional constraints
      return view.constraints;
    }

    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Dataset is not partitioned");

    Constraints constraints = view.constraints;

    Schema schema = descriptor.getSchema();
    PartitionStrategy strategy = descriptor.getPartitionStrategy();

    Iterator<String> parts = PATH_SPLITTER.split(relative.getPath()).iterator();
    for (FieldPartitioner fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
      if (!parts.hasNext()) {
        break;
      }
      String directory = parts.next();
      String value = Iterables.getLast(KV_SPLITTER.split(directory));
      Schema fieldSchema = SchemaUtil.fieldSchema(schema, strategy, fp.getName());
      constraints = constraints.with(
          fp.getName(), Conversions.convert(value, fieldSchema));
    }

    Preconditions.checkArgument(!parts.hasNext(),
        "%s is deeper than the partition strategy", relative);

    return constraints;
  }

  private static class PartitionKeyPredicate implements Predicate<StorageKey> {
    private final Path root;
    private final URI location;

    public PartitionKeyPredicate(Path root, Path location) {
      this.root = root;
      this.location = location.toUri();
    }

    @Override
    public boolean apply(@Nullable StorageKey key) {
      return (key != null &&
          key.getPath() != null &&
          contains(location, root, key.getPath().toUri()));
    }

  }
}
