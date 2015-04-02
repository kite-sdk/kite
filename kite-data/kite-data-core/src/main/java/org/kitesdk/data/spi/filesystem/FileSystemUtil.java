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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.Schemas;
import org.kitesdk.data.spi.partition.ProvidedFieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtil.class);

  private static final List<Format> SUPPORTED_FORMATS = Lists.newArrayList(
      Formats.AVRO, Formats.PARQUET, Formats.JSON, Formats.CSV);

  /**
   * Creates, if necessary, the given the location for {@code descriptor}.
   *
   * @param conf A Configuration
   * @param descriptor A DatasetDescriptor
   * @throws DatasetIOException
   * @since 0.13.0
   */
  public static void ensureLocationExists(
      DatasetDescriptor descriptor, Configuration conf) {
    Preconditions.checkNotNull(descriptor.getLocation(),
        "Cannot get FileSystem for a descriptor with no location");

    Path dataPath = new Path(descriptor.getLocation());
    FileSystem fs = null;

    try {
      fs = dataPath.getFileSystem(conf);
    } catch (IOException e) {
      throw new DatasetIOException(
          "Cannot get FileSystem for descriptor: " + descriptor, e);
    }

    try {
      if (!fs.exists(dataPath)) {
        fs.mkdirs(dataPath);
      }
    } catch (IOException e) {
      throw new DatasetIOException("Cannot access data location", e);
    }
  }

  static boolean cleanlyDelete(FileSystem fs, Path root, Path path) {
    Preconditions.checkNotNull(fs, "File system cannot be null");
    Preconditions.checkNotNull(root, "Root path cannot be null");
    Preconditions.checkNotNull(path, "Path to delete cannot be null");
    try {
      boolean deleted;
      // attempt to relativize the path to delete
      Path relativePath;
      if (path.isAbsolute()) {
        relativePath = new Path(root.toUri().relativize(path.toUri()));
      } else {
        relativePath = path;
      }

      if (relativePath.isAbsolute()) {
        // path is not relative to the root. delete just the path
        LOG.debug("Deleting path {}", path);
        deleted = fs.delete(path, true /* include any files */ );
      } else {
        // the is relative to the root path
        Path absolute = new Path(root, relativePath);
        LOG.debug("Deleting path {}", absolute);
        deleted = fs.delete(absolute, true /* include any files */ );
        // iterate up to the root, removing empty directories
        for (Path current = absolute.getParent();
             !current.equals(root) && !(current.getParent() == null);
             current = current.getParent()) {
          final FileStatus[] stats = fs.listStatus(current);
          if (stats == null || stats.length == 0) {
            // dir is empty and should be removed
            LOG.debug("Deleting empty path {}", current);
            deleted = fs.delete(current, true) || deleted;
          } else {
            // all parent directories will be non-empty
            break;
          }
        }
      }
      return deleted;
    } catch (IOException ex) {
      throw new DatasetIOException("Could not cleanly delete path:" + path, ex);
    }
  }

  public static Schema schema(String name, FileSystem fs, Path location) throws IOException {
    if (!fs.exists(location)) {
      return null;
    }

    return visit(new GetSchema(name), fs, location);
  }

  public static PartitionStrategy strategy(FileSystem fs, Path location) throws IOException {
    if (!fs.exists(location)) {
      return null;
    }

    List<Pair<String, Class<? extends Comparable>>> pairs = visit(
        new GetPartitionInfo(), fs, location);

    if (pairs.isEmpty() || pairs.size() <= 1) {
      return null;
    }

    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();

    // skip the initial partition because it is the containing directory
    for (int i = 1; i < pairs.size(); i += 1) {
      Pair<String, Class<? extends Comparable>> pair = pairs.get(i);
      builder.provided(
          pair.first() == null ? "partition_" + i : pair.first(),
          ProvidedFieldPartitioner.valuesString(pair.second()));
    }

    return builder.build();
  }

  public static Format format(FileSystem fs, Path location) throws IOException {
    if (!fs.exists(location)) {
      return null;
    }

    return visit(new GetFormat(), fs, location);
  }

  private static abstract class PathVisitor<T> {
    abstract T directory(FileSystem fs, Path path, List<T> children) throws IOException;
    abstract T file(FileSystem fs, Path path) throws IOException;
  }

  private static <T> T visit(PathVisitor<T> visitor, FileSystem fs, Path path)
      throws IOException {
    return visit(visitor, fs, path, Lists.<Path>newArrayList());
  }

  private static final DynMethods.UnboundMethod IS_SYMLINK;
  static {
    DynMethods.UnboundMethod isSymlink;
    try {
      isSymlink = new DynMethods.Builder("isSymlink")
          .impl(FileStatus.class)
          .buildChecked();
    } catch (NoSuchMethodException e) {
      isSymlink = null;
    }
    IS_SYMLINK = isSymlink;
  }

  private static <T> T visit(PathVisitor<T> visitor, FileSystem fs, Path path,
                      List<Path> followedLinks) throws IOException {
    if (fs.isFile(path)) {
      return visitor.file(fs, path);
    } else if (IS_SYMLINK != null &&
        IS_SYMLINK.<Boolean>invoke(fs.getFileStatus(path))) {
      Preconditions.checkArgument(!followedLinks.contains(path),
          "Encountered recursive path structure at link: " + path);
      followedLinks.add(path); // no need to remove
      return visit(visitor, fs, fs.getLinkTarget(path), followedLinks);
    }

    List<T> children = Lists.newArrayList();

    FileStatus[] statuses = fs.listStatus(path, PathFilters.notHidden());
    for (FileStatus stat : statuses) {
      children.add(visit(visitor, fs, stat.getPath()));
    }

    return visitor.directory(fs, path, children);
  }

  private static class GetPartitionInfo
      extends PathVisitor<List<Pair<String, Class<? extends Comparable>>>> {
    private static final Splitter EQUALS = Splitter.on('=').limit(2).trimResults();

    @Override
    List<Pair<String, Class<? extends Comparable>>> directory(
        FileSystem fs, Path path, List<List<Pair<String, Class<? extends Comparable>>>> children)
        throws IOException {

      // merge the levels under this one
      List<Pair<String, Class<? extends Comparable>>> accumulated = Lists.newArrayList();
      for (List<Pair<String, Class<? extends Comparable>>> child : children) {
        if (child == null) {
          continue;
        }

        for (int i = 0; i < child.size(); i += 1) {
          if (accumulated.size() > i) {
            Pair<String, Class<? extends Comparable>> pair = merge(
                accumulated.get(i), child.get(i));
            accumulated.set(i, pair);
          } else if (child.get(i) != null) {
            accumulated.add(child.get(i));
          }
        }
      }

      List<String> parts = Lists.newArrayList(EQUALS.split(path.getName()));
      String name;
      String value;
      if (parts.size() == 2) {
        name = parts.get(0);
        value = parts.get(1);
      } else {
        name = null;
        value = parts.get(0);
      }

      accumulated.add(0,
          new Pair<String, Class<? extends Comparable>>(name, dataClass(value)));

      return accumulated;
    }

    @Override
    List<Pair<String, Class<? extends Comparable>>> file(
        FileSystem fs, Path path) throws IOException {
      return null;
    }

    public Pair<String, Class<? extends Comparable>> merge(
        Pair<String, Class<? extends Comparable>> left,
        Pair<String, Class<? extends Comparable>> right) {
      String name = left.first();
      if (name == null || name.isEmpty()) {
        name = right.first();
      }

      if (left.second() == String.class) {
        return new Pair<String, Class<? extends Comparable>>(name, String.class);
      } else if (right.second() == String.class) {
        return new Pair<String, Class<? extends Comparable>>(name, String.class);
      } else if (left.second() == Long.class) {
        return new Pair<String, Class<? extends Comparable>>(name, Long.class);
      } else if (right.second() == Long.class) {
        return new Pair<String, Class<? extends Comparable>>(name, Long.class);
      }
      return new Pair<String, Class<? extends Comparable>>(name, Integer.class);
    }

    public Class<? extends Comparable> dataClass(String value) {
      try {
        Integer.parseInt(value);
        return Integer.class;
      } catch (NumberFormatException e) {
        // not an integer
      }
      try {
        Long.parseLong(value);
        return Long.class;
      } catch (NumberFormatException e) {
        // not a long
      }
      return String.class;
    }
  }

  private static class GetFormat extends PathVisitor<Format> {
    @Override
    Format directory(FileSystem fs, Path path, List<Format> formats) throws IOException {
      Format format = null;
      for (Format otherFormat : formats) {
        if (format == null) {
          format = otherFormat;
        } else if (!format.equals(otherFormat)) {
          throw new ValidationException(String.format(
              "Path contains multiple formats (%s, %s): %s",
              format, otherFormat, path));
        }
      }
      return format;
    }

    @Override
    Format file(FileSystem fs, Path path) throws IOException {
      String filename = path.getName();
      for (Format format : SUPPORTED_FORMATS) {
        if (filename.endsWith(format.getExtension())) {
          return format;
        }
      }
      return null;
    }
  }

  private static class GetSchema extends PathVisitor<Schema> {
    private final String name;

    public GetSchema(String name) {
      this.name = name;
    }

    @Override
    Schema directory(FileSystem fs, Path path, List<Schema> schemas) {
      Schema merged = null;
      for (Schema schema : schemas) {
        merged = merge(merged, schema);
      }
      return merged;
    }

    @Override
    Schema file(FileSystem fs, Path path) throws IOException {
      String filename = path.getName();
      if (filename.endsWith(Formats.AVRO.getExtension())) {
        return Schemas.fromAvro(fs, path);
      } else if (filename.endsWith(Formats.PARQUET.getExtension())) {
        return Schemas.fromParquet(fs, path);
      } else if (filename.endsWith(Formats.JSON.getExtension())) {
        return Schemas.fromJSON(name, fs, path);
      }
      return null;
    }

    private static Schema merge(@Nullable Schema left, @Nullable Schema right) {
      if (left == null) {
        return right;
      } else if (right == null) {
        return left;
      } else {
        return SchemaUtil.merge(left, right);
      }
    }
  }

}
