/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Predicate;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.StorageKey;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class FileSystemPartitionIterator implements
    Iterator<StorageKey>, Iterable<StorageKey> {

  private static final Logger LOG = LoggerFactory.getLogger(
      FileSystemPartitionIterator.class);

  private final FileSystem fs;
  private final Path rootDirectory;
  private final Iterator<StorageKey> iterator;
  private final PartitionStrategy strategy;
  private final Schema schema;
  private final Predicate<StorageKey> predicate;
  private final MakePartialKey makeKey;

  class FileSystemIterator extends MultiLevelIterator<String> {
    public FileSystemIterator(int depth) throws IOException {
      super(depth);
    }

    @Override
    @SuppressWarnings({"unchecked", "deprecation"})
    public Iterable<String> getLevel(List<String> current) {
      final Set<String> dirs = Sets.newLinkedHashSet();

      Path dir = rootDirectory;
      for (int i = 0, n = current.size(); i < n; i += 1) {
        dir = new Path(dir, current.get(i));
      }

      StorageKey key = makeKey.apply(dir);
      boolean applies = true;
      //if the key doesn't have any values then do not apply the predicate and assume it is ok.  The official
      //predicate applied on the full path will exclude the details.
      if(key.size() > 0){
        applies = predicate.apply(key);
      }else{
        LOG.debug("Not applying predicate proactively because path {} does not have any key values.", dir);
      }
      if(applies) {
        try {
          for (FileStatus stat : fs.listStatus(dir, PathFilters.notHidden())) {
            if (stat.isDir()) {
              // TODO: add a check here for range.couldContain(Marker)
              dirs.add(stat.getPath().getName());
            }
          }
        } catch (IOException ex) {
          throw new DatasetException("Cannot list directory:" + dir, ex);
        }
      }else{
        LOG.debug("Skipping exploring {} path as it did not match the predicate {}", dir, predicate);
      }

      return dirs;
    }
  }

  /**
   * Conversion function to transform a List into a {@link StorageKey}.
   */
  private static class MakeKey implements Function<List<String>, StorageKey> {
    private final StorageKey reusableKey;
    private final PathConversion convert;

    public MakeKey(PartitionStrategy strategy, Schema schema) {
      this.reusableKey = new StorageKey(strategy);
      this.convert = new PathConversion(schema);
    }

    @Override
    @SuppressWarnings("unchecked")
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        justification="Non-null @Nullable parameter inherited from Function")
    public StorageKey apply(List<String> dirs) {
      return reusableKey.reuseFor(dirs, convert);
    }

    public StorageKey apply(Path path) {
      return reusableKey.reuseFor(path, convert);
    }
  }

  /**
   * Function that will create a {@link StorageKey} based on the values available instead
   * of requiring the entire path to create a key.
   */
  private static class MakePartialKey implements Function<List<String>, StorageKey> {
    private final StorageKey reusableKey;
    private final PathConversion convert;

    public MakePartialKey(Path rootPath, PartitionStrategy strategy, Schema schema) {
      this.reusableKey = new StorageKey(strategy);
      this.convert = new PartialPathConversion(rootPath, schema);
    }

    @Override
    @SuppressWarnings("unchecked")
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
            justification="Non-null @Nullable parameter inherited from Function")
    public StorageKey apply(List<String> dirs) {
      return reusableKey.reuseFor(dirs, convert);
    }

    public StorageKey apply(Path path) {
      return reusableKey.reuseFor(path, convert);
    }
  }

  @SuppressWarnings("deprecation")
  FileSystemPartitionIterator(
      FileSystem fs, Path root, PartitionStrategy strategy, Schema schema,
      final Predicate<StorageKey> predicate)
      throws IOException {
    Preconditions.checkArgument(fs.isDirectory(root));
    this.fs = fs;
    this.strategy = strategy;
    this.schema = schema;
    this.predicate = predicate;

    this.rootDirectory = root;
    this.makeKey = new MakePartialKey(rootDirectory, strategy, schema);
    this.iterator = Iterators.filter(
        Iterators.transform(
            new FileSystemIterator(
                Accessor.getDefault().getFieldPartitioners(strategy).size()), new MakeKey(strategy, schema)),
        predicate);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public StorageKey next() {
    return iterator.next();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public Iterator<StorageKey> iterator() {
    return this;
  }

}
