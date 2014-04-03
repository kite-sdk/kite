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

package org.kitesdk.data.spi;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MemoryMetadataProvider extends AbstractMetadataProvider {

  private final Map<String, DatasetDescriptor> descriptors = Maps.newHashMap();
  private final Configuration conf;
  private transient final FileSystem fs;

  public MemoryMetadataProvider(Configuration conf) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");

    this.conf = conf;
    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException ex) {
      throw new DatasetIOException("Could not get default FileSystem", ex);
    }
  }

  public void clear() {
    descriptors.clear();
  }

  protected URI newLocation(String name) {
    return new Path(Files.createTempDir().getAbsolutePath()).toUri();
  }

  @Override
  public DatasetDescriptor load(String name) {
    Preconditions.checkNotNull(name, "Name cannot be null");

    if (descriptors.containsKey(name)) {
      return descriptors.get(name);
    } else {
      throw new DatasetNotFoundException("Missing dataset:" + name);
    }
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(name, "Name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    if (descriptors.containsKey(name)) {
      throw new DatasetExistsException(
          "Dataset already exists for name:" + name);
    }

    DatasetDescriptor newDescriptor;
    if (descriptor.getLocation() == null) {
      newDescriptor = new DatasetDescriptor.Builder(descriptor)
          .location(fs.makeQualified(new Path(newLocation(name))))
          .build();
    } else {
      // don't need to modify it
      newDescriptor = descriptor;
    }

    // save and return
    descriptors.put(name, newDescriptor);
    return newDescriptor;
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(name, "Name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    if (!descriptors.containsKey(name)) {
      throw new DatasetNotFoundException("Missing dataset:" + name);
    }

    descriptors.put(name, descriptor);
    return descriptor;
  }

  @Override
  public boolean delete(String name) {
    Preconditions.checkNotNull(name, "Name cannot be null");

    if (descriptors.containsKey(name)) {
      descriptors.remove(name);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean exists(String name) {
    Preconditions.checkNotNull(name, "Name cannot be null");

    return descriptors.containsKey(name);
  }

  @Override
  public Collection<String> list() {
    return descriptors.keySet();
  }

}
