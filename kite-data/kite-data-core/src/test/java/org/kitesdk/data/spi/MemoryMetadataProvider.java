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

import com.google.common.collect.ImmutableList;
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

  private final Map<String, Map<String, DatasetDescriptor>> descriptors =
      Maps.newHashMap();
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
  public DatasetDescriptor load(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Name cannot be null");

    if (descriptors.containsKey(namespace)) {
      Map<String, DatasetDescriptor> datasets = descriptors.get(namespace);
      if (datasets.containsKey(name)) {
        return datasets.get(name);
      }
    }
    throw new DatasetNotFoundException(
        "Missing dataset:" + namespace + ":" + name);
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    if (exists(namespace, name)) {
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
    if (!descriptors.containsKey(namespace)) {
      descriptors.put(namespace, Maps.<String, DatasetDescriptor>newHashMap());
    }
    Map<String, DatasetDescriptor> datasets = descriptors.get(namespace);
    datasets.put(name, newDescriptor);

    return newDescriptor;
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    if (!exists(namespace, name)) {
      throw new DatasetNotFoundException("Missing dataset:" + name);
    }

    descriptors.get(namespace).put(name, descriptor);

    return descriptor;
  }

  @Override
  public boolean delete(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Name cannot be null");

    if (descriptors.containsKey(namespace)) {
      Map<String, DatasetDescriptor> datasets = descriptors.get(namespace);
      if (datasets.containsKey(name)) {
        datasets.remove(name);
        if (datasets.isEmpty()) {
          descriptors.remove(namespace);
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean exists(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Name cannot be null");

    return (descriptors.containsKey(namespace) &&
        descriptors.get(namespace).containsKey(name));
  }

  @Override
  public Collection<String> namespaces() {
    return descriptors.keySet();
  }

  @Override
  public Collection<String> datasets(String namespace) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    if (descriptors.containsKey(namespace)) {
      return descriptors.get(namespace).keySet();
    }
    return ImmutableList.of();
  }

}
