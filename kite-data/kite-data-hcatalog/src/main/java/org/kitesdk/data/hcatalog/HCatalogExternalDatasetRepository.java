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
package org.kitesdk.data.hcatalog;

import java.net.URI;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.MetadataProvider;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
import org.kitesdk.data.spi.AbstractDatasetRepository;

class HCatalogExternalDatasetRepository extends AbstractDatasetRepository {

  private final FileSystemDatasetRepository fsRepository;
  private final URI repositoryUri;

  /**
   * Create an HCatalog dataset repository with external tables.
   */
  HCatalogExternalDatasetRepository(Configuration conf, MetadataProvider provider, URI repositoryUri) {
    this.fsRepository = new FileSystemDatasetRepository.Builder().configuration(conf)
        .metadataProvider(provider).build();
    this.repositoryUri = repositoryUri;
  }

  @Override
  public <E> Dataset<E> create(String name, DatasetDescriptor descriptor) {
    return fsRepository.create(name, descriptor);
  }

  @Override
  public <E> Dataset<E> update(String name, DatasetDescriptor descriptor) {
    return fsRepository.update(name, descriptor);
  }

  @Override
  public <E> Dataset<E> load(String name) {
    return fsRepository.load(name);
  }

  @Override
  public boolean delete(String name) {
    return fsRepository.delete(name);
  }

  @Override
  public boolean exists(String name) {
    return fsRepository.exists(name);
  }

  @Override
  public Collection<String> list() {
    return fsRepository.list();
  }

  @Override
  public URI getUri() {
    return repositoryUri;
  }

}
