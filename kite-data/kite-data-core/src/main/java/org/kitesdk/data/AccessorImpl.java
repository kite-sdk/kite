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
package org.kitesdk.data;

import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.URIPattern;

final class AccessorImpl extends Accessor {

  @Override
  public Format newFormat(String name) {
    return new Format(name);
  }
  @Override
  public PartitionKey newPartitionKey(Object... values) {
    return new PartitionKey(values);
  }

  @Override
  @SuppressWarnings("deprecation")
  public PartitionStrategy getSubpartitionStrategy(PartitionStrategy partitionStrategy, int startIndex) {
    return partitionStrategy.getSubpartitionStrategy(startIndex);
  }

  @Override
  public String toExpression(PartitionStrategy partitionStrategy) {
    return PartitionExpression.toExpression(partitionStrategy);
  }

  @Override
  public PartitionStrategy fromExpression(String partitionExpression) {
    return new PartitionExpression(partitionExpression, true).evaluate();
  }

  @Override
  public void registerDatasetRepository(URIPattern pattern, OptionBuilder<DatasetRepository> builder) {
    DatasetRepositories.register(pattern, builder);
  }

  /**
   * Returns the given {@link DatasetException} wrapped in a deprecated
   * {@link MetadataProviderException}. Users should switch to catching the
   * wrapped exception.
   *
   * This is located here to avoid build failures when importing
   * {@code MetadataProviderException} and should be removed in 0.14.0 with
   * {@code MetadataProviderException}.
   *
   * @param e a {@code DatasetException} to wrap and throw
   */
  @SuppressWarnings("deprecation")
  public MetadataProviderException providerExceptionFor(DatasetException e) {
    return new MetadataProviderException(e);
  }
}
