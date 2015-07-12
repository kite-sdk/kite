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
package org.kitesdk.data.spi;

import org.kitesdk.data.DatasetReader;

import java.util.Map;

/**
 * SPI to support constraints optimizations on the server side on the {@link DatasetReader}
 *
 * @see Constraints
 * @param <E>
 * @since 1.1.1
 */
public interface ServerSideConstrainableReader<E> extends DatasetReader<E> {
  void applyFilters(Map<String, Object> serverSideFilters);
}
