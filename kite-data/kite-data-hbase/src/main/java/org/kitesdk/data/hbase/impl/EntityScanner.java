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
package org.kitesdk.data.hbase.impl;

import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.spi.InitializeAccessor;

/**
 * A Scanner interface that represents an Iterable that allows us to iterate
 * over entities in an HBase table, returning them as Entity instances.
 * 
 * @param <E>
 *          The type of the entity to return
 */
public interface EntityScanner<E> extends DatasetReader<E>, InitializeAccessor {
}
