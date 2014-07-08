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

import edu.umd.cs.findbugs.annotations.*;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * A key for retrieving partitions from a {@link Dataset}.
 * </p>
 * <p>
 * A {@code PartitionKey} is an ordered sequence of values corresponding to the
 * {@link org.kitesdk.data.spi.FieldPartitioner}s in a
 * {@link PartitionStrategy}. You can obtain a {@link org.kitesdk.data.PartitionKey} using
 * {@link PartitionStrategy#partitionKey(Object...)} or
 * {@link PartitionStrategy#partitionKeyForEntity(Object)}.
 * </p>
 * <p>
 * Implementations of {@link org.kitesdk.data.PartitionKey} are typically not thread-safe; that
 * is, the behavior when accessing a single instance from multiple threads is
 * undefined.
 * </p>
 *
 * @see PartitionStrategy
 * @see org.kitesdk.data.spi.FieldPartitioner
 * @see Dataset
 *
* @deprecated will be removed in 0.16.0; use {@link org.kitesdk.data.RefinableView}
* methods instead
 */
@NotThreadSafe
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification="Part of deprecation process.")
public class PartitionKey extends org.kitesdk.data.spi.PartitionKey {

  PartitionKey(Object... values) {
    super(values);
  }

  PartitionKey(int size) {
    this(new Object[size]);
  }

  protected void set(int index, Object value) {
    super.set(index, value);
  }
}
