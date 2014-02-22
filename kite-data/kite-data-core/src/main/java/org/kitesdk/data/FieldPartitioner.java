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

import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * Partitions values for a named field.
 * </p>
 * <p>
 * Used by a {@link PartitionStrategy} to calculate which partition an entity
 * belongs in, based on the value of a given field, called the source field. A field
 * partitioner can, in some cases, provide meaningful cardinality hints to query
 * systems. A good example of this is a hash partitioner which always knows the number of
 * buckets produced by the function.
 * </p>
 * <p>
 * Implementations of {@link FieldPartitioner} are immutable.
 * </p>
 *
 * @param <S> The type of the source field in the entity. The partition function must
 *           accept values of this type.
 * @param <T> The type of the target field, which is the type of the return value of the
 *           partition function.
 * @see PartitionStrategy
 * @see org.kitesdk.data.spi.FieldPartitioner
 * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
 */
@Immutable
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
           "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS"},
    justification="Replaced by parent class")
public abstract class FieldPartitioner<S, T> extends org.kitesdk.data.spi.FieldPartitioner<S, T> {
  protected FieldPartitioner(String name, Class<S> sourceType, Class<T> type,
      int cardinality) {
    super(name, name, sourceType, type, cardinality);
  }

  protected FieldPartitioner(String sourceName, String name,
      Class<S> sourceType, Class<T> type, int cardinality) {
    super(sourceName, name, sourceType, type, cardinality);
  }
}
