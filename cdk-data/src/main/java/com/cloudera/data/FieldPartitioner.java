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
package com.cloudera.data;

import com.google.common.base.Function;
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
 * @see PartitionStrategy
 */
@Immutable
public abstract class FieldPartitioner implements Function<Object, Object> {

  private final String sourceName;
  private final String name;
  private final int cardinality;

  protected FieldPartitioner(String name, int cardinality) {
    this(name, name, cardinality);
  }

  protected FieldPartitioner(String sourceName, String name, int cardinality) {
    this.sourceName = sourceName;
    this.name = name;
    this.cardinality = cardinality;
  }

  /**
   * @return the name of the partition field. Note that the partition field is derived
   * from {@link #getSourceName()} and does not appear in the dataset entity.
   */
  public String getName() {
    return name;
  }

  /**
   * @return the name of the field from which the partition field is derived.
   */
  public String getSourceName() {
    return sourceName;
  }

  public int getCardinality() {
    return cardinality;
  }

  /**
   * <p>
   * Apply the partition function to the given {@code value}.
   * </p>
   * <p>
   * The type of value must be compatible with the field partitioner
   * implementation. Normally, this is validated at the time of initial
   * configuration rather than at runtime.
   * </p>
   */
  @Override
  public abstract Object apply(Object value);

  /**
   * <p>
   * Retrieve the value for the field from the string representation.
   * </p>
   */
  public abstract Object valueFromString(String stringValue);

}
