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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import java.util.Comparator;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Partitions values for a named field.
 * </p>
 * <p>
 * Used by a {@link org.kitesdk.data.PartitionStrategy} to calculate which partition an entity
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
 * @see org.kitesdk.data.PartitionStrategy
 */
@Immutable
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
public abstract class FieldPartitioner<S, T> implements Function<S, T>, Comparator<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FieldPartitioner.class);

  public static final int UNKNOWN_CARDINALITY = -1;

  private final String sourceName;
  private final String name;
  private final Class<S> sourceType;
  private final Class<T> type;
  private final int cardinality;

  protected FieldPartitioner(String sourceName, String name,
                             Class<S> sourceType, Class<T> type) {
    this(sourceName, name, sourceType, type, UNKNOWN_CARDINALITY);
  }

  protected FieldPartitioner(String sourceName, String name,
      Class<S> sourceType, Class<T> type, int cardinality) {
    this.sourceName = sourceName;
    this.name = name;
    this.sourceType = sourceType;
    this.type = type;
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
   * @since 0.3.0
   */
  public String getSourceName() {
    return sourceName;
  }

  /**
   * @return the number of buckets in the partition.
   */
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
  public abstract T apply(S value);

  /**
   * <p>
   * The type of the source field, which is the type of the type expected by
   * the apply function.
   * </p>
   * @since 0.8.0
   */
  public Class<S> getSourceType() {
    return sourceType;
  }

  /**
   * <p>
   * The type of the target field, which is the type of the return value of the
   * partition function.
   * </p>
   * @since 0.8.0
   */
  public Class<T> getType() {
    return type;
  }

  /**
   * <p>
   * The type of the target field, which is the type of the return value of the
   * partition function.
   * </p>
   * @since 0.8.0
   */
  public Class<? extends T> getType(Class<? extends S> sourceType) {
    return getType();
  }

  /**
   * Projects a source-data constraint {@link Predicate} into the image of the
   * backing partition function.
   *
   * If the function cannot be projected exactly, the resulting predicate must
   * be more permissive than the original predicate.
   *
   * @since 0.11.0
   */
  public abstract Predicate<T> project(Predicate<S> predicate);

  /**
   * Projects a source-data constraint {@link Predicate} into the image of the
   * backing partition function.
   *
   * If the function cannot be projected exactly, the resulting predicate must
   * only accept a value if the original predicate would have.
   *
   * If this method returns null, then there are no values in the image of the
   * partition function that guarantee the original Predicate is satisfied.
   *
   * @param predicate a Predicate for this function's source domain
   * @return a strict Predicate in this function's target domain or
   *         null if the Predicate cannot be projected.
   * @since 0.12.0
   */
  public abstract Predicate<T> projectStrict(Predicate<S> predicate);
}
