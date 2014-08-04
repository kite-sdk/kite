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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.partition.DayOfMonthFieldPartitioner;
import org.kitesdk.data.spi.partition.HourFieldPartitioner;
import org.kitesdk.data.spi.partition.MinuteFieldPartitioner;
import org.kitesdk.data.spi.partition.MonthFieldPartitioner;
import org.kitesdk.data.spi.partition.PartitionFunctions;
import java.util.List;

import java.util.Locale;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.partition.HashFieldPartitioner;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.kitesdk.data.spi.partition.IntRangeFieldPartitioner;
import org.kitesdk.data.spi.partition.RangeFieldPartitioner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.partition.YearFieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The strategy used to determine how a dataset is partitioned.
 * </p>
 * <p>
 * A {@code PartitionStrategy} is configured with one or more
 * {@link FieldPartitioner}s upon creation. When a {@link Dataset} is configured
 * with a partition strategy, that data is considered partitioned. Any entities
 * written to a partitioned dataset are evaluated with its
 * {@code PartitionStrategy} to determine which partition to write to.
 * </p>
 * <p>
 * You should use the inner {@link Builder} to create new instances.
 * </p>
 * 
 * @see FieldPartitioner
 * @see DatasetDescriptor
 * @see Dataset
 */
@Immutable
@SuppressWarnings("deprecation")
public class PartitionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStrategy.class);

  private final List<FieldPartitioner> fieldPartitioners;
  private final Map<String, FieldPartitioner> partitionerMap;

  static {
    Accessor.setDefault(new AccessorImpl());
  }

  /**
   * Construct a partition strategy with a list of field partitioners.
   */
  PartitionStrategy(List<FieldPartitioner> partitioners) {
    this.fieldPartitioners = ImmutableList.copyOf(partitioners);
    ImmutableMap.Builder<String, FieldPartitioner> mapBuilder =
        ImmutableMap.builder();
    for (FieldPartitioner fp : partitioners) {
      mapBuilder.put(fp.getName(), fp);
    }
    this.partitionerMap = mapBuilder.build();
  }

  /**
   * <p>
   * Get the list of field partitioners used for partitioning.
   * </p>
   * <p>
   * {@link FieldPartitioner}s are returned in the same order they are used
   * during partition selection.
   * </p>
   */
  public List<FieldPartitioner> getFieldPartitioners() {
    return fieldPartitioners;
  }

  /**
   * Get a partitioner by partition name.
   * @return a FieldPartitioner with the given partition name
   * @since 0.15.0
   */
  public FieldPartitioner getPartitioner(String name) {
    return partitionerMap.get(name);
  }

  /**
   * Check if a partitioner for the partition name exists.
   * @return {@code true} if this strategy has a partitioner for the name
   * @since 0.15.0
   */
  public boolean hasPartitioner(String name) {
    return partitionerMap.containsKey(name);
  }

  /**
   * <p>
   * Return the cardinality produced by the contained field partitioners.
   * </p>
   * <p>
   * This can be used to aid in calculating resource usage during certain
   * operations. For example, when writing data to a partitioned dataset, you
   * can use this method to estimate (or discover exactly, depending on the
   * partition functions) how many leaf partitions exist.
   * </p>
   * <p>
   * <strong>Warning:</strong> This method is allowed to lie and should be
   * treated only as a hint. Some partition functions are fixed (for example, 
   * hash modulo number of buckets), while others are open-ended (for
   * example, discrete value) and depend on the input data.
   * </p>
   * 
   * @return The estimated (or possibly concrete) number of leaf partitions.
   */
  public int getCardinality() {
    int cardinality = 1;
    for (FieldPartitioner fieldPartitioner : fieldPartitioners) {
      if (fieldPartitioner.getCardinality() == FieldPartitioner.UNKNOWN_CARDINALITY) {
        return FieldPartitioner.UNKNOWN_CARDINALITY;
      }
      cardinality *= fieldPartitioner.getCardinality();
    }
    return cardinality;
  }

  /**
   * Return a {@link PartitionStrategy} for subpartitions starting at the given
   * index.
   */
  PartitionStrategy getSubpartitionStrategy(int startIndex) {
    if (startIndex == 0) {
      return this;
    }
    if (startIndex >= fieldPartitioners.size()) {
      return null;
    }
    return new PartitionStrategy(fieldPartitioners.subList(startIndex,
        fieldPartitioners.size()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    PartitionStrategy that = (PartitionStrategy) o;
    return Objects.equal(this.fieldPartitioners, that.fieldPartitioners);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldPartitioners);
  }

  @Override
  public String toString() {
    return PartitionStrategyParser.toString(this, false);
  }

  /**
   * @param pretty {@code true} to indent and format JSON
   * @return this PartitionStrategy as its JSON representation
   */
  public String toString(boolean pretty) {
    return PartitionStrategyParser.toString(this, pretty);
  }

  /**
   * A fluent builder to aid in the construction of {@link PartitionStrategy}s.
   */
  public static class Builder {

    private final List<FieldPartitioner> fieldPartitioners = Lists.newArrayList();
    private final Set<String> names = Sets.newHashSet();

    /**
     * Configure a hash partitioner with the specified number of
     * {@code buckets}.
     *
     * The partition name is the source field name with a "_hash" suffix.
     * For example, hash("color", 34) creates "color_hash" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param buckets
     *          The number of buckets into which data is to be partitioned.
     * @return An instance of the builder for method chaining.
     */
    public Builder hash(String sourceName, int buckets) {
      add(new HashFieldPartitioner(sourceName, buckets));
      return this;
    }

    /**
     * Configure a hash partitioner with the specified number of
     * {@code buckets}. If name is null, the partition name will be the source
     * field name with a "_hash" suffix. For example, hash("color", null, 34)
     * will create "color_hash" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @param buckets
     *          The number of buckets into which data is to be partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder hash(String sourceName, @Nullable String name, int buckets) {
      add(new HashFieldPartitioner(sourceName, name, buckets));
      return this;
    }

    /**
     * Configure an identity partitioner.
     *
     * The partition name is the source field name with a "_copy" suffix.
     * For example, identity("color", String.class, 34) creates "color_copy"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @see IdentityFieldPartitioner
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName) {
      add(new IdentityFieldPartitioner(sourceName, Object.class));
      return this;
    }

    /**
     * Configure an identity partitioner. If name is null, the partition name
     * will be the source field name with a "_copy" suffix. For example,
     * identity("color", null, ...) will create "color_copy" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          A name for the partition field
     * @return An instance of the builder for method chaining.
     * @see IdentityFieldPartitioner
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName, String name) {
      add(new IdentityFieldPartitioner(sourceName, name, Object.class));
      return this;
    }

    /**
     * Configure an identity partitioner with a cardinality hint of
     * {@code cardinalityHint}.
     *
     * The partition name is the source field name with a "_copy" suffix.
     * For example, identity("color", String.class, 34) creates "color_copy"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param cardinalityHint
     *          A hint as to the number of partitions that will be created (i.e.
     *          the number of discrete values for the field {@code name} in the
     *          data).
     * @return An instance of the builder for method chaining.
     * @see IdentityFieldPartitioner
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName, int cardinalityHint) {
      add(new IdentityFieldPartitioner(sourceName, Object.class, cardinalityHint));
      return this;
    }

    /**
     * Configure an identity partitioner with a cardinality hint of
     * {@code cardinalityHint}. If name is null, the partition name will be the source
     * field name with a "_copy" suffix. For example, identity("color", null, ...)
     * will create "color_copy" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          A name for the partition field
     * @param cardinalityHint
     *          A hint as to the number of partitions that will be created (i.e.
     *          the number of discrete values for the field {@code name} in the
     *          data).
     * @return An instance of the builder for method chaining.
     * @see IdentityFieldPartitioner
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName, String name, int cardinalityHint) {
      add(new IdentityFieldPartitioner(sourceName, name, Object.class, cardinalityHint));
      return this;
    }

    /**
     * Configure a range partitioner with a set of {@code upperBounds}.
     *
     * The partition name will be the source field name with a "_bound" suffix.
     * For example, range("number", 5, 10) creates "number_bound"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param upperBounds
     *          A variadic list of upper bounds of each partition.
     * @return An instance of the builder for method chaining.
     * @see IntRangeFieldPartitioner
     */
    public Builder range(String sourceName, int... upperBounds) {
      add(new IntRangeFieldPartitioner(sourceName, upperBounds));
      return this;
    }

    /**
     * Configure a range partitioner for strings with a set of {@code upperBounds}.
     *
     * The partition name will be the source field name with a "_bound" suffix.
     * For example, range("color", "blue", "green") creates "color_bound"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param upperBounds
     *          A variadic list of upper bounds of each partition.
     * @return An instance of the builder for method chaining.
     */
    public Builder range(String sourceName, String... upperBounds) {
      add(new RangeFieldPartitioner(sourceName, upperBounds));
      return this;
    }

    /**
     * Configure a partitioner for extracting the year from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "year".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder year(String sourceName, @Nullable String name) {
      add(new YearFieldPartitioner(sourceName, name));
      return this;
    }

    /**
     * Configure a partitioner for extracting the year from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "year".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder year(String sourceName) {
      add(new YearFieldPartitioner(sourceName));
      return this;
    }

    /**
     * Configure a partitioner for extracting the month from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "month".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder month(String sourceName, @Nullable String name) {
      add(new MonthFieldPartitioner(sourceName, name));
      return this;
    }

    /**
     * Configure a partitioner for extracting the month from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "month".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder month(String sourceName) {
      add(new MonthFieldPartitioner(sourceName));
      return this;
    }

    /**
     * Configure a partitioner for extracting the day from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "day".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder day(String sourceName, @Nullable String name) {
      add(new DayOfMonthFieldPartitioner(sourceName, name));
      return this;
    }

    /**
     * Configure a partitioner for extracting the day from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "day".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder day(String sourceName) {
      add(new DayOfMonthFieldPartitioner(sourceName));
      return this;
    }

    /**
     * Configure a partitioner for extracting the hour from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "hour".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder hour(String sourceName, @Nullable String name) {
      add(new HourFieldPartitioner(sourceName, name));
      return this;
    }

    /**
     * Configure a partitioner for extracting the hour from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "hour".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder hour(String sourceName) {
      add(new HourFieldPartitioner(sourceName));
      return this;
    }

    /**
     * Configure a partitioner for extracting the minute from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "minute".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder minute(String sourceName, @Nullable String name) {
      add(new MinuteFieldPartitioner(sourceName, name));
      return this;
    }

    /**
     * Configure a partitioner for extracting the minute from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "minute".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder minute(String sourceName) {
      add(new MinuteFieldPartitioner(sourceName));
      return this;
    }

    /**
     * Configure a partitioner that applies a custom date format to a timestamp
     * field. The UTC timezone is assumed.
     *
     * @param sourceName
     *          The entity field name of the timestamp to format
     * @param name
     *          A name for the partitions created by the format (e.g. "day")
     * @param format
     *          A {@link java.text.SimpleDateFormat} format-string.
     * @return This builder for method chaining.
     * @since 0.9.0
     */
    public Builder dateFormat(String sourceName, String name, String format) {
      add(PartitionFunctions.dateFormat(sourceName, name, format));
      return this;
    }

    /**
     * Build a configured {@link PartitionStrategy} instance.
     *
     * This builder should be considered single use and discarded after a call
     * to this method.
     *
     * @return The configured instance of {@link PartitionStrategy}.
     * @since 0.9.0
     */
    public PartitionStrategy build() {
      return new PartitionStrategy(fieldPartitioners);
    }

    private void add(FieldPartitioner fp) {
      // in 0.14.0, change to a Precondition
      //Preconditions.checkState(!names.contains(fp.getName()),
      //    "Partition name conflicts with an existing field or partition name");
      if (names.contains(fp.getName())) {
        LOG.warn(
            "Partition name conflicts with an existing partition name");
      }
      fieldPartitioners.add(fp);
      names.add(fp.getName());
    }
  }

}
