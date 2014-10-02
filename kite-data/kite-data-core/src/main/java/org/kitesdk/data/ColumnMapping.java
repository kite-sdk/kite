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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.ColumnMappingParser;

/**
 * A descriptor for an entity's column mappings, which defines how an entity
 * maps to a columnar store.
 *
 * @since 0.14.0
 */
@Immutable
public class ColumnMapping {

  private final Collection<FieldMapping> fieldMappings;

  private ColumnMapping(Collection<FieldMapping> mappings) {
    fieldMappings = ImmutableList.copyOf(mappings);
  }

  public Collection<FieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  public FieldMapping getFieldMapping(String fieldName) {
    for (FieldMapping fm : fieldMappings) {
      if (fm.getFieldName().equals(fieldName)) {
        return fm;
      }
    }
    return null;
  }

  /**
   * Get the columns required by this schema.
   *
   * @return The set of columns
   */
  public Set<String> getRequiredColumns() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping fieldMapping : fieldMappings) {
      if (FieldMapping.MappingType.KEY == fieldMapping.getMappingType()) {
        continue;
      } else if (FieldMapping.MappingType.KEY_AS_COLUMN == fieldMapping.getMappingType()) {
        set.add(fieldMapping.getFamilyAsString() + ":");
      } else {
        set.add(fieldMapping.getFamilyAsString() + ":"
            + fieldMapping.getQualifierAsString());
      }
    }
    return set;
  }

  /**
   * Get the column families required by this schema.
   *
   * @return The set of column families.
   */
  public Set<String> getRequiredColumnFamilies() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping mapping : fieldMappings) {
      if (FieldMapping.MappingType.KEY != mapping.getMappingType())
      set.add(mapping.getFamilyAsString());
    }
    return set;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
      justification="Default annotation is not correct for equals")
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnMapping that = (ColumnMapping) o;
    return Objects.equal(fieldMappings, that.fieldMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldMappings.hashCode());
  }

  @Override
  public String toString() {
    return ColumnMappingParser.toString(this, false);
  }

  public String toString(boolean pretty) {
    return ColumnMappingParser.toString(this, pretty);
  }

  /**
   * A fluent builder to aid in constructing a {@link ColumnMapping}.
   *
   * @since 0.14.0
   */
  public static class Builder {
    boolean hasOCCVersion = false;
    boolean hasCounter = false;

    private Collection<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();

    /**
     * Adds a mapping to store the record field {@code name} in the record key.
     *
     * The underlying dataset's {@link PartitionStrategy} must have an identity
     * partitioner for the record field {@code name} so that the value can be
     * recovered.
     *
     * @param name The name of a record field
     * @return This Builder for method chaining
     */
    public Builder key(String name) {
      addField(FieldMapping.key(name));
      return this;
    }

    /**
     * Adds a mapping to store the record field {@code name} in a column using
     * the {@code family} and {@code qualifier}.
     *
     * @param name The name of a record field
     * @param family The column family for storing the record field value
     * @param qualifier The column qualifier for storing the record field value
     * @return This Builder for method chaining
     */
    public Builder column(String name, String family, String qualifier) {
      addField(FieldMapping.column(name, family, qualifier));
      return this;
    }

    /**
     * Adds a key-as-column mapping to store the record field {@code name} in
     * the given {@code family}, using its keys or fields as column qualifiers.
     *
     * The record field must be a map or a record.
     *
     * @param name The name of a record field
     * @param family The column family for storing the map or record values
     * @return This Builder for method chaining
     */
    public Builder keyAsColumn(String name, String family) {
      addField(FieldMapping.keyAsColumn(name, family));
      return this;
    }

    /**
     * Adds a key-as-column mapping to store the record field {@code name} in
     * the given {@code family}, using column qualifiers built from its keys or
     * field names appended to the {@code qualifierPrefix}.
     *
     * The record field must be a map or a record.
     *
     * @param name The name of a record field
     * @param family The column family for storing the map or record values
     * @param qualifierPrefix A prefix to add when building column qualifiers
     * @return This Builder for method chaining
     */
    public Builder keyAsColumn(String name, String family, String qualifierPrefix) {
      addField(FieldMapping.keyAsColumn(name, family, qualifierPrefix));
      return this;
    }

    /**
     * Adds a counter mapping to store record field {@code name} in a column
     * using the {@code family} and {@code qualifier}. The record field can be
     * updated atomically using
     * {@link RandomAccessDataset#increment(Key, String, long)}
     *
     * The record field must be an int or a long.
     *
     * Counters cannot be used in combination with optimistic concurrency (OCC).
     *
     * @param name The name of a record field
     * @param family The column family for storing the record counter value
     * @param qualifier The column qualifier for storing the record field value
     * @return This Builder for method chaining
     */
    public Builder counter(String name, String family, String qualifier) {
      addField(FieldMapping.counter(name, family, qualifier));
      return this;
    }

    /**
     * Adds an OCC version mapping for the record field {@code name}. The
     * record field will contain the current version number of the record.
     *
     * Using this mapping enables optimistic concurrency (OCC), where the
     * underlying {@link RandomAccessDataset} will only persist changes to a
     * record if it has not changed by another process.
     *
     * The record field must be an int or a long.
     *
     * Optimistic concurrency (OCC) cannot be used in combination with counters.
     *
     * @param name The name of a record field to use for an OCC version
     * @return This Builder for method chaining
     */
    public Builder occ(String name) {
      addField(FieldMapping.occ(name));
      return this;
    }

    /**
     * Adds an OCC version mapping for the record field {@code name}. The
     * record field will contain the current version number of the record.
     *
     * Using this mapping enables optimistic concurrency (OCC), where the
     * underlying {@link RandomAccessDataset} will only persist changes to a
     * record if it has not changed by another process.
     *
     * The record field must be an int or a long.
     *
     * Optimistic concurrency (OCC) cannot be used in combination with counters.
     *
     * @param name The name of a record field to use for an OCC version
     * @return This Builder for method chaining
     */
    public Builder version(String name) {
      return occ(name);
    }

    /**
     * Adds a {@link FieldMapping}.
     *
     * @param fieldMapping A {@code FieldMapping}
     * @return This Builder for method chaining
     */
    public Builder fieldMapping(FieldMapping fieldMapping) {
      addField(fieldMapping);
      return this;
    }

    /**
     * Adds each {@link FieldMapping} from a collection.
     *
     * @param fieldMappings A collection of {@code FieldMapping} objects
     * @return This Builder for method chaining
     */
    public Builder fieldMappings(Collection<FieldMapping> fieldMappings) {
      for (FieldMapping fieldMapping : fieldMappings) {
        addField(fieldMapping);
      }
      return this;
    }

    /**
     * Builds and returns a {@link ColumnMapping} from the fields
     * mappings added to this builder.
     *
     * @return a ColumnMapping
     */
    public ColumnMapping build() {
      return new ColumnMapping(fieldMappings);
    }

    /**
     * Validate that a {@link FieldMapping} is compatible with this builder's
     * current set of mappings and add it to the set of mappings.
     *
     * A mapping is not compatible if it results in:
     * <pre>
     * 1. Multiple occVersion mappings in the mapping set
     * 2. Both a counter and an occVersion mapping in the mapping set
     * </pre>
     *
     * @param fm a {@code FieldMapping} to add to this builder
     */
    private void addField(FieldMapping fm) {
      // validate!
      if (fm.getMappingType() == FieldMapping.MappingType.OCC_VERSION) {
        ValidationException.check(!hasOCCVersion,
            "Cannot use multiple occVersion fields");
        ValidationException.check(!hasCounter,
            "Cannot use both counter and occVersion fields");
        hasOCCVersion = true;
      } else if (fm.getMappingType() == FieldMapping.MappingType.COUNTER) {
        ValidationException.check(!hasOCCVersion,
            "Cannot use both counter and occVersion fields");
        hasCounter = true;
      }
      fieldMappings.add(fm);
    }
  }

}
