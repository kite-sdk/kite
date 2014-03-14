/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods for checking compatibility.
 */
public abstract class Compatibility {

  private static final Logger LOG = LoggerFactory.getLogger(Compatibility.class);

  // https://github.com/apache/hive/blob/trunk/ql/src/java/org/apache/hadoop/hive/ql/parse/HiveLexer.g#L426
  // The above reference includes a comment that states column names can be
  // quoted strings with any character, but this is not the case in practice.
  private static CharMatcher TABLE_NAME_START = CharMatcher.DIGIT
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.inRange('A', 'Z'));
  private static CharMatcher TABLE_NAME_CHARS = TABLE_NAME_START
      .or(CharMatcher.is('_'));

  /**
   * Checks the name and descriptor for known compatibility issues and warns.
   *
   * If the column names are not compatible across components or if any
   * partition name duplicates its source field name, this will warn the user.
   *
   * @param datasetName a String dataset name
   * @param descriptor a {@link DatasetDescriptor}
   */
  public static void checkAndWarn(String datasetName, DatasetDescriptor descriptor) {
    try {
      checkDatasetName(datasetName);
      checkDescriptor(descriptor);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage());
    } catch (IllegalStateException e) {
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Precondition-style validation that a dataset name is compatible.
   *
   * @param name a String name
   */
  public static void checkDatasetName(String name) {
    Preconditions.checkArgument(name != null, "Dataset name cannot be null");
    Preconditions.checkArgument(Compatibility.isCompatibleName(name),
        "Hive incompatible: Dataset name {} is not alphanumeric (plus '_')",
        name);
  }

  /**
   * Precondition-style validation that a {@link Schema} is compatible.
   *
   * @param schema an avro {@code Schema}
   */
  public static void checkSchema(Schema schema) {
    Preconditions.checkState(schema != null,
        "Descriptor schema is required and cannot be null");
    List<String> incompatible = getIncompatibleNames(schema);
    Preconditions.checkState(incompatible.isEmpty(),
        "Hive incompatible: field names are not alphanumeric (plus '_'): {}",
        Joiner.on(", ").join(incompatible));
  }

  /**
   * Precondition-style validation that the DatasetDescriptor is compatible.
   *
   * @param descriptor a {@link DatasetDescriptor}
   */
  public static void checkDescriptor(DatasetDescriptor descriptor) {
    Preconditions.checkArgument(descriptor != null,
        "Descriptor cannot be null");

    Schema schema = descriptor.getSchema();
    checkSchema(schema);

    if (descriptor.isPartitioned()) {
      // marked as [BUG] because this is checked in DatasetDescriptor
      Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD,
          "[BUG] Partitioned datasets must have record schemas");

      Set<String> names = Sets.newHashSet();
      for (Schema.Field field : schema.getFields()) {
        names.add(field.name());
      }

      List<String> incompatible = Lists.newArrayList();
      List<String> duplicates = Lists.newArrayList();
      for (FieldPartitioner fp :
          descriptor.getPartitionStrategy().getFieldPartitioners()) {
        String name = fp.getName();
        if (!isCompatibleName(name)) {
          incompatible.add(name);
        } else if (names.contains(name)) {
          duplicates.add(name);
        } else {
          names.add(name);
        }
      }
      Preconditions.checkState(incompatible.isEmpty(),
          "Hive incompatible: partition names are not alphanumeric (plus '_'): {}",
          Joiner.on(", ").join(incompatible));
      Preconditions.checkState(duplicates.isEmpty(),
          "Hive incompatible: partition names duplicate data fields: {}",
          Joiner.on(", ").join(duplicates));
    }
  }

  /**
   * Returns true if the name does not contain characters that are known to be
   * incompatible with some projects, such as Hive or HBase.
   *
   * @param name a String name to check
   * @return true if the name is compatible, false if known to be incompatible
   */
  public static boolean isCompatibleName(String name) {
    return (TABLE_NAME_START.matches(name.charAt(0)) &&
        TABLE_NAME_CHARS.matchesAllOf(name));
  }

  /**
   * Returns a list of field names from the schema that contain characters that
   * are known to be incompatible with some projects, such as Hive or HBase.
   *
   * @param schema a {@link org.apache.avro.Schema} to check
   * @return a {@link java.util.List} of incompatible field names
   */
  private static List<String> getIncompatibleNames(Schema schema) {
    NameValidation validation = new NameValidation();
    SchemaUtil.visit(schema, validation);
    return validation.getIncompatibleNames();
  }

  /**
   * Schema visitor used to check compatibility of nested record field names.
   */
  private static class NameValidation extends SchemaUtil.SchemaVisitor<Void> {
    private List<String> incompatible = Lists.newArrayList();

    @Override
    public Void record(Schema record, List<String> names, List<Void> fields) {
      String recordName = record.getName();
      for (String name : names) {
        if (!isCompatibleName(name)) {
          incompatible.add(recordName + "." + name);
        }
      }
      return null;
    }

    public List<String> getIncompatibleNames() {
      return incompatible;
    }
  }
}
