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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.ValidationException;
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
  private static Pattern hiveCompatible = Pattern
      .compile("[a-zA-Z0-9][a-zA-Z0-9_]*");
  
  //As per the Avro specs mentioned here -http://avro.apache.org/docs/1.7.5/spec.html
  // It should start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_]
  private static Pattern avroCompatible = Pattern.
		  compile("^[A-Za-z_][A-Za-z\\d_]*$");

  /**
   * Checks the name and descriptor for known compatibility issues and warns.
   *
   * If the column names are not compatible across components or if any
   * partition name duplicates its source field name, this will warn the user.
   *
   * @param namespace a String namespace
   * @param name a String dataset name
   * @param descriptor a {@link DatasetDescriptor}
   */
  public static void checkAndWarn(String namespace, String name, DatasetDescriptor descriptor) {
    try {
      checkDatasetName(namespace, name);
      checkDescriptor(descriptor);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage());
    } catch (IllegalStateException e) {
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Checks the name and schema for known compatibility issues and warns.
   *
   * If the column names are not compatible across components, this will warn
   * the user.
   *
   * @param namespace a String namespace
   * @param datasetName a String dataset name
   * @param schema a {@link Schema}
   */
  public static void checkAndWarn(String namespace, String datasetName, Schema schema) {
    try {
      checkDatasetName(namespace, datasetName);
      checkSchema(schema);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage());
    } catch (IllegalStateException e) {
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Precondition-style validation that a dataset name is compatible.
   *
   * @param namespace a String namespace
   * @param name a String name
   */
  public static void checkDatasetName(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkArgument(Compatibility.isCompatibleName(namespace),
        "Deprecated: Namespace %s is not alphanumeric (plus '_')",
        namespace);
    Preconditions.checkArgument(Compatibility.isCompatibleName(name),
        "Deprecated: Dataset name %s is not alphanumeric (plus '_')",
        name);
  }

  /**
   * Precondition-style validation that a {@link Schema} is compatible.
   *
   * @param schema an avro {@code Schema}
   */
  public static void checkSchema(Schema schema) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
    List<String> incompatible = getIncompatibleNames(schema);
    Preconditions.checkState(incompatible.isEmpty(),
        "Deprecated: field names are not alphanumeric (plus '_'): %s",
        Joiner.on(", ").join(incompatible));
  }

  /**
   * Precondition-style validation that the DatasetDescriptor is compatible.
   *
   * @param descriptor a {@link DatasetDescriptor}
   */
  public static void checkDescriptor(DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

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
          "Deprecated: partition names are not alphanumeric (plus '_'): %s",
          Joiner.on(", ").join(incompatible));
      Preconditions.checkState(duplicates.isEmpty(),
          "Deprecated: partition names duplicate data fields: %s",
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
    return hiveCompatible.matcher(name).matches();
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

  /**
   * Returns true if the name does not contain characters that are known to be
   * incompatible with the specs defined in Avro schema.
   *
   * @param name a String field name to check
   * @return will return true if the name is Avro compatible ,false if not
   */
  public static boolean isAvroCompatibleName(String name) {
	  return avroCompatible.matcher(name).matches();
  }

  /**
   * Checks that the {@code existing} {@link DatasetDescriptor} can be replaced
   * by {@code updated}.
   *
   * @param existing the current {@code DatasetDescriptor} for a dataset
   * @param updated a new {@code DatasetDescriptor} for the same dataset
   */
  public static void checkUpdate(DatasetDescriptor existing,
                                 DatasetDescriptor updated) {
    checkNotChanged("location", existing.getLocation(), updated.getLocation());
    checkCompatible(existing, updated);
  }

  /**
   * Checks that the {@code existing} {@link DatasetDescriptor} is compatible
   * with {@code test}.
   *
   * @param existing the current {@code DatasetDescriptor} for a dataset
   * @param test a new {@code DatasetDescriptor} for the same dataset
   */
  public static void checkCompatible(DatasetDescriptor existing,
                                     DatasetDescriptor test) {
    checkNotChanged("format", existing.getFormat(), test.getFormat());

    checkNotChanged("partitioning",
        existing.isPartitioned(), test.isPartitioned());

    if (existing.isPartitioned()) {
      checkNotChanged("partition strategy",
          existing.getPartitionStrategy(), test.getPartitionStrategy());
    }

    // check can read records written with old schema using new schema
    Schema oldSchema = existing.getSchema();
    Schema testSchema = test.getSchema();
    if (!SchemaValidationUtil.canRead(oldSchema, testSchema)) {
      throw new IncompatibleSchemaException("Schema cannot read data " +
          "written using existing schema. Schema: " + testSchema.toString(true) +
          "\nExisting schema: " + oldSchema.toString(true));
    }

  }

  private static void checkNotChanged(String what,
                                      @Nullable Object existing,
                                      @Nullable Object test) {
    ValidationException.check(
        (existing == test) || (existing != null && existing.equals(test)),
        "Dataset %s is not compatible with existing: %s != %s",
        what, String.valueOf(existing), String.valueOf(test));
  }

}
