/**
 * Copyright 2014 Cloudera Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.DatasetDescriptor;

public class DescriptorUtil {

  private static final String KITE_SCHEMA_URI = "kite.%s.schema-uri";
  private static final String KITE_SCHEMA_LITERAL = "kite.%s.schema-literal";
  private static final String KITE_STRATEGY = "kite.%s.partition-strategy";
  private static final String KITE_MAPPING = "kite.%s.column-mapping";
  private static final String KITE_FORMAT = "kite.%s.format";
  private static final String KITE_COMPRESSION = "kite.%s.compression-type";
  private static final String KITE_LOCATION = "kite.%s.location";
  private static final String KITE_PROPERTIES = "kite.%s.properties";

  private static Joiner.MapJoiner PROP_JOINER = Joiner.on(',').withKeyValueSeparator("=");
  private static Splitter.MapSplitter PROP_SPLITTER = Splitter.on(',').withKeyValueSeparator("=");

  /**
   * Returns whether the value of the descriptor property is {@code true}.
   *
   * @param property a String property name
   * @param descriptor a {@link DatasetDescriptor}
   * @return {@code true} if set and "true", {@code false} otherwise.
   */
  public static boolean isEnabled(String property, DatasetDescriptor descriptor) {
    if (descriptor.hasProperty(property)) {
      // return true only if the property value is "true"
      return Boolean.valueOf(descriptor.getProperty(property));
    }
    return false;
  }

  /**
   * Builds a {@link DatasetDescriptor} from the properties set in the given
   * {@link Configuration} that contain the context string.
   * <p>
   * The context string is used to build the property names that store the
   * descriptor data. For example, "out" will use "kite.out.*" names.
   *
   * @param conf a Configuration
   * @param context a String used to build descriptor property names
   * @return a DatasetDescriptor
   * @throws IOException if the Schema URI cannot be opened
   */
  public static DatasetDescriptor buildFromConfiguration(
      Configuration conf, String context) throws IOException {
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();

    String schemaUri = get(conf, KITE_SCHEMA_URI, context);
    if (schemaUri != null) {
      builder.schemaUri(schemaUri);
    } else {
      String schemaLiteral = get(conf, KITE_SCHEMA_LITERAL, context);
      Preconditions.checkNotNull(schemaLiteral,
          "Missing schema: literal or URI is required");
      builder.schemaLiteral(schemaLiteral);
    }
    String strategyLiteral = get(conf, KITE_STRATEGY, context);
    if (strategyLiteral != null) {
      builder.partitionStrategyLiteral(strategyLiteral);
    }
    String mappingLiteral = get(conf, KITE_MAPPING, context);
    if (mappingLiteral != null) {
      builder.columnMappingLiteral(mappingLiteral);
    }
    String outputFileFormat = get(conf, KITE_FORMAT, context);
    if (outputFileFormat != null) {
      builder.format(outputFileFormat);
    }
    String compressionType = get(conf, KITE_COMPRESSION, context);
    if (compressionType != null) {
      builder.compressionType(compressionType);
    }
    String location = get(conf, KITE_LOCATION, context);
    if (location != null) {
      builder.location(location);
    }
    String properties = get(conf, KITE_PROPERTIES, context);
    if (properties != null && !properties.isEmpty()) {
      Map<String, String> propMap = PROP_SPLITTER.split(properties);
      for (Map.Entry<String, String> entry : propMap.entrySet()) {
        builder.property(entry.getKey(), entry.getValue());
      }
    }

    return builder.build();
  }

  /**
   * Stores a {@link DatasetDescriptor} in properties in the given
   * {@link Configuration} using a context string.
   * <p>
   * The context string is used to build the property names that will store the
   * descriptor data. For example, "out" will produce "kite.out.*" names.
   *
   * @param descriptor a DatasetDescriptor to store
   * @param conf a Configuration
   * @param context a String used to build descriptor property names
   */
  public static void addToConfiguration(DatasetDescriptor descriptor,
                                        String context, Configuration conf) {
    if (descriptor.getSchemaUri() != null) {
      set(conf, KITE_SCHEMA_URI, context, descriptor.getSchemaUri());
    } else {
      set(conf, KITE_SCHEMA_LITERAL, context, descriptor.getSchema());
    }
    if (descriptor.isPartitioned()) {
      set(conf, KITE_STRATEGY, context, descriptor.getPartitionStrategy());
    }
    if (descriptor.isColumnMapped()) {
      set(conf, KITE_MAPPING, context, descriptor.getColumnMapping());
    }
    set(conf, KITE_FORMAT, context, descriptor.getFormat().getName());
    set(conf, KITE_COMPRESSION, context, descriptor.getCompressionType().getName());
    set(conf, KITE_LOCATION, context, descriptor.getLocation());
    set(conf, KITE_PROPERTIES, context, PROP_JOINER.join(propertiesAsMap(descriptor)));
  }

  private static Map<String, String> propertiesAsMap(DatasetDescriptor descriptor) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    for (String property : descriptor.listProperties()) {
      properties.put(property, descriptor.getProperty(property));
    }
    return properties.build();
  }

  private static String get(Configuration conf, String property, String context) {
    return conf.get(String.format(property, context));
  }

  private static void set(Configuration conf, String property, String context,
                          Object value) {
    if (value != null) {
      conf.set(String.format(property, context), value.toString());
    }
  }
}
