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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.Schema;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.ValidationException;

/**
 * Parser for {@link ColumnMapping}. Will parse the mapping annotation from
 * Avro schemas, and will parse the ColumnMapping JSON format. An
 * example of that is:
 *
 * <pre>
 * [
 *   { "source": "field1", "type": "column", "value": "cf:field1" },
 *   { "source": "field2", "type": "keyAsColumn", "value": "kac:" },
 *   { "source": "field3", "type": "occVersion" }
 * ]
 * </pre>
 *
 */
public class ColumnMappingParser {

  // name of the json node when embedded in a schema
  private static final String MAPPING = "mapping";

  // property constants
  private static final String TYPE = "type";
  private static final String SOURCE = "source";
  private static final String FAMILY = "family";
  private static final String QUALIFIER = "qualifier";
  private static final String PREFIX = "prefix";
  private static final String VALUE = "value";

  private static final Splitter VALUE_SPLITTER = Splitter.on(":").limit(2);

  /**
   * Parses the Mapping Descriptor as a JSON string.
   * 
   * @param mappingDescriptor
   *          The mapping descriptor as a JSON string
   * @return ColumnMapping
   */
  public static ColumnMapping parse(String mappingDescriptor) {
    return buildColumnMapping(JsonUtil.parse(mappingDescriptor));
  }

  /**
   * Parses the Mapping Descriptor from a File
   *
   * @param file
   *          The File that contains the Mapping Descriptor in JSON format.
   * @return ColumnMapping.
   */
  public static ColumnMapping parse(File file) {
    return buildColumnMapping(JsonUtil.parse(file));
  }

  /**
   * Parses the Mapping Descriptor from an input stream
   *
   * @param in
   *          The input stream that contains the Mapping Descriptor in JSON
   *          format.
   * @return ColumnMapping.
   */
  public static ColumnMapping parse(InputStream in) {
    return buildColumnMapping(JsonUtil.parse(in));
  }

  public static boolean hasEmbeddedColumnMapping(Schema schema) {
    return schema.getJsonProp(MAPPING) != null;
  }

  public static Schema removeEmbeddedMapping(Schema schema) {
    // TODO: avoid embedding mappings in the schema
    // Avro considers Props read-only and uses an older Jackson version
    // Parse the Schema as a String because Avro uses com.codehaus.jackson
    ObjectNode schemaJson = JsonUtil.parse(schema.toString(), ObjectNode.class);
    schemaJson.remove(MAPPING);
    return new Schema.Parser().parse(schemaJson.toString());
  }

  public static ColumnMapping parseFromSchema(Schema schema) {
    // parse the String because Avro uses com.codehaus.jackson
    return parse(schema.getJsonProp(MAPPING).toString());
  }

  public static boolean hasEmbeddedFieldMappings(Schema schema) {
    if (Schema.Type.RECORD == schema.getType()) {
      for (Schema.Field field : schema.getFields()) {
        if (field.getJsonProp(MAPPING) != null) {
          return true;
        }
      }
    }
    return false;
  }

  public static ColumnMapping parseFromSchemaFields(Schema schema) {
    if (Schema.Type.RECORD == schema.getType()) {
      ColumnMapping.Builder builder = new ColumnMapping.Builder();
      for (Schema.Field field : schema.getFields()) {
        if (field.getJsonProp(MAPPING) != null) {
          // parse the String because Avro uses com.codehaus.jackson
          builder.fieldMapping(parseFieldMapping(field.name(),
              JsonUtil.parse(field.getJsonProp(MAPPING).toString())));
        }
      }
      return builder.build();
    }
    throw new IllegalArgumentException(
        "Cannot parse field-level mappings from non-Record");
  }

  public static Schema embedColumnMapping(Schema schema, ColumnMapping mapping) {
    // TODO: avoid embedding mappings in the schema
    // Avro considers Props read-only and uses an older Jackson version
    // Parse the Schema as a String because Avro uses com.codehaus.jackson
    ObjectNode schemaJson = JsonUtil.parse(schema.toString(), ObjectNode.class);
    schemaJson.set(MAPPING, toJson(mapping));
    return new Schema.Parser().parse(schemaJson.toString());
  }

  public static Map<Integer, FieldMapping> parseKeyMappingsFromSchemaFields(
      Schema schema) {
    Map<Integer, FieldMapping> keyMappings = Maps.newHashMap();
    if (Schema.Type.RECORD == schema.getType()) {
      for (Schema.Field field : schema.getFields()) {
        if (field.getJsonProp(MAPPING) != null) {
          // parse the String because Avro uses com.codehaus.jackson
          JsonNode mappingNode = JsonUtil.parse(
              field.getJsonProp(MAPPING).toString());
          FieldMapping fm = parseFieldMapping(field.name(), mappingNode);
          if (FieldMapping.MappingType.KEY == fm.getMappingType() &&
              mappingNode.has(VALUE)) {
            Integer index = mappingNode.get(VALUE).asInt();
            keyMappings.put(index, fm);
          }
        }
      }
      return keyMappings;
    }
    throw new IllegalArgumentException(
        "Cannot parse field-level mappings from non-Record");
  }

  /**
   * Parses the FieldMapping from an annotated schema field.
   *
   * @param mappingNode
   *          The value of the "mapping" node
   * @return FieldMapping
   */
  public static FieldMapping parseFieldMapping(JsonNode mappingNode) {
    ValidationException.check(mappingNode.isObject(),
        "A column mapping must be a JSON record");

    ValidationException.check(mappingNode.has(SOURCE),
        "Partitioners must have a %s.", SOURCE);
    String source = mappingNode.get("source").asText();

    return parseFieldMapping(source, mappingNode);
  }

  /**
   * Parses the FieldMapping from an annotated schema field.
   *
   * @param source
   *          The source field name for this mapping
   * @param mappingNode
   *          The value of the "mapping" node
   * @return FieldMapping
   */
  public static FieldMapping parseFieldMapping(String source, JsonNode mappingNode) {
    ValidationException.check(mappingNode.isObject(),
        "A column mapping must be a JSON record");

    ValidationException.check(mappingNode.has(TYPE),
        "Column mappings must have a %s.", TYPE);
    String type = mappingNode.get(TYPE).asText();

    // return easy cases
    if ("occVersion".equals(type)) {
      return FieldMapping.version(source);
    } else if ("key".equals(type)) {
      return FieldMapping.key(source);
    }

    String family = null;
    String qualifier = null;
    String prefix = null;

    // for backward-compatibility, check for "value": "fam:qual"
    if (mappingNode.has(VALUE)) {
      // avoids String#split because of odd cases, like ":".split(":")
      String value = mappingNode.get(VALUE).asText();
      Iterator<String> values = VALUE_SPLITTER.split(value).iterator();
      if (values.hasNext()) {
        family = values.next();
      }
      if (values.hasNext()) {
        if ("keyAsColumn".equals(type)) {
          prefix = values.next();
          if (prefix.isEmpty()) {
            prefix = null;
          }
        } else {
          qualifier = values.next();
        }
      }
    }

    // replace any existing values with explicit family and qualifier
    if (mappingNode.has(FAMILY)) {
      family = mappingNode.get(FAMILY).textValue();
    }
    if (mappingNode.has(QUALIFIER)) {
      qualifier = mappingNode.get(QUALIFIER).textValue();
    }

    if ("column".equals(type)) {
      ValidationException.check(family != null && !family.isEmpty(),
          "Column mapping %s must have a %s", source, FAMILY);
      ValidationException.check(qualifier != null && !qualifier.isEmpty(),
          "Column mapping %s must have a %s", source, QUALIFIER);
      return FieldMapping.column(source, family, qualifier);

    } else if ("keyAsColumn".equals(type)) {
      ValidationException.check(family != null && !family.isEmpty(),
          "Column mapping %s must have a %s", source, FAMILY);
      ValidationException.check(qualifier == null,
          "Key-as-column mapping %s cannot have a %s", source, QUALIFIER);
      if (mappingNode.has(PREFIX)) {
        prefix = mappingNode.get(PREFIX).asText();
        if (prefix.isEmpty()) {
          prefix = null;
        }
      }
      return FieldMapping.keyAsColumn(source, family, prefix);

    } else if ("counter".equals(type)) {
      ValidationException.check(family != null && !family.isEmpty(),
          "Counter mapping %s must have a %s", source, FAMILY);
      ValidationException.check(qualifier != null && !qualifier.isEmpty(),
          "Counter mapping %s must have a %s", source, QUALIFIER);
      return FieldMapping.counter(source, family, qualifier);

    } else {
      throw new ValidationException("Invalid mapping type: " + type);
    }
  }

  private static ColumnMapping buildColumnMapping(JsonNode node) {
    ValidationException.check(node.isArray(),
        "Must be a JSON array of column mappings");

    ColumnMapping.Builder builder = new ColumnMapping.Builder();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
      builder.fieldMapping(parseFieldMapping(it.next()));
    }
    return builder.build();
  }

  private static JsonNode toJson(FieldMapping fm) {
    ObjectNode fieldMapping = JsonNodeFactory.instance.objectNode();
    fieldMapping.set(SOURCE, TextNode.valueOf(fm.getFieldName()));
    switch (fm.getMappingType()) {
      case KEY:
        fieldMapping.set(TYPE, TextNode.valueOf("key"));
        break;
      case KEY_AS_COLUMN:
        fieldMapping.set(TYPE, TextNode.valueOf("keyAsColumn"));
        fieldMapping.set(FAMILY, TextNode.valueOf(fm.getFamilyAsString()));
        if (fm.getPrefix() != null) {
          fieldMapping.set(PREFIX, TextNode.valueOf(fm.getPrefix()));
        }
        break;
      case COLUMN:
        fieldMapping.set(TYPE, TextNode.valueOf("column"));
        fieldMapping.set(FAMILY, TextNode.valueOf(fm.getFamilyAsString()));
        fieldMapping.set(QUALIFIER, TextNode.valueOf(fm.getQualifierAsString()));
        break;
      case COUNTER:
        fieldMapping.set(TYPE, TextNode.valueOf("counter"));
        fieldMapping.set(FAMILY, TextNode.valueOf(fm.getFamilyAsString()));
        fieldMapping.set(QUALIFIER, TextNode.valueOf(fm.getQualifierAsString()));
        break;
      case OCC_VERSION:
        fieldMapping.set(TYPE, TextNode.valueOf("occVersion"));
        break;
      default:
        throw new ValidationException(
            "Unknown mapping type: " + fm.getMappingType());
    }
    return fieldMapping;
  }

  public static String toString(FieldMapping mapping) {
    StringWriter writer = new StringWriter();
    JsonGenerator gen;
    try {
      gen = new JsonFactory().createGenerator(writer);
      gen.setCodec(new ObjectMapper());
      gen.writeTree(toJson(mapping));
      gen.close();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot write to JSON generator", e);
    }
    return writer.toString();
  }

  private static JsonNode toJson(ColumnMapping mapping) {
    ArrayNode mappingJson = JsonNodeFactory.instance.arrayNode();
    for (FieldMapping fm : mapping.getFieldMappings()) {
      mappingJson.add(toJson(fm));
    }
    return mappingJson;
  }

  public static String toString(ColumnMapping mapping, boolean pretty) {
    StringWriter writer = new StringWriter();
    JsonGenerator gen;
    try {
      gen = new JsonFactory().createGenerator(writer);
      if (pretty) {
        gen.useDefaultPrettyPrinter();
      }
      gen.setCodec(new ObjectMapper());
      gen.writeTree(toJson(mapping));
      gen.close();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot write to JSON generator", e);
    }
    return writer.toString();
  }
}
