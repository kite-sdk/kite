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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetIOException;
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
  public ColumnMapping parse(String mappingDescriptor) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(mappingDescriptor, JsonNode.class);
      return buildColumnMapping(node);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  /**
   * Parses the Mapping Descriptor from a File
   *
   * @param file
   *          The File that contains the Mapping Descriptor in JSON format.
   * @return ColumnMapping.
   */
  public ColumnMapping parse(File file) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(file, JsonNode.class);
      return buildColumnMapping(node);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  /**
   * Parses the Mapping Descriptor from an input stream
   *
   * @param in
   *          The input stream that contains the Mapping Descriptor in JSON
   *          format.
   * @return ColumnMapping.
   */
  public ColumnMapping parse(InputStream in) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(in, JsonNode.class);
      return buildColumnMapping(node);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public boolean hasEmbeddedColumnMappings(Schema schema) {
    return schema.getJsonProp(MAPPING) != null;
  }

  public ColumnMapping parseFromSchema(Schema schema) {
    // parse the String because Avro uses com.codehaus.jackson
    return parse(schema.getJsonProp(MAPPING).toString());
  }

  /**
   * Parses the FieldMapping from an annotated schema field.
   *
   * @param mappingNode
   *          The value of the "mapping" node
   * @return FieldMapping
   */
  public FieldMapping parseFieldMapping(JsonNode mappingNode) {
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
  public FieldMapping parseFieldMapping(String source, JsonNode mappingNode) {
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

    // for backward-compatibility, check for "value": "fam:qual"
    if (mappingNode.has(VALUE)) {
      // avoids String#split because of odd cases, like ":".split(":")
      String value = mappingNode.get(VALUE).asText();
      Iterator<String> values = VALUE_SPLITTER.split(value).iterator();
      if (values.hasNext()) {
        family = values.next();
      }
      if (values.hasNext()) {
        qualifier = values.next();
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
      ValidationException.check(family != null,
          "Column mapping %s must have a %s", source, FAMILY);
      ValidationException.check(qualifier != null,
          "Column mapping %s must have a %s", source, QUALIFIER);
      return FieldMapping.column(source, family, qualifier);

    } else if ("keyAsColumn".equals(type)) {
      ValidationException.check(qualifier == null,
          "Key-as-column mapping %s cannot have a %s", source, QUALIFIER);
      if (mappingNode.has(PREFIX)) {
        String prefix = mappingNode.get(PREFIX).asText();
        return FieldMapping.keyAsColumn(source, family, prefix);
      } else {
        return FieldMapping.keyAsColumn(source, family);
      }

    } else if ("counter".equals(type)) {
      ValidationException.check(family != null,
          "Counter mapping %s must have a %s", source, FAMILY);
      ValidationException.check(qualifier != null,
          "Counter mapping %s must have a %s", source, QUALIFIER);
      return FieldMapping.counter(source, family, qualifier);

    } else {
      throw new ValidationException("Invalid mapping type: " + type);
    }
  }

  private ColumnMapping buildColumnMapping(JsonNode node) {
    ValidationException.check(node.isArray(),
        "Must be a JSON array of column mappings");

    ColumnMapping.Builder builder = new ColumnMapping.Builder();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
      builder.fieldMapping(parseFieldMapping(it.next()));
    }
    return builder.build();
  }

  public static String toString(ColumnMapping mappings, boolean pretty) {
    StringWriter writer = new StringWriter();
    JsonGenerator gen;
    try {
      gen = new JsonFactory().createGenerator(writer);
      if (pretty) {
        gen.useDefaultPrettyPrinter();
      }
      gen.writeStartArray();
      for (FieldMapping fm : mappings.getFieldMappings()) {
        gen.writeStartObject();
        gen.writeStringField(SOURCE, fm.getFieldName());
        switch (fm.getMappingType()) {
          case KEY:
            gen.writeStringField(TYPE, "key");
            break;
          case KEY_AS_COLUMN:
            gen.writeStringField(TYPE, "keyAsColumn");
            gen.writeStringField(FAMILY, fm.getFamilyAsString());
            if (fm.getPrefix() != null) {
              gen.writeStringField(PREFIX, fm.getPrefix());
            }
            break;
          case COLUMN:
            gen.writeStringField(TYPE, "column");
            gen.writeStringField(FAMILY, fm.getFamilyAsString());
            gen.writeStringField(QUALIFIER, fm.getQualifierAsString());
            break;
          case COUNTER:
            gen.writeStringField(TYPE, "counter");
            gen.writeStringField(FAMILY, fm.getFamilyAsString());
            gen.writeStringField(QUALIFIER, fm.getQualifierAsString());
            break;
          case OCC_VERSION:
            gen.writeStringField(TYPE, "occVersion");
            break;
          default:
            throw new ValidationException(
                "Unknown mapping type: " + fm.getMappingType());
        }
        gen.writeEndObject();
      }
      gen.writeEndArray();
      gen.close();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot write to JSON generator", e);
    }
    return writer.toString();
  }
}
