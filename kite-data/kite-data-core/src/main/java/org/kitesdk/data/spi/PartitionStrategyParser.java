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
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.partition.DateFormatPartitioner;
import org.kitesdk.data.spi.partition.DayOfMonthFieldPartitioner;
import org.kitesdk.data.spi.partition.LongFixedSizeRangeFieldPartitioner;
import org.kitesdk.data.spi.partition.HashFieldPartitioner;
import org.kitesdk.data.spi.partition.HourFieldPartitioner;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.kitesdk.data.spi.partition.MinuteFieldPartitioner;
import org.kitesdk.data.spi.partition.MonthFieldPartitioner;
import org.kitesdk.data.spi.partition.ProvidedFieldPartitioner;
import org.kitesdk.data.spi.partition.YearFieldPartitioner;

/**
 * Parses a PartitionStrategy from JSON representation.
 * 
 * <pre>
 * [
 *   { "type": "provided", "name": "version", "values": "int" }
 *   { "type": "identity", "source": "id", "name": "id" },
 *   { "type": "year", "source": "created" },
 *   { "type": "month", "source": "created" },
 *   { "type": "day", "source": "created" }
 * ]
 * </pre>
 * 
 */
public class PartitionStrategyParser {

  // name of the json node when embedded in a schema
  private static final String PARTITIONS = "partitions";

  // property constants
  private static final String TYPE = "type";
  private static final String SOURCE = "source";
  private static final String NAME = "name";
  private static final String BUCKETS = "buckets";
  private static final String FORMAT = "format";
  private static final String VALUES = "values";
  private static final String SIZE = "size";

  /**
   * Parses a PartitionStrategy from a JSON string.
   * 
   * @param json
   *          The JSON string
   * @return The PartitionStrategy.
   */
  public static PartitionStrategy parse(String json) {
    return buildPartitionStrategy(JsonUtil.parse(json));
  }

  /**
   * Parses a PartitionStrategy from a File
   * 
   * @param file
   *          The File that contains the PartitionStrategy in JSON format.
   * @return The PartitionStrategy.
   */
  public static PartitionStrategy parse(File file) {
    return buildPartitionStrategy(JsonUtil.parse(file));
  }

  /**
   * Parses a PartitionStrategy from an input stream
   * 
   * @param in
   *          The input stream that contains the PartitionStrategy in JSON
   *          format.
   * @return The PartitionStrategy.
   */
  public static PartitionStrategy parse(InputStream in) {
      return buildPartitionStrategy(JsonUtil.parse(in));
  }

  public static boolean hasEmbeddedStrategy(Schema schema) {
    return schema.getJsonProp(PARTITIONS) != null;
  }

  public static PartitionStrategy parseFromSchema(Schema schema) {
    // parse the String because Avro uses com.codehaus.jackson
    return parse(schema.getJsonProp(PARTITIONS).toString());
  }

  public static Schema removeEmbeddedStrategy(Schema schema) {
    // TODO: avoid embedding strategies in the schema
    // Avro considers Props read-only and uses an older Jackson version
    // Parse the Schema as a String because Avro uses com.codehaus.jackson
    ObjectNode schemaJson = JsonUtil.parse(schema.toString(), ObjectNode.class);
    schemaJson.remove(PARTITIONS);
    return new Schema.Parser().parse(schemaJson.toString());
  }

  public static Schema embedPartitionStrategy(Schema schema, PartitionStrategy strategy) {
    // TODO: avoid embedding strategies in the schema
    // Avro considers Props read-only and uses an older Jackson version
    // Parse the Schema as a String because Avro uses com.codehaus.jackson
    ObjectNode schemaJson = JsonUtil.parse(schema.toString(), ObjectNode.class);
    schemaJson.set(PARTITIONS, toJson(strategy));
    return new Schema.Parser().parse(schemaJson.toString());
  }

  private static PartitionStrategy buildPartitionStrategy(JsonNode node) {
    ValidationException.check(node.isArray(),
        "A partition strategy must be a JSON array of partitioners");

    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
      JsonNode fieldPartitioner = it.next();
      ValidationException.check(fieldPartitioner.isObject(),
          "A partitioner must be a JSON record");

      ValidationException.check(fieldPartitioner.has(TYPE),
          "Partitioners must have a %s", TYPE);
      String type = fieldPartitioner.get(TYPE).asText();

      // only provided partitioners do not need a source field
      boolean isProvided = type.equals("provided");
      ValidationException.check(isProvided || fieldPartitioner.has(SOURCE),
          "Partitioners must have a %s", SOURCE);

      String source = null;
      // only provided has no source field
      if (!isProvided) {
        source = fieldPartitioner.get(SOURCE).asText();
      }

      String name = null;
      if (fieldPartitioner.has(NAME)) {
        name = fieldPartitioner.get(NAME).asText();
      }

      // Note: string range, int range, and list partitioners are not supported
      if (type.equals("identity")) {
        builder.identity(source, name);
      } else if (type.equals("hash")) {
        ValidationException.check(fieldPartitioner.has(BUCKETS),
            "Hash partitioner %s must have attribute %s",
            name == null ? source : name, BUCKETS);
        int buckets = fieldPartitioner.get(BUCKETS).asInt();
        ValidationException.check(buckets > 0,
            "Invalid number of buckets for hash partitioner %s: %s",
            name == null ? source : name,
            fieldPartitioner.get(BUCKETS).asText());
        builder.hash(source, name, buckets);
      } else if (type.equals("range")) {
        ValidationException.check(fieldPartitioner.has(SIZE),
            "Range partitioner %s must have attribute %s",
            name == null ? source : name, SIZE);
        long size = fieldPartitioner.get(SIZE).asLong();
        ValidationException.check(size > 0,
            "Invalid size for range partitioner %s: %s",
            name == null ? source : name,
            fieldPartitioner.get(SIZE).asText());
        builder.fixedSizeRange(source, name, size);
      } else if (type.equals("year")) {
        builder.year(source, name);
      } else if (type.equals("month")) {
        builder.month(source, name);
      } else if (type.equals("day")) {
        builder.day(source, name);
      } else if (type.equals("hour")) {
        builder.hour(source, name);
      } else if (type.equals("minute")) {
        builder.minute(source, name);
      } else if (type.equals("dateFormat")) {
        ValidationException.check(name != null,
            "Date format partitioner %s must have a %s.", source, NAME);
        ValidationException.check(fieldPartitioner.has(FORMAT),
            "Date format partitioner %s must have a %s.", name, FORMAT);
        String format = fieldPartitioner.get(FORMAT).asText();
        builder.dateFormat(source, name, format);
      } else if (isProvided) {
        ValidationException.check(name != null,
            "Provided partitioners must have a %s.", NAME);
        String valuesType = null;
        if (fieldPartitioner.has(VALUES)) {
          valuesType = fieldPartitioner.get(VALUES).asText();
        }
        builder.provided(name, valuesType);

      } else {
        throw new ValidationException("Invalid FieldPartitioner: " + type);
      }
    }
    return builder.build();
  }

  private static JsonNode toJson(PartitionStrategy strategy) {
    ArrayNode strategyJson = JsonNodeFactory.instance.arrayNode();
    for (FieldPartitioner fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
      ObjectNode partitioner = JsonNodeFactory.instance.objectNode();
      partitioner.set(NAME, TextNode.valueOf(fp.getName()));
      if (fp instanceof IdentityFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("identity"));
      } else if (fp instanceof HashFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("hash"));
        partitioner.set(BUCKETS, LongNode.valueOf(fp.getCardinality()));
      } else if (fp instanceof LongFixedSizeRangeFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("range"));
        partitioner.set(SIZE,
            LongNode.valueOf(((LongFixedSizeRangeFieldPartitioner) fp).getSize()));
      } else if (fp instanceof YearFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("year"));
      } else if (fp instanceof MonthFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("month"));
      } else if (fp instanceof DayOfMonthFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("day"));
      } else if (fp instanceof HourFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("hour"));
      } else if (fp instanceof MinuteFieldPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("minute"));
      } else if (fp instanceof DateFormatPartitioner) {
        partitioner.set(SOURCE, TextNode.valueOf(fp.getSourceName()));
        partitioner.set(TYPE, TextNode.valueOf("dateFormat"));
        partitioner.set(FORMAT,
            TextNode.valueOf(((DateFormatPartitioner) fp).getPattern()));
      } else if (fp instanceof ProvidedFieldPartitioner) {
        partitioner.set(TYPE, TextNode.valueOf("provided"));
        partitioner.set(VALUES,
            TextNode.valueOf(((ProvidedFieldPartitioner) fp).getTypeAsString()));
      } else {
        throw new ValidationException(
            "Unknown partitioner class: " + fp.getClass());
      }
      strategyJson.add(partitioner);
    }
    return strategyJson;
  }

  public static String toString(PartitionStrategy strategy, boolean pretty) {
    StringWriter writer = new StringWriter();
    JsonGenerator gen;
    try {
      gen = new JsonFactory().createGenerator(writer);
      if (pretty) {
        gen.useDefaultPrettyPrinter();
      }
      gen.setCodec(new ObjectMapper());
      gen.writeTree(toJson(strategy));
      gen.close();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot write to JSON generator", e);
    }
    return writer.toString();
  }

}
