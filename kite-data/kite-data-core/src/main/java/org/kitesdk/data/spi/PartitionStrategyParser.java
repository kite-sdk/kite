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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.partition.DateFormatPartitioner;
import org.kitesdk.data.spi.partition.DayOfMonthFieldPartitioner;
import org.kitesdk.data.spi.partition.HashFieldPartitioner;
import org.kitesdk.data.spi.partition.HourFieldPartitioner;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.kitesdk.data.spi.partition.MinuteFieldPartitioner;
import org.kitesdk.data.spi.partition.MonthFieldPartitioner;
import org.kitesdk.data.spi.partition.YearFieldPartitioner;

/**
 * Parses a PartitionStrategy from JSON representation.
 * 
 * <pre>
 * [
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

  /**
   * Parses a PartitionStrategy from a JSON string.
   * 
   * @param json
   *          The JSON string
   * @return The PartitionStrategy.
   */
  public PartitionStrategy parse(String json) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(json, JsonNode.class);
      return buildPartitionStrategy(node);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  /**
   * Parses a PartitionStrategy from a File
   * 
   * @param file
   *          The File that contains the PartitionStrategy in JSON format.
   * @return The PartitionStrategy.
   */
  public PartitionStrategy parse(File file) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(file, JsonNode.class);
      return buildPartitionStrategy(node);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  /**
   * Parses a PartitionStrategy from an input stream
   * 
   * @param in
   *          The input stream that contains the PartitionStrategy in JSON
   *          format.
   * @return The PartitionStrategy.
   */
  public PartitionStrategy parse(InputStream in) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(in, JsonNode.class);
      return buildPartitionStrategy(node);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public boolean hasEmbeddedStrategy(Schema schema) {
    return schema.getJsonProp(PARTITIONS) != null;
  }

  public PartitionStrategy parseFromSchema(Schema schema) {
    // parse the String because Avro uses com.codehaus.jackson
    return parse(schema.getJsonProp(PARTITIONS).toString());
  }

  private PartitionStrategy buildPartitionStrategy(JsonNode node) {
    ValidationException.check(node.isArray(),
        "A partition strategy must be a JSON array of partitioners");

    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
      JsonNode fieldPartitioner = it.next();
      ValidationException.check(fieldPartitioner.isObject(),
          "A partitioner must be a JSON record");
      ValidationException.check(fieldPartitioner.has(TYPE),
          "Partitioners must have a %s", TYPE);
      ValidationException.check(fieldPartitioner.has(SOURCE),
          "Partitioners must have a %s", SOURCE);

      String type = fieldPartitioner.get(TYPE).asText();
      String source = fieldPartitioner.get(SOURCE).asText();

      String name = null;
      if (fieldPartitioner.has(NAME)) {
        name = fieldPartitioner.get(NAME).asText();
      }

      // Note: string range, int range, and list partitioners are not supported
      if (type.equals("identity")) {
        ValidationException.check(name != null,
            "Identity partitioner %s must have a %s", source, NAME);
        builder.identity(source, name, Object.class, -1);
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
      } else {
        throw new ValidationException("Invalid FieldPartitioner: " + type);
      }
    }
    return builder.build();
  }

  public static String toString(PartitionStrategy strategy, boolean pretty) {
    StringWriter writer = new StringWriter();
    JsonGenerator gen;
    try {
      gen = new JsonFactory().createGenerator(writer);
      if (pretty) {
        gen.useDefaultPrettyPrinter();
      }
      gen.writeStartArray();
      for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
        gen.writeStartObject();
        if (fp instanceof IdentityFieldPartitioner) {
          gen.writeStringField(TYPE, "identity");
        } else if (fp instanceof HashFieldPartitioner) {
          gen.writeStringField(TYPE, "hash");
          gen.writeNumberField(BUCKETS, fp.getCardinality());
        } else if (fp instanceof YearFieldPartitioner) {
          gen.writeStringField(TYPE, "year");
        } else if (fp instanceof MonthFieldPartitioner) {
          gen.writeStringField(TYPE, "month");
        } else if (fp instanceof DayOfMonthFieldPartitioner) {
          gen.writeStringField(TYPE, "day");
        } else if (fp instanceof HourFieldPartitioner) {
          gen.writeStringField(TYPE, "hour");
        } else if (fp instanceof MinuteFieldPartitioner) {
          gen.writeStringField(TYPE, "minute");
        } else if (fp instanceof DateFormatPartitioner) {
          gen.writeStringField(TYPE, "dateFormat");
          gen.writeStringField(FORMAT,
              ((DateFormatPartitioner) fp).getPattern());
        } else {
          throw new ValidationException(
              "Unknown partitioner class: " + fp.getClass());
        }
        gen.writeStringField(SOURCE, fp.getSourceName());
        gen.writeStringField(NAME, fp.getName());
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
