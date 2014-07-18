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

package org.kitesdk.data.spi.filesystem;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.avro.Schema;

public class CSVUtil {

  public static CSVReader newReader(InputStream incoming, CSVProperties props) {
    return new CSVReader(
        new InputStreamReader(incoming, Charset.forName(props.charset)),
        props.delimiter.charAt(0), props.quote.charAt(0),
        props.escape.charAt(0), props.linesToSkip,
        false /* strict quotes off: don't ignore unquoted strings */,
        true /* ignore leading white-space */ );
  }

  public static CSVWriter newWriter(OutputStream outgoing, CSVProperties props) {
    return new CSVWriter(new OutputStreamWriter(
        outgoing, Charset.forName(props.charset)),
        props.delimiter.charAt(0), props.quote.charAt(0),
        props.escape.charAt(0));
  }

  private static final Pattern LONG = Pattern.compile("\\d+");
  private static final Pattern DOUBLE = Pattern.compile("\\d*\\.\\d*[dD]?");
  private static final Pattern FLOAT = Pattern.compile("\\d*\\.\\d*[fF]?");
  private static final int DEFAULT_INFER_LINES = 25;

  public static Schema inferNullableSchema(String name, InputStream incoming,
                                   CSVProperties props)
      throws IOException {
    return inferSchemaInternal(name, incoming, props, true);
  }

  public static Schema inferSchema(String name, InputStream incoming,
                                           CSVProperties props)
      throws IOException {
    return inferSchemaInternal(name, incoming, props, false);
  }

  private static Schema inferSchemaInternal(String name, InputStream incoming,
                                   CSVProperties props, boolean makeNullable)
      throws IOException {
    CSVReader reader = newReader(incoming, props);

    String[] header;
    String[] line;
    if (props.useHeader) {
      // read the header and then the first line
      header = reader.readNext();
      checkHeader(header);
      line = reader.readNext();
      Preconditions.checkNotNull(line, "No content to infer schema");

    } else {
      // use the first line to create a header
      line = reader.readNext();
      Preconditions.checkNotNull(line, "No content to infer schema");
      header = new String[line.length];
      for (int i = 0; i < line.length; i += 1) {
        header[i] = "field_" + String.valueOf(i);
      }
    }

    Schema.Type[] types = new Schema.Type[header.length];
    String[] values = new String[header.length];
    boolean[] nullable = new boolean[header.length];

    for (int processed = 0; processed < DEFAULT_INFER_LINES; processed += 1) {
      for (int i = 0; i < header.length; i += 1) {
        if (types[i] == null) {
          types[i] = inferFieldType(line[i]);
          if (types[i] == null) {
            nullable[i] = true;
          } else {
            // keep track of the value used
            values[i] = line[i];
          }
        } else if (line[i] == null || line[i].isEmpty()) {
          nullable[i] = true;
        }
      }
      line = reader.readNext();
      if (line == null) {
        break;
      }
    }

    // types may be missing, but fieldSchema will return a nullable string
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < header.length; i += 1) {
      if (header[i] == null) {
        throw new RuntimeException("Bad header for field " + i + ": null");
      } else if (header[i].trim().isEmpty()) {
        throw new RuntimeException(
            "Bad header for field " + i + ": \"" + header[i] + "\"");
      } else if(!header[i].trim().matches("^[A-Za-z_][A-Za-z\\d_]*$")) {
          throw new RuntimeException(
              "Bad header for field, should start with a character " +
              "or _ and can contain only alphanumerics and _ " +
              i + ": \"" + header[i] + "\"");
      }
      fields.add(new Schema.Field(
          header[i].trim(), schema(types[i], makeNullable || nullable[i]),
          "Type inferred from '" + String.valueOf(values[i]) + "'", null));
    }

    Schema record = Schema.createRecord(
        name, "Schema generated by Kite", null, false);
    record.setFields(fields);
    return record;
  }

  /**
   * Create a {@link Schema} for the given type. If the type is null,
   * the schema will be a nullable String. If isNullable is true, the returned
   * schema will be nullable.
   *
   * @param type a {@link Schema.Type} compatible with {@code Schema.create}
   * @param makeNullable If {@code true}, the return type will be nullable
   * @return a {@code Schema} for the given {@code Schema.Type}
   * @see Schema#create(org.apache.avro.Schema.Type)
   */
  private static Schema schema(Schema.Type type, boolean makeNullable) {
    Schema schema = Schema.create(type == null ? Schema.Type.STRING : type);
    if (makeNullable || type == null) {
      schema = Schema.createUnion(Lists.newArrayList(
          Schema.create(Schema.Type.NULL), schema));
    }
    return schema;
  }

  private static Schema.Type inferFieldType(String example) {
    if (example == null || example.isEmpty()) {
      return null; // not enough information
    } else if (LONG.matcher(example).matches()) {
      return Schema.Type.LONG;
    } else if (DOUBLE.matcher(example).matches()) {
      return Schema.Type.DOUBLE;
    } else if (FLOAT.matcher(example).matches()) {
      return Schema.Type.FLOAT;
    }
    return Schema.Type.STRING;
  }

  private static void checkHeader(String[] header) {
    Preconditions.checkNotNull(header, "No header content");
    for (int i = 0; i < header.length; i += 1) {
      if (header[i] == null) {
        throw new RuntimeException("Bad header for field " + i + ": null");
      } else if (header[i].trim().isEmpty()) {
        throw new RuntimeException(
            "Bad header for field " + i + ": \"" + header[i] + "\"");
      }
    }
  }
}
