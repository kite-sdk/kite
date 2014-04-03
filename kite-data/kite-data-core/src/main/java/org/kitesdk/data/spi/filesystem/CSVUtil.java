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

  public static Schema inferSchema(InputStream incoming, CSVProperties props)
      throws IOException {
    CSVReader reader = newReader(incoming, props);

    String[] header;
    String[] line;
    if (props.useHeader) {
      // read the header and then the first line
      header = reader.readNext();
      Preconditions.checkNotNull(header, "No header content");
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

    boolean missing = true; // tracks whether a type is missing
    while (missing && line != null) {
      missing = false;
      for (int i = 0; i < header.length; i += 1) {
        if (types[i] == null) {
          types[i] = inferFieldType(line[i]);
          if (types[i] == null) {
            missing = true; // still need to find a value
          }
        }
      }
      line = reader.readNext();
    }

    // types may be missing, but fieldSchema will return a nullable string
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < header.length; i += 1) {
      fields.add(new Schema.Field(
          header[i], nullableSchema(types[i]), null, null));
    }

    return Schema.createRecord(fields);
  }

  /**
   * Create a nullable {@link Schema} for the given type. If the type is null,
   * the schema will be a nullable String.
   *
   * @param type a {@link Schema.Type} compatible with {@code Schema.create}
   * @return a nullable {@code Schema} for the given {@code Schema.Type}
   * @see Schema#create(org.apache.avro.Schema.Type)
   */
  private static Schema nullableSchema(Schema.Type type) {
    return Schema.createUnion(Lists.newArrayList(
        Schema.create(Schema.Type.NULL), // always nullable
        Schema.create(type == null ? Schema.Type.STRING : type)));
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
}
