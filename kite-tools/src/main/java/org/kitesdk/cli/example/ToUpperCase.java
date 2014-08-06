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

package org.kitesdk.cli.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.crunch.MapFn;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"SE_NO_SERIALVERSIONID","DM_CONVERT_CASE"},
    justification="This class is an example.")
public class ToUpperCase extends MapFn<GenericRecord, GenericRecord> {
  @Override
  public GenericRecord map(GenericRecord input) {
    Schema schema = input.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Object value = input.get(field.name());
      if (value instanceof String || value instanceof Utf8) {
        // replace with upper case
        input.put(field.name(), value.toString().toUpperCase());
      }
    }
    return input;
  }
}
