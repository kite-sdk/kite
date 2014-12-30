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

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.kitesdk.compat.DynConstructors;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;

public class InputFormatUtil {
  public static final String INPUT_FORMAT_CLASS_PROP = "kite.inputformat.class";
  public static final String INPUT_FORMAT_RECORD_PROP = "kite.inputformat.record-type";

  public static enum RecordType {
    KEY,
    VALUE
  }

  public static <K, V> FileInputFormat<K, V> newInputFormatInstance(
      DatasetDescriptor descriptor) {
    DynConstructors.Ctor<FileInputFormat<K, V>> ctor =
        new DynConstructors.Builder()
            .impl(descriptor.getProperty(INPUT_FORMAT_CLASS_PROP))
            .build();
    return ctor.newInstance();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="DM_CONVERT_CASE", justification="For record types only")
  public static <E> RecordReader<E, Void> newRecordReader(
      DatasetDescriptor descriptor) {
    String typeString = descriptor.getProperty(INPUT_FORMAT_RECORD_PROP);
    RecordType type = RecordType.VALUE;
    if (typeString != null) {
      type = RecordType.valueOf(typeString.trim().toUpperCase());
    }

    if (type == RecordType.KEY) {
      FileInputFormat<E, Object> format = newInputFormatInstance(descriptor);
      return new KeyReaderWrapper<E>(format);
    } else if (type == RecordType.VALUE) {
      FileInputFormat<Object, E> format = newInputFormatInstance(descriptor);
      return new ValueReaderWrapper<E>(format);
    } else {
      throw new DatasetException("[BUG] Invalid record type: " + type);
    }
  }
}
