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

import au.com.bytecode.opencsv.CSVParser;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DataModelUtil;

public class CSVRecordParser<E> {
  private final CSVParser parser;
  private final CSVRecordBuilder<E> builder;

  public CSVRecordParser(CSVProperties props,
                         View<E> view,
                         List<String> header) {
    this.parser = CSVUtil.newParser(props);
    this.builder = new CSVRecordBuilder<E>(
        DataModelUtil.getReaderSchema(
            view.getType(), view.getDataset().getDescriptor().getSchema()),
        view.getType(), header);
  }

  public E read(String line) {
    return read(line, null);
  }

  public E read(String line, @Nullable E reuse) {
    try {
      return builder.makeRecord(parser.parseLine(line), reuse);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot parse line: " + line, e);
    }
  }
}
