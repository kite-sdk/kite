/**
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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.fastreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;


public final class CSVReader {
  
  private final CSVStrategy strategy;
  private final CSVTokenizer tokenizer;

  public CSVReader(CSVStrategy strategy, CSVTokenizer tokenizer) {
    this.strategy = strategy;
    this.tokenizer = tokenizer;
  }

  public boolean readNext(BufferedReader reader, List<String> columns) throws IOException {
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        return false;
      }

      if (strategy.isIgnoreEmptyLines() && isTrimmedLineEmpty(line)) {
        continue;
      }

      String commentIndicator = strategy.getCommentIndicator();
      if (commentIndicator.length() > 0 && line.startsWith(commentIndicator)) {
        continue;
      }

      tokenizer.tokenizeLine(line, strategy, reader, columns);
      return true;
    }
  }
  
  private boolean isTrimmedLineEmpty(String line) {
//    return line.trim().length() == 0; // slow
    int len = line.length();
    for (int i = 0; i < len; i++) {
      if (line.charAt(i) > ' ') {
        return false;
      }
    }
    return true;
  }

}
