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
package org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader;

import java.io.BufferedReader;
import java.io.IOException;

import org.kitesdk.morphline.api.Record;



/**
 * The CSVTokenizer specifies the behaviour how to parse each line into a List of Strings.
 */
public interface CSVTokenizer {
  /**
   * Splits the line into tokens.
   *
   * @param line the current line
   * @param reader the reader may be used to read further lines if the line ends with an open quotation
   * @return the tokens
   */
  public boolean tokenizeLine(String line, BufferedReader reader, Record record) throws IOException;
}
