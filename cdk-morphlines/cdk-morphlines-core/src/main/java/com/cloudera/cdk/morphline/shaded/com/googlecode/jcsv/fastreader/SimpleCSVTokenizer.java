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


/**
 * A very simple csv tokenizer implementation.
 * If you do not need field quotations or multi line columns, this
 * will serve your purposes.
 *
 */
public final class SimpleCSVTokenizer implements CSVTokenizer {

	/**
	 * Performs a split() on the input string. Uses the delimiter specified in the csv strategy.
	 *
	 */
	@Override
	public void tokenizeLine(String line, CSVStrategy strategy, BufferedReader reader, List<String> columns) throws IOException {
		// split the line and preserve all tokens
		split(line, strategy.getDelimiter(), true, columns);
	}
	
	 /**
   * Splits the provided text into an array, separator specified, preserving
   * all tokens, including empty tokens created by adjacent separators.
   *
   * CSVUtil.split(null, *, true) = null
   * CSVUtil.split("", *, , true) = []
   * CSVUtil.split("a.b.c", '.', true) = ["a", "b", "c"]
   * CSVUtil.split("a...c", '.', true) = ["a", "", "", "c"]
   * CSVUtil.split("a...c", '.', false) = ["a", "c"]
   *
   * @param str
   *            the string to parse
   * @param separatorChar
   *            the seperator char
   * @param preserveAllTokens
   *            if true, adjacent separators are treated as empty token
   *            separators
   * @return the splitted string
   */
  private static void split(String str, char separatorChar, boolean preserveAllTokens, List<String> list) {
    int len = str.length();
    if (len == 0) {
      return;
    }
    int i = 0, start = 0;
    boolean match = false;
    boolean lastMatch = false;
    while (i < len) {
      if (str.charAt(i) == separatorChar) {
        if (match || preserveAllTokens) {
          list.add(str.substring(start, i));
          match = false;
          lastMatch = true;
        }
        start = ++i;
        continue;
      }
      lastMatch = false;
      match = true;
      i++;
    }
    if (match || preserveAllTokens && lastMatch) {
      list.add(str.substring(start, i));
    }
    return;
  }

}
