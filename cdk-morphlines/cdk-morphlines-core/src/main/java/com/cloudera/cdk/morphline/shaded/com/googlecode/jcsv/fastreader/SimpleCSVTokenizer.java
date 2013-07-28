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
  
  private StringBuilder buf;
  private int counter = 0;

	/**
	 * Performs a split() on the input string. Uses the delimiter specified in the csv strategy.
	 *
	 */
	@Override
	public void tokenizeLine(String line, CSVStrategy strategy, BufferedReader reader, List<String> columns) throws IOException {
	  if (counter++ % 1000 == 0) {
	    buf = new StringBuilder(); // periodically gc memory from large outlier columns
	  }
	  buf.setLength(0);
	  char separatorChar = strategy.getDelimiter();
	  int len = line.length();
	  for (int i = 0; i < len; i++) {
	    char c = line.charAt(i);
	    if (c == separatorChar) {
	      columns.add(buf.toString());
	      buf.setLength(0);
	    } else {
	      buf.append(c);
	    }
	  }
    columns.add(buf.toString());
	}
	
}
