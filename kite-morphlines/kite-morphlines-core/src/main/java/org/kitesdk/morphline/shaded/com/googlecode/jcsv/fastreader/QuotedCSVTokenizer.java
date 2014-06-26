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
import java.util.List;

import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This implementation follows the csv formatting standard, described in:
 * http://en.wikipedia.org/wiki/Comma-separated_values
 *
 * If you have a more specific csv format, such as constant column widths or
 * columns that do not need to be quoted, you may consider to write a more simple
 * but performant CSVTokenizer.
 *
 */
public final class QuotedCSVTokenizer implements CSVTokenizer {
  
  private final char separatorChar;
  private final boolean trim;
  private final boolean addEmptyStrings; 
  private final List<String> columnNames;
  private final char quoteChar;
  private final int maxCharactersPerRecord;
  private final boolean ignoreTooLongRecords;
  
  private static final Logger LOG = LoggerFactory.getLogger(QuotedCSVTokenizer.class);
  
  public QuotedCSVTokenizer(char separatorChar, boolean trim, boolean addEmptyStrings, List<String> columnNames,
      int maxCharactersPerRecord, boolean ignoreTooLongRecords, char quoteChar) {
    this.separatorChar = separatorChar;
    this.trim = trim;
    this.addEmptyStrings = addEmptyStrings;
    this.columnNames = columnNames;
    this.maxCharactersPerRecord = maxCharactersPerRecord;
    this.ignoreTooLongRecords = ignoreTooLongRecords;
    this.quoteChar = quoteChar;
  }
  
  /** Splits the given input line into parts, using the given delimiter. */
  @Override
  public boolean tokenizeLine(String line, BufferedReader reader, Record record) throws IOException {
    int numChars = line.length();
    final char DELIMITER = separatorChar;
    final char QUOTE = quoteChar;
    final StringBuilder sb = new StringBuilder(30);
    boolean isQuoted = false;
    int i = 0;
    int j = 0;
    
    while (true) {
      
      if (!isQuoted) {
        if (i == line.length()) {
          break; // we're done
        }
        final char c = line.charAt(i);
        if (c == DELIMITER) {
          put(sb, j, record);
          j++;
          sb.setLength(0);
        } else if (c == QUOTE) {
          if (sb.length() == 0) {
            isQuoted = true;
          } else if (i + 1 < line.length() && line.charAt(i + 1) == QUOTE) {
            sb.append(c);
            i++;
          } else {
            isQuoted = true;
          }
        } else {
          sb.append(c);
        }
          
      } else {
        assert isQuoted;
        if (i == line.length()) {
          sb.append('\n');
          i = -1;
          line = reader.readLine();
          if (line == null) {
            throw new IllegalStateException("unexpected end of file, unclosed quotation");
          }
          numChars += line.length();
          if (!verifyRecordLength(
              numChars, maxCharactersPerRecord, line, ignoreTooLongRecords, LOG)) {
            return false; // attempt to ignore it
          }
        } else {
          final char c = line.charAt(i);
          if (c == QUOTE) {
            if (i + 1 < line.length() && line.charAt(i + 1) == QUOTE) {
              sb.append(c); // found two quotes -> insert single quote
              i++;
            } else {
              isQuoted = false;
            }
          } else {
            sb.append(c);
          }
        }
      }

      i++;
    }
    
    if (!(j == 0 && sb.length() == 0)) {
      put(sb, j, record);
    }
    
    return true;
  }
  
  private void put(StringBuilder sb, int j, Record record) {
    if (j >= columnNames.size()) {
      columnNames.add("column" + j);
    }
    String columnName = columnNames.get(j);
    if (columnName.length() != 0) { // empty column name indicates omit this field on output
      String col = sb.toString();
      col = trim ? col.trim() : col;
      if (col.length() > 0 || addEmptyStrings) {
        record.put(columnName, col);
      }
    }
  }

  public static boolean verifyRecordLength(int numChars, int maxChars, String line, boolean ignoreTooLongRecords, Logger LOG) {
    if (numChars <= maxChars) {
      return true;
    }
    if (line.length() > 10000) {
      line = line.substring(0, 10000) + " ..."; // prevent gigantic messages
    }
    String msg = "Invalid input data - CSV record length is larger than the maximum of "
        + maxChars + " characters near line: " + line;
    if (ignoreTooLongRecords) {
      LOG.warn(msg);
      return false;
    } else {
      throw new IllegalArgumentException(msg);
    }      
  }
  
}
