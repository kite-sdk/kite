/*
 * Copyright 2013 Cloudera Inc.
 *
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
package org.kitesdk.morphline.base;

import java.util.ArrayList;
import java.util.List;

import org.kitesdk.morphline.api.Record;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

/**
 * Helper to fetch the values of a field of a {@link Record} referred to by a field expression,
 * which is a String of the form <code>@{fieldname}</code>.
 */
public final class FieldExpression {
  
  private String expression;
  
  private static final String START_TOKEN = "@{";
  private static final char END_TOKEN = '}';
  
  public FieldExpression(String expression, Config config) {
    Preconditions.checkNotNull(expression);
    this.expression = expression; // TODO: optimize by separating compilation and evaluation phase
  }
  
  /** Returns the values of a {@link Record} referred to by the given field expression */
  public List evaluate(Record record) {
    ArrayList results = new ArrayList(1);
    evaluate(0, record, new StringBuilder(), results);
    return results;
  }

  private void evaluate(int from, Record record, StringBuilder buf, ArrayList results) {
    int start = expression.indexOf(START_TOKEN, from);
    if (start < 0) { // START_TOKEN not found
      if (from == 0) {
        results.add(expression); // fast path
      } else {
        buf.append(expression, from, expression.length());
        results.add(buf.toString());
      }
    } else { // START_TOKEN found
      int end = expression.indexOf(END_TOKEN, start + START_TOKEN.length());
      if (end < 0) {
        throw new IllegalArgumentException("Missing closing token: " + END_TOKEN);
      }
      buf.append(expression, from, start);
      String ref = expression.substring(start + START_TOKEN.length(), end);
      if (ref.length() == 0) {
        buf.append(record.toString()); // @{} means dump string representation of entire record
        evaluate(end + 1, record, buf, results);
      } else {
        List resolvedValues = record.get(ref);
        if (start == 0 && end + 1 == expression.length()) { 
          results.addAll(resolvedValues); // "@{first_name}" resolves to object list rather than string concat
        } else {
          for (Object value : resolvedValues) {
            StringBuilder buf2 = new StringBuilder(buf);
            buf2.append(value.toString());
            evaluate(end + 1, record, buf2, results);
          }
        }
      }
    }
  }
  
}
