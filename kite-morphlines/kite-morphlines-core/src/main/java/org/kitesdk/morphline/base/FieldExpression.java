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
import java.util.Collections;
import java.util.List;

import org.kitesdk.morphline.api.Record;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

/**
 * Helper to fetch the values of a field of a {@link Record} referred to by a field expression,
 * which is a String of the form <code>@{fieldname}</code>.
 */
public final class FieldExpression {
  
  private final List<Object> fields = Lists.newArrayList();
  
  private static final String START_TOKEN = "@{";
  private static final char END_TOKEN = '}';
  
  public FieldExpression(String expression, Config config) {
    Preconditions.checkNotNull(expression);
    int from = 0;
    int start;
    while (from < expression.length() && (start = expression.indexOf(START_TOKEN, from)) >= 0) {
      if (from != start) {
        fields.add(expression.substring(from, start));
      }
      int end = expression.indexOf(END_TOKEN, start + START_TOKEN.length());
      if (end < 0) {
        throw new IllegalArgumentException("Missing closing token: " + END_TOKEN);
      }
      String ref = expression.substring(start + START_TOKEN.length(), end);
      fields.add(new Field(ref));
      from = end + 1;
    }
    
    assert from <= expression.length();
    if (from < expression.length() || fields.size() == 0) {
      fields.add(expression.substring(from, expression.length()));
    }
    assert fields.size() > 0;
  }
  
  /** Returns the values of a {@link Record} referred to by the given field expression */
  public List evaluate(Record record) {
    // fast path (not functionally necessary):
    if (fields.size() == 1) {
      Object first = fields.get(0);
      if (first instanceof String) {
        return Collections.singletonList(first);
      } else {
        String ref = ((Field) first).getName();
        if (ref.length() != 0) {
          List resolvedValues = record.get(ref);
          return resolvedValues;
        }
      }
    }

    // slow path:
    ArrayList<Object> results = new ArrayList<Object>(1);
    evaluate2(0, record, new StringBuilder(), results);
    return results;
  }

  private void evaluate2(int from, Record record, StringBuilder buf, ArrayList<Object> results) {
    if (from >= fields.size()) {
      results.add(buf.toString());
      return;
    }
    
    Object item = fields.get(from);
    if (item instanceof String) {
      buf.append(item);
      evaluate2(from + 1, record, buf, results);
    } else {
      String ref = ((Field) item).getName();
      if (ref.length() == 0) {
        buf.append(record.toString()); // @{} means dump string representation of entire record
        evaluate2(from + 1, record, buf, results);
      } else {
        List resolvedValues = record.get(ref);
        for (Object value : resolvedValues) {
          StringBuilder buf2 = new StringBuilder(buf);
          buf2.append(value.toString());
          evaluate2(from + 1, record, buf2, results);
        }
      }
    }
  }
    
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Field {
    
    private final String name;
    
    public Field(String name) {
      assert name != null;
      this.name = name;
    }
    
    public String getName() {
      return name;
    }
    
    @Override
    public String toString() {
      return "Field:" + getName();
    }
  }
}
