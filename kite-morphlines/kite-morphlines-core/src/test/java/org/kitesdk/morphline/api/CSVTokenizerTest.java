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
package org.kitesdk.morphline.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader.CSVTokenizer;
import org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader.QuotedCSVTokenizer;
import org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader.SimpleCSVTokenizer;

public class CSVTokenizerTest extends Assert {
  
  @Test
  public void testBasic() throws Exception {
    assertEquals(Arrays.asList("hello", "world"), split("hello,world", ',', false));
    assertEquals(Arrays.asList(" hello", " world "), split(" hello| world ", '|', false));
    assertEquals(Arrays.asList("", "hello", "world", ""), split("|hello|world|", '|', false));
    assertEquals(Arrays.asList(""), split("", '|', false));
    assertEquals(Arrays.asList("", "", "x"), split("||x", '|', false));
  }
  
  private List<String> split(String line, char separator, boolean isQuoted) throws IOException {
    Record record = new Record();
    CSVTokenizer tokenizer;
    if (isQuoted) {
      tokenizer = new QuotedCSVTokenizer(separator, false, new ArrayList(), '"');
      tokenizer.tokenizeLine(line, new BufferedReader(new StringReader("")), record);      
    } else {
      tokenizer = new SimpleCSVTokenizer(separator, false, new ArrayList());
      tokenizer.tokenizeLine(line, null, record);
    }
    List results = new ArrayList();
    for (int i = 0; i < record.getFields().asMap().size(); i++) {
      assertEquals(1, record.get("column" + i).size());
      results.add(record.getFirstValue("column" + i));
    }
    return results;
  }
  
  @Test
  public void testQuoted() throws Exception {
    assertEquals(
        Arrays.asList("5", "orange", "This is a\nmulti, line text", "no\""), 
        split("5,orange,\"This is a\nmulti, line text\",no\"\"", ',', true)
    );
  }

  @Test(expected=IllegalStateException.class)
  public void testIllegalQuotedState() throws Exception {
    split("foo,maybe\"", ',', true);
  }

}
