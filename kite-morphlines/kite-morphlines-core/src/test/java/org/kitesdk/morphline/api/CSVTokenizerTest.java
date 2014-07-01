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

import com.google.common.collect.Lists;

public class CSVTokenizerTest extends Assert {
  
  @Test
  public void testBasic() throws Exception {
    split(Arrays.asList("hello", "world"), "hello,world", ',', false, true);
    split(Arrays.asList(" hello", " world "), " hello| world ", '|', false, true);
    split(Arrays.asList("", "hello", "world", ""), "|hello|world|", '|', false, true);
    split(Arrays.asList(""), "", '|', false, true);
    split(Arrays.asList("", "", "x"), "||x", '|', false, true);
    split(Lists.newArrayList(), "", '|', false, false);
    split(Arrays.asList("x"), "x", '|', false, false);
    split(Arrays.asList(null,"x"), "|x", '|', false, false);
    
    Record record = new Record();
    CSVTokenizer tokenizer = new SimpleCSVTokenizer(',', true, true, new ArrayList<String>());
    tokenizer.tokenizeLine(" x ", null, record);
    assertEquals(Arrays.asList("x"), record.get("column0"));
  }
  
  private void split(List expected, String line, char separator, boolean isQuoted, boolean addEmptyStrings) throws IOException {
    Record record = new Record();
    CSVTokenizer tokenizer;
    if (isQuoted) {
      tokenizer = new QuotedCSVTokenizer(separator, false, addEmptyStrings, new ArrayList<String>(), 1000, false, '"');
      tokenizer.tokenizeLine(line, new BufferedReader(new StringReader("")), record);      
    } else {
      tokenizer = new SimpleCSVTokenizer(separator, false, addEmptyStrings, new ArrayList<String>());
      tokenizer.tokenizeLine(line, null, record);
    }
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), record.getFirstValue("column" + i));
    }
    assertTrue(record.getFields().asMap().size() <= expected.size());
  }
  
  @Test
  public void testQuoted() throws Exception {
    char quote = '"';
    String doubleQuote = String.valueOf(quote) + String.valueOf(quote);
    split(
        Arrays.asList("5", "orange", "This is a\nmulti, line text", "no\""), 
        "5,orange,\"This is a\nmulti, line text\",no\"\"", ',', true, true);
    split(Arrays.asList("x", ""), "x|", '|', true, true);
    split(Arrays.asList("x", ""), "x|" + doubleQuote, '|', true, true);
    split(Lists.newArrayList(), doubleQuote, '|', true, true);
    split(Lists.newArrayList(), "", '|', true, true);
    split(Lists.newArrayList(), "", '|', true, false);
    split(Arrays.asList("x"), "x", '|', true, false);
    split(Arrays.asList(null,"x"), "|x", '|', true, false);
    split(Arrays.asList(null,"x"), doubleQuote + "|" + quote + "x" + quote, '|', true, false);
  }

  @Test(expected=IllegalStateException.class)
  public void testIllegalQuotedState() throws Exception {
    split(Lists.newArrayList(), "foo,maybe\"", ',', true, true);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testThrowExceptionIfRecordTooLong() throws Exception {
    boolean ignoreTooLongRecords = false;
    int maxCharactersPerRecord = 10;
    CSVTokenizer tokenizer = new QuotedCSVTokenizer(
        ',', false, false, new ArrayList<String>(), maxCharactersPerRecord, ignoreTooLongRecords, '"');
    tokenizer.tokenizeLine(
        "\"", 
        new BufferedReader(new StringReader("line tooooooooo long\"")), 
        new Record());
  }

  @Test
  public void testIgnoreRecordTooLong() throws Exception {
    boolean ignoreTooLongRecords = true;
    int maxCharactersPerRecord = 10;
    CSVTokenizer tokenizer = new QuotedCSVTokenizer(
        ',', false, false, new ArrayList<String>(), maxCharactersPerRecord, ignoreTooLongRecords, '"');
    assertFalse(tokenizer.tokenizeLine(
        "\"", 
        new BufferedReader(new StringReader("line tooooooooo long\"")), 
        new Record()));
  }

}
