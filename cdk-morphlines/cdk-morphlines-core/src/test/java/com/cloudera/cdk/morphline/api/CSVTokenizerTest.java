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
package com.cloudera.cdk.morphline.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.fastreader.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.fastreader.SimpleCSVTokenizer;

public class CSVTokenizerTest extends Assert {
  
  @Test
  public void testBasic() throws Exception {
    assertEquals(Arrays.asList("hello", "world"), split("hello,world", ','));
    assertEquals(Arrays.asList(" hello", " world "), split(" hello| world ", '|'));
    assertEquals(Arrays.asList("", "hello", "world", ""), split("|hello|world|", '|'));
    assertEquals(Arrays.asList(""), split("", '|'));
    assertEquals(Arrays.asList("", "", "x"), split("||x", '|'));
  }
  
  private List<String> split(String line, char separator) throws IOException {
    List<String> columns = new ArrayList();
    CSVStrategy strategy = new CSVStrategy(separator, '"', "#", false, false);
    new SimpleCSVTokenizer().tokenizeLine(line, strategy, null, columns);
    return columns;
  }
}
