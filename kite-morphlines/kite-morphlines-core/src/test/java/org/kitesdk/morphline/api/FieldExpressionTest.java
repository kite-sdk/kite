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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.base.FieldExpression;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings("unchecked")
public class FieldExpressionTest extends Assert {
  
  @Test
  public void testSimplePatterns() throws Exception {
    //             012345678901234567890123456789
    String expr = "Mr. @{first_name} age: @{age}";
    String regex = "@\\{(.*?)\\}";
    Matcher matcher = Pattern.compile(regex).matcher(expr);
    assertTrue(matcher.find());
    assertEquals("first_name", matcher.group(1));
    assertEquals(6, matcher.start(1));
    assertEquals(6 + "first_name".length(), matcher.end(1));
    assertTrue(matcher.find());
    assertEquals("age", matcher.group(1));
    assertEquals(25, matcher.start(1));
    assertEquals(25 + "age".length(), matcher.end(1));
    assertFalse(matcher.find());    
    
    matcher = Pattern.compile("foo").matcher("foo");
    assertTrue(matcher.matches());
    matcher = Pattern.compile("foo").matcher("barfoo");
    assertFalse(matcher.matches());
    matcher = Pattern.compile(".*foo").matcher("barfoo");
    assertTrue(matcher.matches());
  }
  
  @Test
  public void testSimpleExpression() throws Exception {
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    record.getFields().put("age", 8);
    record.getFields().put("tags", "one");
    record.getFields().put("tags", 2);
    record.getFields().put("tags", "three");
    
    assertEquals("foo", resolveExpressionSimple("foo", record));
    assertEquals("", resolveExpressionSimple("", record));
    assertEquals("Nadja", resolveExpressionSimple("@{first_name}", record));
    assertEquals("Ms. Nadja", resolveExpressionSimple("Ms. @{first_name}", record));
    assertEquals("Ms. Nadja is 8 years old.", resolveExpressionSimple("Ms. @{first_name} is @{age} years old.", record));
    
    assertEquals(Arrays.asList("Nadja"), resolveReference("@{first_name}", record));
    assertEquals(Arrays.asList("one", 2, "three"), resolveReference("@{tags}", record));
    try {
      resolveReference("first_name", record);
      fail();
    } catch (MorphlineCompilationException e) {
      ;
    }
  }
  
  @Test
  public void testExpression() throws Exception {
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    record.getFields().put("age", 8);
    record.getFields().put("tags", "one");
    record.getFields().put("tags", 2);
    record.getFields().put("tags", "three");
    record.getFields().put("likes", "pembo");
    record.getFields().put("likes", "shanti");
    record.getFields().put("hates", "brothers");
    record.getFields().put("hates", "milk");
    
    assertEquals(Arrays.asList("foo"), resolveExpression("foo", record));
    assertEquals(Arrays.asList(""), resolveExpression("", record));
    assertEquals(Arrays.asList("Nadja"), resolveExpression("@{first_name}", record));
    assertEquals(Arrays.asList("pembo", "shanti"), resolveExpression("@{likes}", record));
    assertEquals(Arrays.asList("Ms. Nadja"), resolveExpression("Ms. @{first_name}", record));
    assertEquals(Arrays.asList(), resolveExpression("Ms. @{nonExistingField}", record));
    assertEquals(Arrays.asList(), resolveExpression("@{nonExistingField}", record));
    
    assertEquals(Arrays.asList(
        "Ms. Nadja is 8 years old."), 
        resolveExpression("Ms. @{first_name} is @{age} years old.", record));

    assertEquals(Arrays.asList(
        "is one years", 
        "is 2 years", 
        "is three years"), 
        resolveExpression("is @{tags} years", record));
    
    assertEquals(Arrays.asList(
        "Ms. Nadja is one years old.", 
        "Ms. Nadja is 2 years old.", 
        "Ms. Nadja is three years old."), 
        resolveExpression("Ms. @{first_name} is @{tags} years old.", record));
    
    assertEquals(Arrays.asList(
        "She likes pembo and hates brothers.", 
        "She likes pembo and hates milk.",        
        "She likes shanti and hates brothers.", 
        "She likes shanti and hates milk."),
        resolveExpression("She likes @{likes} and hates @{hates}.", record));
    
    assertEquals(Arrays.asList("Nadja"), resolveReference("@{first_name}", record));
    assertEquals(Arrays.asList("one", 2, "three"), resolveReference("@{tags}", record));
    
    try {
      resolveReference("first_name", record);
      fail();
    } catch (MorphlineCompilationException e) {
      ;
    }
  }
  
  private List resolveExpression(String expr, Record record) {
    return new FieldExpression(expr, null).evaluate(record);
  }
  
  private List resolveReference(String expr, Record record) {
    return resolveReference(expr, record, ConfigFactory.empty());
  }
  
  // example: @{first_name}
  private static final Pattern PATTERN = Pattern.compile("@\\{.*?\\}");
  
  // TODO: optimize by not using regexes
  private List resolveReference(String reference, Record record, Config config) {
    Matcher matcher = PATTERN.matcher(reference);
    if (!matcher.matches()) {
      throw new MorphlineCompilationException("Invalid variable reference", config);
    }
    String value = reference.substring("@{".length(), reference.length() - "}".length());
    List resolvedValues = record.getFields().get(value);
    return resolvedValues;
  }

  // TODO: optimize by not using regexes
  private String resolveExpressionSimple(String expr, Record record) {
    Matcher matcher = PATTERN.matcher(expr);    
    StringBuilder buf = new StringBuilder();
    int from = 0;
    while (matcher.find()) {
      int start = matcher.start();
      int end = matcher.end();
      buf.append(expr.substring(from, start));
      String ref = expr.substring(start + "@{".length(), end - "}".length());
      if (ref.length() == 0) {
        buf.append(record.toString()); // @{} means dump string representation of entire record 
      } else {
        List resolvedValues = record.getFields().get(ref);
        Joiner.on(" ").appendTo(buf, resolvedValues);
      }
      from = end;
    }
    buf.append(expr.substring(from, expr.length()));
    return buf.toString();
  }

}
