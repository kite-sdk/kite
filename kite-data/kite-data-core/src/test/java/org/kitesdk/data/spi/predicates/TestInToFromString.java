/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.predicates;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

public class TestInToFromString {
  private static final Schema STRING = Schema.create(Schema.Type.STRING);
  private static final Schema BOOL = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema INT = Schema.create(Schema.Type.INT);
  private static final Schema LONG = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE = Schema.create(Schema.Type.DOUBLE);
  private static final Schema BYTES = Schema.create(Schema.Type.BYTES);
  private static final Schema ARRAY = Schema.createArray(INT);

  @Test
  public void testSingleStringValue() {
    Assert.assertEquals("In<Utf8>#toString(STRING)",
        "a", Predicates.in(new Utf8("a")).toString(STRING));
    Assert.assertEquals("In#toString(STRING)",
        "a", Predicates.in("a").toString(STRING));
    Assert.assertEquals("In.fromString(String, STRING)",
        Predicates.in("a"), In.<String>fromString("a", STRING));
  }

  @Test
  public void testSingleStringValueUrlEncoded() {
    Assert.assertEquals("In<Utf8>#toString(STRING)",
        "a%2Cb", Predicates.in(new Utf8("a,b")).toString(STRING));
    Assert.assertEquals("In.fromString(String, STRING)",
        Predicates.in("a,b"), In.<String>fromString("a%2Cb", STRING));
  }

  @Test
  public void testMultipleStringValues() {
    Assert.assertEquals("In<Utf8>#toString(STRING)",
        "a,b,c",
        Predicates.in(new Utf8("a"), new Utf8("b"), new Utf8("c")).toString(STRING));
    Assert.assertEquals("In#toString(STRING)",
        "a,b,c", Predicates.in("a", "b", "c").toString(STRING));
    Assert.assertEquals("In.fromString(String, STRING)",
        Predicates.in("a", "b", "c"), In.<String>fromString("a,b,c", STRING));
  }

  @Test
  public void testSingleBooleanValue() {
    Assert.assertEquals("In#toString(BOOL)",
        "false", Predicates.in(false, false).toString(BOOL));
    Assert.assertEquals("In.fromString(String, BOOL)",
        Predicates.in(false), In.<Boolean>fromString("34", BOOL));
  }

  @Test
  public void testMultipleBooleanValues() {
    Assert.assertEquals("In#toString(BOOL)",
        "false,true", Predicates.in(false,true).toString(BOOL));
    Assert.assertEquals("In.fromString(String, BOOL)",
        Predicates.in(false,true), In.<Boolean>fromString("false,true", BOOL));
  }

  @Test
  public void testSingleIntegerValue() {
    Assert.assertEquals("In#toString(INT)",
        "34", Predicates.in(34).toString(INT));
    Assert.assertEquals("In.fromString(String, INT)",
        Predicates.in(34), In.<Integer>fromString("34", INT));
  }

  @Test
  public void testMultipleIntegerValues() {
    Assert.assertEquals("In#toString(INT)",
        "3,4,5", Predicates.in(3,4,5).toString(INT));
    Assert.assertEquals("In.fromString(String, INT)",
        Predicates.in(3,4,5), In.<Integer>fromString("3,4,5", INT));
  }

  @Test
  public void testSingleLongValue() {
    Assert.assertEquals("In#toString(LONG)",
        "34", Predicates.in(34l).toString(LONG));
    Assert.assertEquals("In.fromString(String, LONG)",
        Predicates.in(34l), In.<Long>fromString("34", LONG));
  }

  @Test
  public void testMultipleLongValues() {
    Assert.assertEquals("In#toString(LONG)",
        "3,4,5", Predicates.in(3l,4l,5l).toString(LONG));
    Assert.assertEquals("In.fromString(String, LONG)",
        Predicates.in(3l,4l,5l), In.<Long>fromString("3,4,5", LONG));
  }

  @Test
  public void testSingleFloatValue() {
    Assert.assertEquals("In#toString(FLOAT)",
        "34.56", Predicates.in(34.56f).toString(FLOAT));
    Assert.assertEquals("In.fromString(String, FLOAT)",
        Predicates.in(34.56f), In.<Float>fromString("34.56", FLOAT));
  }

  @Test
  public void testMultipleFloatValues() {
    Assert.assertEquals("In#toString(FLOAT)",
        "34.0,5.6", Predicates.in(34.0f,5.6f).toString(FLOAT));
    Assert.assertEquals("In.fromString(String, FLOAT)",
        Predicates.in(34.0f, 5.6f), In.<Float>fromString("34.0,5.6", FLOAT));
  }

  @Test
  public void testSingleDoubleValue() {
    Assert.assertEquals("In#toString(DOUBLE)",
        "34.56", Predicates.in(34.56).toString(DOUBLE));
    Assert.assertEquals("In.fromString(String, DOUBLE)",
        Predicates.in(34.56), In.<Double>fromString("34.56", DOUBLE));
  }

  @Test
  public void testMultipleDoubleValues() {
    Assert.assertEquals("In#toString(DOUBLE)",
        "34.0,5.6", Predicates.in(34.0,5.6).toString(DOUBLE));
    Assert.assertEquals("In.fromString(String, DOUBLE)",
        Predicates.in(34.0,5.6), In.<Double>fromString("34.0,5.6", DOUBLE));
  }

  @Test
  public void testSingleBytesValue() {
    ByteBuffer data = ByteBuffer.wrap(new byte[] {'a', 'b', 'c'});
    Assert.assertEquals("In#toString(BYTES)",
        "BmFiYw", Predicates.in(data).toString(BYTES));
    Assert.assertEquals("In.fromString(String, BYTES)",
        Predicates.in(data), In.<ByteBuffer>fromString("BmFiYw", BYTES));
  }

  @Test
  public void testMultipleBytesValues() {
    ByteBuffer data1 = ByteBuffer.wrap(new byte[] {'a', 'b', 'c'});
    ByteBuffer data2 = ByteBuffer.wrap(new byte[] {'d', 'e', 'f'});
    Assert.assertEquals("In#toString(BYTES)",
        "BmFiYw,BmRlZg", Predicates.in(data1, data2).toString(BYTES));
    Assert.assertEquals("In.fromString(String, BYTES)",
        Predicates.in(data1, data2), In.<ByteBuffer>fromString("BmFiYw,BmRlZg", BYTES));
  }

  @Test
  public void testSingleArrayValue() {
    List<Integer> list = Lists.newArrayList(1,2,3);
    Assert.assertEquals("In#toString(ARRAY)",
        "BgIEBgA", Predicates.in((Object) list).toString(ARRAY));
    Assert.assertEquals("In.fromString(String, ARRAY)",
        Predicates.in((Object) list), In.fromString("BgIEBgA", ARRAY));
  }

  @Test
  public void testMultipleArrayValues() {
    List<Integer> list1 = Lists.newArrayList(1,2,3);
    List<Integer> list2 = Lists.newArrayList(4,5,6);
    Assert.assertEquals("In#toString(ARRAY)",
        "BgIEBgA,BggKDAA", Predicates.in((Object) list1, list2).toString(ARRAY));
    Assert.assertEquals("In.fromString(String, ARRAY)",
        Predicates.in((Object) list1, list2), In.fromString("BgIEBgA,BggKDAA", ARRAY));
  }
}
