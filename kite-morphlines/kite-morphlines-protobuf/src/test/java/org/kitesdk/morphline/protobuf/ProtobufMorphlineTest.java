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
package org.kitesdk.morphline.protobuf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.protobuf.Protos.Complex;
import org.kitesdk.morphline.protobuf.Protos.Complex.Link;
import org.kitesdk.morphline.protobuf.Protos.Complex.Name;
import org.kitesdk.morphline.protobuf.Protos.Complex.Type;
import org.kitesdk.morphline.protobuf.Protos.RepeatedLongs;

public class ProtobufMorphlineTest extends AbstractMorphlineTest {

  private static final List<Long> LIST_OF_LONGS = Arrays.asList(1l, 10l, 100l, 1000l, 10000l, 100000l);
  private static RepeatedLongs repeatedLongs;
  private static Complex complex;

  private static byte[] nameBytes;
  private static byte[] link1Bytes;
  private static byte[] link2Bytes;

  @BeforeClass
  public static void prepareInputExtractProtoPaths() {

    Name.Builder nameBuilder = Complex.Name.newBuilder();
    nameBuilder.setFloatVal(5f).setIntVal(100).setLongVal(5000l);
    nameBuilder.addAllStringVal(Arrays.asList("All", "you", "need", "is", "money"));
    nameBuilder.setRepeatedLong(RepeatedLongs.newBuilder().addAllLongVal(LIST_OF_LONGS));
    Name name = nameBuilder.build();
    Link link1 = Link.newBuilder().addLanguage("CZ").addLanguage("EN-US").setUrl("http://google.com").build();
    Link link2 = Link.newBuilder().addLanguage("RU").setUrl("https://vk.com/").build();

    Complex.Builder complexBuilder = Complex.newBuilder();
    complexBuilder.setDocId(5);
    complexBuilder.setName(name);
    complexBuilder.addLink(link1);
    complexBuilder.addLink(link2);
    complexBuilder.setType(Type.UPDATE);
    complex = complexBuilder.build();

    nameBytes = name.toByteArray();
    link1Bytes = link1.toByteArray();
    link2Bytes = link2.toByteArray();
  }

  @BeforeClass
  public static void prepareInputReadProto() {
    RepeatedLongs.Builder repeatedLongsBuilder = RepeatedLongs.newBuilder();
    repeatedLongsBuilder.addAllLongVal(LIST_OF_LONGS);
    repeatedLongs = repeatedLongsBuilder.build();
  }

  @Test
  public void testExtractProtoPaths() throws Exception {
    morphline = createMorphline("test-morphlines/extractProtoPaths");
    InputStream in = new ByteArrayInputStream(complex.toByteArray());
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);

    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    in.close();
    assertEquals(1, collector.getRecords().size());

    Record firstRecord = collector.getFirstRecord();
    assertEquals(Arrays.asList(complex.getDocId()), firstRecord.get("docId"));
    assertArrayEquals(nameBytes, (byte[]) firstRecord.getFirstValue("name"));
    assertEquals(Arrays.asList(complex.getName().getIntVal()), firstRecord.get("intVal"));
    assertEquals(Arrays.asList(complex.getName().getLongVal()), firstRecord.get("longVal"));
    assertFalse(firstRecord.getFields().containsKey("doubleVal"));
    assertEquals(Arrays.asList(complex.getName().getFloatVal()), firstRecord.get("floatVal"));
    assertEquals(complex.getName().getStringValList(), firstRecord.get("stringVals"));
    assertEquals(LIST_OF_LONGS, firstRecord.get("longVals"));
    assertArrayEquals(link1Bytes, (byte[]) firstRecord.get("links").get(0));
    assertArrayEquals(link2Bytes, (byte[]) firstRecord.get("links").get(1));
    assertEquals(Arrays.asList("CZ", "EN-US", "RU"), firstRecord.get("languages"));
    assertEquals(Arrays.asList("http://google.com", "https://vk.com/"), firstRecord.get("urls"));
    assertEquals(Arrays.asList(Type.UPDATE.name()), firstRecord.get("type"));
  }

  @Test
  public void testReadProto() throws Exception {
    morphline = createMorphline("test-morphlines/readProto");
    for (int j = 0; j < 3; j++) { // also test reuse of objects and low level
                                  // avro buffers
      InputStream in = new ByteArrayInputStream(repeatedLongs.toByteArray());
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, in);

      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      in.close();

      Iterator<Record> iter = collector.getRecords().iterator();

      assertTrue(iter.hasNext());

      assertEquals(repeatedLongs, iter.next().getFirstValue(Fields.ATTACHMENT_BODY));
      assertFalse(iter.hasNext());
    }
  }
}
