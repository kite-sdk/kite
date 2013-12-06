/**
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
package org.kitesdk.data.hbase.avro.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ColumnDecoderTest {

  @Test
  public void testDecodeInt() throws Exception {
    InputStream in = new ByteArrayInputStream(new byte[] { (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x01});
    Decoder decoder = new ColumnDecoder(in);
    int i = decoder.readInt();
    assertEquals(1, i);

    in = new ByteArrayInputStream(new byte[] { (byte) 0xff, (byte) 0xff,
        (byte) 0xff, (byte) 0xff });
    decoder = new ColumnDecoder(in);
    i = decoder.readInt();
    assertEquals(-1, i);

    in = new ByteArrayInputStream(new byte[] { (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00 });
    decoder = new ColumnDecoder(in);
    i = decoder.readInt();
    assertEquals(0, i);
  }

  @Test
  public void testDecodeLong() throws Exception {
    InputStream in = new ByteArrayInputStream(new byte[] { (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x01 });
    Decoder decoder = new ColumnDecoder(in);
    long i = decoder.readLong();
    assertEquals(1L, i);

    in = new ByteArrayInputStream(new byte[] { (byte) 0xff, (byte) 0xff,
        (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
        (byte) 0xff });
    decoder = new ColumnDecoder(in);
    i = decoder.readLong();
    assertEquals(-1L, i);

    in = new ByteArrayInputStream(new byte[] { (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00 });
    decoder = new ColumnDecoder(in);
    i = decoder.readLong();
    assertEquals(0L, i);
  }

  @Test
  public void testReadBytes() throws Exception {
    InputStream in = new ByteArrayInputStream(new byte[] { (byte) 0x06,
        (byte) 0x01, (byte) 0x00, (byte) 0xff });
    Decoder decoder = new ColumnDecoder(in);
    ByteBuffer bytes = decoder.readBytes(null);
    assertArrayEquals(new byte[]{(byte) 0x01, (byte) 0x00, (byte) 0xff},
        bytes.array());
  }

  @Test
  public void testReadString() throws Exception {
    String s = "hello";
    InputStream in = new ByteArrayInputStream(s.getBytes("UTF-8"));
    Decoder decoder = new ColumnDecoder(in);
    assertEquals(s, decoder.readString());

    in = new ByteArrayInputStream(s.getBytes("UTF-8"));
    decoder = new ColumnDecoder(in);
    assertEquals(s, decoder.readString(new Utf8()).toString());
  }

  @Test
  public void testReadEncoderOutput() throws Exception {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    Encoder encoder = new ColumnEncoder(byteOutputStream);
    encoder.writeFloat(1.1f);
    encoder.flush();
    InputStream in = new ByteArrayInputStream(byteOutputStream.toByteArray());
    Decoder decoder = new ColumnDecoder(in);
    float readFloat = decoder.readFloat();
    assertEquals(1.1f, readFloat, 0.0001);

    byteOutputStream = new ByteArrayOutputStream();
    encoder = new ColumnEncoder(byteOutputStream);
    encoder.writeDouble(1.1d);
    encoder.flush();
    in = new ByteArrayInputStream(byteOutputStream.toByteArray());
    decoder = new ColumnDecoder(in);
    double readDouble = decoder.readDouble();
    assertEquals(1.1d, readDouble, 0.0001);

    byteOutputStream = new ByteArrayOutputStream();
    encoder = new ColumnEncoder(byteOutputStream);
    encoder.writeString("hello there");
    encoder.flush();
    in = new ByteArrayInputStream(byteOutputStream.toByteArray());
    decoder = new ColumnDecoder(in);
    Utf8 readString = decoder.readString(null);
    assertEquals("hello there", readString.toString());
  }
}
