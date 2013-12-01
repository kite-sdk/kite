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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.util.Utf8;
import org.apache.avro.io.Encoder;

/**
 * A class that will encode Avro types, whose sort order can be determined by a
 * memcmp.
 */
public class MemcmpEncoder extends Encoder {
  private OutputStream out;

  public MemcmpEncoder(OutputStream out) {
    this.out = out;
  }

  @Override
  public void flush() throws IOException {
    if (out != null) {
      out.flush();
    }
  }

  @Override
  public void writeNull() throws IOException {
  }

  /**
   * A boolean is encoded as a byte, with 1 being true, and 0 being false.
   * 
   * @param b
   *          The boolean to encode.
   */
  @Override
  public void writeBoolean(boolean b) throws IOException {
    out.write(b ? 1 : 0);
  }

  /**
   * An int is written by flipping the sign bit, and writing it as a big endian
   * int.
   * 
   * @param n
   *          The int to encode.
   */
  @Override
  public void writeInt(int n) throws IOException {
    byte[] intBytes = new byte[] { (byte) ((n >>> 24) ^ 0x80),
        (byte) (n >>> 16), (byte) (n >>> 8), (byte) n };
    out.write(intBytes);
  }

  /**
   * A long is written by flipping the sign bit, and writing it as a big endian
   * long.
   * 
   * @param n
   *          The long to encode.
   */
  @Override
  public void writeLong(long n) throws IOException {
    byte[] intBytes = new byte[] { (byte) ((n >>> 56) ^ 0x80),
        (byte) (n >>> 48), (byte) (n >>> 40), (byte) (n >>> 32),
        (byte) (n >>> 24), (byte) (n >>> 16), (byte) (n >>> 8), (byte) n };
    out.write(intBytes);
  }

  /**
   * A float is written as 4 bytes. It is encoded as a 32 bit integer using the
   * int encoding defined in this class. If it is a negative number, the
   * complement of the 31 bits (every bit except the sign bit) is taken. This is
   * to ensure proper ordering of negative numbers.
   * 
   * @param f
   *          The float to encode.
   */
  @Override
  public void writeFloat(float f) throws IOException {
    int n = Float.floatToIntBits(f);
    if ((n >>> 31) > 0) {
      n ^= 0x7fffffff;
    }
    writeInt(n);
  }

  /**
   * A double is written as 8 bytes. It is encoded as a 64 bit long using the
   * long encoding defined in this class. If it is a negative number, the
   * complement of the 63 bits (every bit except the sign bit) is taken. This is
   * to ensure proper ordering of negative numbers.
   * 
   * @param d
   *          The double to encode.
   */
  @Override
  public void writeDouble(double d) throws IOException {
    long n = Double.doubleToLongBits(d);
    if ((n >>> 63) > 0) {
      n ^= 0x7fffffffffffffffL;
    }
    writeLong(n);
  }

  /**
   * Strings are encoded by fetching the bytes of the UTF8 encoding, and using
   * the writeBytes method of this class.
   * 
   * @param utf8
   *          The utf8 string to encode.
   */
  @Override
  public void writeString(Utf8 utf8) throws IOException {
    writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
  }

  /**
   * Fixed values are encoded as the raw bytes.
   * 
   * @param bytes
   *          The bytes to encode.
   * @param start
   *          The start of the byte array to encode.
   * @param len
   *          The length of the byte array to encode.
   */
  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    out.write(bytes, start, len);
  }

  /**
   * Bytes are encoded by writing all bytes out as themselves, except for bytes
   * of value 0x00. Bytes of value 0x00 are encoded as two bytes, 0x00 0x01. The
   * end marker is signified by two 0x00 bytes. This guarantees that the end
   * marker is the least possible value.
   * 
   * @param bytes
   *          The bytes to encode.
   */
  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    writeBytes(bytes.array(), bytes.position(), bytes.remaining());
  }

  /**
   * Bytes are encoded by writing all bytes out as themselves, except for bytes
   * of value 0x00. Bytes of value 0x00 are encoded as two bytes, 0x00 0x01. The
   * end marker is signified by two 0x00 bytes. This guarantees that the end
   * marker is the least possible value.
   * 
   * @param bytes
   *          The bytes to encode.
   * @param start
   *          The start of the byte array to encode.
   * @param len
   *          The length of the byte array to encode.
   */
  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    for (int i = start; i < start + len; ++i) {
      if (bytes[i] == 0x00) {
        out.write(0);
        out.write(1);
      } else {
        out.write((int) bytes[i]);
      }
    }
    out.write(0);
    out.write(0);
  }

  /**
   * Enums are encoded using the Integer encoding of this class.
   */
  @Override
  public void writeEnum(int e) throws IOException {
    writeInt(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
  }

  /**
   * Each item in an array is prepended by a 1 byte. This is to ensure that one
   * array longer than another with equal elements up to that point will always
   * be greater.
   */
  @Override
  public void startItem() throws IOException {
    out.write(1);
  }

  /**
   * Arrays are appended with a 0 byte.
   */
  @Override
  public void writeArrayEnd() throws IOException {
    out.write(0);
  }

  /**
   * Memcmp encoding for maps not supported, since ordering of Map in Avro is
   * undefined.
   */
  @Override
  public void writeMapStart() throws IOException {
    throw new IOException("MemcmpEncoder does not support writing Map types.");
  }

  /**
   * Memcmp encoding for maps not supported, since ordering of Map in Avro is
   * undefined.
   */
  @Override
  public void writeMapEnd() throws IOException {
    throw new IOException("MemcmpEncoder does not support writing Map types.");
  }

  /**
   * Union indexes are written using the int encoding of this class.
   * 
   * @param unionIndex
   *          The union index to encode.
   */
  @Override
  public void writeIndex(int unionIndex) throws IOException {
    writeInt(unionIndex);
  }
}
