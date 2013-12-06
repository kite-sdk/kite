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
import java.io.EOFException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.util.Utf8;
import org.apache.avro.io.Decoder;

/**
 * A class that will decode Avro types, whose sort order can be determined by a
 * memcmp. Decodes avro types encoded with the MemcmpEncoder class. See that
 * class for information on how each type of value is encoded.
 */
public class MemcmpDecoder extends Decoder {
  private InputStream in;

  public MemcmpDecoder(InputStream in) {
    this.in = in;
  }

  @Override
  public void readNull() throws IOException {
  }

  /**
   * A boolean is decoded where a byte value of 1 is true, and 0 is false.
   * 
   * @return The decoded boolean.
   */
  @Override
  public boolean readBoolean() throws IOException {
    int byteRead = in.read();
    if (byteRead == -1) {
      throw new EOFException();
    }
    return byteRead > 0;
  }

  /**
   * A int was written by flipping the sign bit, and writing it as a big endian
   * int.
   * 
   * To decode, we do the reverse, flipping the sign bit, and reading the int
   * from the bytes.
   * 
   * @return The decoded int.
   */
  @Override
  public int readInt() throws IOException {
    byte[] intBytes = new byte[4];
    int i = in.read(intBytes);
    if (i < 4) {
      throw new EOFException();
    }
    intBytes[0] = (byte) (intBytes[0] ^ 0x80);

    int value = 0;
    for (int j = 0; j < intBytes.length; ++j) {
      value = (value << 8) + (intBytes[j] & 0xff);
    }
    return value;
  }

  /**
   * A long was written by flipping the sign bit, and writing it as a big endian
   * long.
   * 
   * To decode, we do the reverse, flipping the sign bit, and reading the long
   * from the bytes.
   * 
   * @return The decoded long.
   */
  @Override
  public long readLong() throws IOException {
    byte[] longBytes = new byte[8];
    int i = in.read(longBytes);
    if (i < 8) {
      throw new EOFException();
    }
    longBytes[0] = (byte) (longBytes[0] ^ 0x80);

    long value = 0;
    for (int j = 0; j < longBytes.length; ++j) {
      value = (value << 8) + (longBytes[j] & 0xff);
    }
    return value;
  }

  /**
   * A float was written as 4 bytes. It was encoded as a 32 bit integer. If it
   * was a negative number, the complement of the 31 bits (every bit except the
   * sign bit) was taken.
   * 
   * To decode, we read the 4 byte integer. If the most significant bit is 1, we
   * take the compliment of the lower 31 bits. Finally we convert this integer
   * to float using Float.intBitsToFloat.
   * 
   * @return The decoded float.
   */
  @Override
  public float readFloat() throws IOException {
    int floatAsInt = readInt();
    if ((floatAsInt >>> 31) > 0) {
      floatAsInt ^= 0x7FFFFFFF;
    }
    return Float.intBitsToFloat(floatAsInt);
  }

  /**
   * A double was written as 8 bytes. It was encoded as a 64 bit long. If it was
   * a negative number, the complement of the 63 bits (every bit except the sign
   * bit) was taken.
   * 
   * To decode, we read the 8 byte long. If the most significant bit is 1, we
   * take the compliment of the lower 63 bits. Finally we convert this double to
   * long using Double.longBitsToDouble.
   * 
   * @return The decoded double.
   */
  @Override
  public double readDouble() throws IOException {
    long doubleAsLong = readLong();
    if ((doubleAsLong >>> 63) > 0) {
      doubleAsLong ^= 0x7FFFFFFFFFFFFFFFL;
    }
    return Double.longBitsToDouble(doubleAsLong);
  }

  /**
   * A string is decoded by reading the string as bytes using the readBytes
   * function.
   * 
   * @return The decoded String.
   */
  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    ByteBuffer stringBytes = readBytes(null);
    return new Utf8(stringBytes.array());
  }

  @Override
  public String readString() throws IOException {
    return readString(null).toString();
  }

  /**
   * To skip a string, we have to read it since the memcmp encoder doesn't
   * length prefix the string.
   */
  @Override
  public void skipString() throws IOException {
    readString(null);
  }

  /**
   * Bytes are decoded by reading each byte until we find two consecutive 0
   * bytes. A 0 byte followed by a 1 byte is translated into a 0 byte.
   * 
   * @return the decoded byte buffer.
   */
  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    while (true) {
      int byteRead = in.read();
      if (byteRead < 0) {
        throw new EOFException();
      }
      if (byteRead == 0) {
        int secondByteRead = in.read();
        if (byteRead < 0) {
          throw new EOFException();
        }
        if (secondByteRead == 0) {
          break;
        } else if (secondByteRead == 1) {
          bytesOut.write(0);
        } else {
          String msg = "Illegal encoding. 0 byte cannot be followed by "
              + "anything other than 0 or 1. It was followed by "
              + Integer.toString(byteRead);
          throw new IOException(msg);
        }
      } else {
        bytesOut.write(byteRead);
      }
    }
    return ByteBuffer.wrap(bytesOut.toByteArray());
  }

  /**
   * To skip bytes, we have to read the bytes, since we aren't length prefixing
   * the byte array.
   */
  public void skipBytes() throws IOException {
    readBytes(null);
  }

  /**
   * A fixed is decoded by just reading length bytes, and placing the bytes read
   * into the bytes array, starting at index start.
   * 
   * @param bytes
   *          The bytes array to populate.
   * @param start
   *          The index in bytes to place the read bytes.
   * @param length
   *          The number of bytes to read.
   */
  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    int i = in.read(bytes, start, length);
    if (i < length) {
      throw new EOFException();
    }
  }

  /**
   * Skips the fixed amount in the input stream.
   * 
   * @param length
   *          The length of the fixed to skip.
   */
  @Override
  public void skipFixed(int length) throws IOException {
    in.skip(length);
  }

  /**
   * Enums are decoded by reading the integer representation of the enum.
   * 
   * @return The integer rep of the Enum.
   */
  @Override
  public int readEnum() throws IOException {
    return readInt();
  }

  /**
   * Array starts with a 1 if the array size is larger than 0, else it starts
   * with a 0.
   * 
   * @return 1 if the array size is larger than 0, else 0.
   */
  @Override
  public long readArrayStart() throws IOException {
    return (long) readByte();
  }

  /**
   * Each element of the array is prefixed with 1. The end of the array is a 0.
   * 
   * @return 0 if there are no more elements in the array, else return 1.
   */
  @Override
  public long arrayNext() throws IOException {
    return (long) readByte();
  }

  /**
   * Return the array element prefix, or the end of the array.
   * 
   * @return 0 if we are at the end of the array, else 1.
   */
  @Override
  public long skipArray() throws IOException {
    return (long) readByte();
  }

  @Override
  public long readMapStart() throws IOException {
    throw new IOException("MemcmpDecoder does not support reading Map types.");
  }

  @Override
  public long mapNext() throws IOException {
    throw new IOException("MemcmpDecoder does not support reading Map types.");
  }

  @Override
  public long skipMap() throws IOException {
    throw new IOException("MemcmpDecoder does not support reading Map types.");
  }

  /**
   * Union index decoded as an Integer using the int decoding defined in this
   * class.
   * 
   * @return the union index.
   */
  @Override
  public int readIndex() throws IOException {
    return readInt();
  }

  /**
   * Read a single byte from the input stream, and return it.
   * 
   * @return the byte read.
   */
  private byte readByte() throws IOException {
    int byteRead = in.read();
    if (byteRead == -1) {
      throw new EOFException();
    }
    return (byte) byteRead;
  }
}
