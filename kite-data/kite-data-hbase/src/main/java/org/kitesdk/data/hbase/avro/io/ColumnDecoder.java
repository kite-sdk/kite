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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An Avro Decoder implementation used for decoding Avro instances from HBase
 * columns. This is basically an Avro BinaryDecoder with custom encoding of int,
 * long, and String types.
 * 
 * int and long are serialized in standard 4 and 8 byte format (instead of
 * Avro's ZigZag encoding) so that we can use HBase's atomic increment
 * functionality on columns.
 *
 * Strings are encoded as UTF-8 bytes. This is consistent
 * with HBase, and will allow appends in the future.
 */
public class ColumnDecoder extends Decoder {

  private final BinaryDecoder wrappedDecoder;
  private final InputStream in;
  private final DataInputStream dataIn;

  public ColumnDecoder(InputStream in) {
    this.in = in;
    this.wrappedDecoder = new DecoderFactory().binaryDecoder(in, null);
    this.dataIn = new DataInputStream(in);
  }

  @Override
  public void readNull() throws IOException {
    wrappedDecoder.readNull();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return wrappedDecoder.readBoolean();
  }

  @Override
  public int readInt() throws IOException {
    byte[] b = new byte[4];
    dataIn.readFully(b);
    return Bytes.toInt(b);
  }

  @Override
  public long readLong() throws IOException {
    byte[] b = new byte[8];
    dataIn.readFully(b);
    return Bytes.toLong(b);
  }

  @Override
  public float readFloat() throws IOException {
    return wrappedDecoder.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return wrappedDecoder.readDouble();
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    int bytesAvailable = in.available(); // assumes 'in' is ByteArrayInputStream so knows length
    byte[] bytes = new byte[bytesAvailable];
    in.read(bytes);
    return new Utf8(bytes);
  }

  @Override
  public String readString() throws IOException {
    return readString(null).toString();
  }

  @Override
  public void skipString() throws IOException {
    int bytesAvailable = in.available(); // assumes 'in' is ByteArrayInputStream so knows length
    in.skip(bytesAvailable);
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return wrappedDecoder.readBytes(old);
  }

  @Override
  public void skipBytes() throws IOException {
    wrappedDecoder.skipBytes();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    wrappedDecoder.readFixed(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    wrappedDecoder.skipFixed(length);
  }

  @Override
  public int readEnum() throws IOException {
    return wrappedDecoder.readEnum();
  }

  @Override
  public long readArrayStart() throws IOException {
    return wrappedDecoder.readArrayStart();
  }

  @Override
  public long arrayNext() throws IOException {
    return wrappedDecoder.arrayNext();
  }

  @Override
  public long skipArray() throws IOException {
    return wrappedDecoder.skipArray();
  }

  @Override
  public long readMapStart() throws IOException {
    return wrappedDecoder.readMapStart();
  }

  @Override
  public long mapNext() throws IOException {
    return wrappedDecoder.mapNext();
  }

  @Override
  public long skipMap() throws IOException {
    return wrappedDecoder.skipMap();
  }

  @Override
  public int readIndex() throws IOException {
    return wrappedDecoder.readIndex();
  }

}
