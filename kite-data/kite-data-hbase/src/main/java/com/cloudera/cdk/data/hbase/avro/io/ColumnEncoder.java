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
package com.cloudera.cdk.data.hbase.avro.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

/* An Avro Encoder implementation used for encoding Avro
 * instances to HBase columns. This is basically an
 * Avro BinaryEncoder with custom encoding of int,
 * long, and String types.
 * 
 * int and long are serialized in standard 4 and 8 byte
 * format (instead of Avro's ZigZag encoding) so that
 * we can use HBase's atomic increment functionality on
 * columns.
 * 
 * Strings are encoding as UTF-8 bytes. This is for
 * backward compatibility reasons, and is something
 * we want to change in the future.
 */
public class ColumnEncoder extends Encoder {

  private final BinaryEncoder wrappedEncoder;
  private final OutputStream out;

  public ColumnEncoder(OutputStream out) {
    this.out = out;
    wrappedEncoder = new EncoderFactory().binaryEncoder(out, null);
  }
  
  public ColumnEncoder(OutputStream out, ColumnEncoder reuse) {
    this.out = out;
    wrappedEncoder = new EncoderFactory().binaryEncoder(out, reuse.wrappedEncoder);
  }

  @Override
  public void flush() throws IOException {
    wrappedEncoder.flush();
  }

  @Override
  public void writeNull() throws IOException {
    wrappedEncoder.writeNull();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    wrappedEncoder.writeBoolean(b);
  }

  @Override
  public void writeInt(int n) throws IOException {
    DataOutputStream dataOut = new DataOutputStream(out);
    dataOut.writeInt(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    DataOutputStream dataOut = new DataOutputStream(out);
    dataOut.writeLong(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    wrappedEncoder.writeFloat(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    wrappedEncoder.writeDouble(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    byte[] bytes = utf8.toString().getBytes("UTF-8");
    out.write(bytes);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    wrappedEncoder.writeBytes(bytes);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    wrappedEncoder.writeBytes(bytes, start, len);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    wrappedEncoder.writeFixed(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    wrappedEncoder.writeEnum(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
    wrappedEncoder.writeArrayStart();
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    wrappedEncoder.setItemCount(itemCount);
  }

  @Override
  public void startItem() throws IOException {
    wrappedEncoder.startItem();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    wrappedEncoder.writeArrayEnd();
  }

  @Override
  public void writeMapStart() throws IOException {
    wrappedEncoder.writeMapStart();
  }

  @Override
  public void writeMapEnd() throws IOException {
    wrappedEncoder.writeMapEnd();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    wrappedEncoder.writeIndex(unionIndex);
  }

}
