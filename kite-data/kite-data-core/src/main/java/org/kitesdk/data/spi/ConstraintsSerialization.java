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

package org.kitesdk.data.spi;

import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;

/**
 * Serialization Utils for serializing {@link Constraints}
 */
class ConstraintsSerialization {

  /**
   * Writes the {@code predicates} for the {@code schema} to the {@code out} stream.
   * @param schema The schema described by the predicates
   * @param predicates the constraints to serialize.
   * @param out the stream for writing constraints.
   * @throws IOException error writing the constraints.
   */
  public static void writeConstraints(
      Schema schema, PartitionStrategy strategy,
      Map<String, Predicate> predicates, ObjectOutputStream out)
      throws IOException {
    out.writeInt(predicates.size());
    for (Map.Entry<String, Predicate> entry: predicates.entrySet()) {
      out.writeUTF(entry.getKey());
      writePredicate(
          SchemaUtil.fieldSchema(schema, strategy, entry.getKey()),
          entry.getValue(), out);
    }
  }

  /**
   * Read the {@code predicates} for the {@code schema} from the {@code out} stream.
   * @param schema The schema described by the predicates
   * @param in the stream for reading constraints.
   * @return the constraints read from the stream.
   * @throws IOException error reading the constraints.
   */
  public static Map<String, Predicate> readConstraints(
      Schema schema, PartitionStrategy strategy, ObjectInputStream in)
      throws IOException {
    int numPredicates = in.readInt();
    Map<String, Predicate> predicates = new HashMap<String, Predicate>();
    for(int i = 0; i < numPredicates; i++){
      String name = in.readUTF();
      Predicate predicate = readPredicate(
          SchemaUtil.fieldSchema(schema, strategy, name), in);
      predicates.put(name, predicate);
    }

    return predicates;
  }

  /**
   * Writes out the individual {@link Predicate}.  Currently this only supports a {@code predicate} of one of the
   * following types:
   *
   * <ul>
   *   <li>{@link org.kitesdk.data.spi.predicates.In}</li>
   *   <li>{@link org.kitesdk.data.spi.predicates.Exists}</li>
   *   <li>{@link com.google.common.collect.Range}</li>
   * </ul>
   * @param fieldSchema schema for the predicate's field.
   * @param predicate The predicate to serialize.
   * @param out the stream to write out the serialized predicate.
   * @throws java.io.IOException error persisting the predicate.
   */
  private static void writePredicate(Schema fieldSchema, Predicate predicate, ObjectOutputStream out) throws IOException {
    out.writeUTF(predicate.getClass().getName());
    if (predicate instanceof In) {
      writeInPredicate(fieldSchema, (In) predicate, out);
    } else if (predicate instanceof Range) {
      writeRangePredicate(fieldSchema, (Range) predicate, out);
    }
  }

  /**
   * Reads in a predicate from the {@code in} stream.
   * @param fieldSchema the {@link Schema} for the predicate being read.
   * @param in the stream from which to read the serialized predicate.
   * @return the serialized predicate.
   * @throws IOException error reading the predicate
   */
  private static Predicate readPredicate(Schema fieldSchema, ObjectInputStream in) throws IOException{
    String className = in.readUTF();
    if (className.equals(In.class.getName())) {
      return readInPredicate(fieldSchema, in);
    } else if (className.equals(Range.class.getName())) {
      return readRangePredicate(fieldSchema, in);
    } else if (className.equals(Exists.class.getName())) {
      return Predicates.exists();
    }
    throw new IOException("Unable to deserialize predicate of type "+className);
  }


  /**
   * Serializes an {@link org.kitesdk.data.spi.predicates.In} predicate to the stream {@code out}.
   */
  private static void writeInPredicate(Schema fieldSchema, In in, ObjectOutputStream out) throws IOException{
    Set values = Predicates.asSet(in);
    out.writeInt(values.size());
    for (Object value: values) {
      writeValue(fieldSchema, value, out);
    }
  }

  /**
   * Deserializes an {@link org.kitesdk.data.spi.predicates.In} predicate from the stream {@code In}.
   */
  private static In readInPredicate(Schema fieldSchema, ObjectInputStream in) throws IOException{
    int numValues = in.readInt();
    Set<Object> values = new HashSet<Object>();
    for (int i = 0; i < numValues; i++) {
      values.add(readValue(fieldSchema, in));
    }
    return Predicates.in(values);
  }

  /**
   * Serializes an {@link Range} into the specified {@code out} stream.
   */
  private static void writeRangePredicate(Schema fieldSchema, Range range, ObjectOutputStream out) throws IOException{
    if (range.hasLowerBound()) {
      //write out that there is a lower endpoint and the value.
      out.writeBoolean(true);
      out.writeBoolean(range.isLowerBoundOpen());
      writeValue(fieldSchema, range.lowerEndpoint(), out);
    } else {
      //Write that there is no lower bound
      out.writeBoolean(false);
    }

    if (range.hasUpperBound()) {
      out.writeBoolean(true);
      out.writeBoolean(range.isUpperBoundOpen());
      //write out that there is a lower endpoint and the value.
      writeValue(fieldSchema, range.upperEndpoint(), out);
    } else {
      //write out that there is not an upper bound
      out.writeBoolean(false);
    }
  }

  /**
   * Deserializes an {@link Range} from the specified {@code in} stream.
   */
  @SuppressWarnings("unchecked")
  private static Range readRangePredicate(Schema fieldSchema, ObjectInputStream in) throws IOException{
    Range range = null;

    //read in boolean indicating if there is a lower bound
    if (in.readBoolean()) {
      BoundType lowerType = in.readBoolean() ? BoundType.OPEN : BoundType.CLOSED;
      Comparable lowerBound = (Comparable) readValue(fieldSchema, in);
      if (lowerType.equals(BoundType.OPEN)) {
        range = Ranges.greaterThan(lowerBound);
      } else {
        range = Ranges.atLeast(lowerBound);
      }
    }
    //read in boolean indicating if there is an upper bound
    if (in.readBoolean()) {
      Range upperRange = null;
      BoundType upperType = in.readBoolean() ? BoundType.OPEN : BoundType.CLOSED;
      Comparable upperBound = (Comparable) readValue(fieldSchema, in);
      if (upperType.equals(BoundType.OPEN)) {
        upperRange = Ranges.lessThan(upperBound);
      } else {
        upperRange = Ranges.atMost(upperBound);
      }
      range = range == null ? upperRange : range.intersection(upperRange);
    }

    return range;
  }

  /**
   * Serializes the {@code value} to the specified {@code out} stream.  The value is expected to be either
   * {@link java.io.Serializable} or an Avro {@link org.apache.avro.generic.IndexedRecord record}.
   */
  @SuppressWarnings("unchecked")
  private static void writeValue(Schema fieldSchema, Object value, ObjectOutputStream out) throws IOException{
    if (value instanceof Serializable) {
      //write out that the value is not an Avro object
      out.writeBoolean(false);
      out.writeObject(value);
    } else {
      //write a true boolean indicating it is an avro object
      out.writeBoolean(true);

      DatumWriter writer = new SpecificDatumWriter(fieldSchema);

      //Write out the value
      ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(byteOutStream, null);
      writer.write(value, encoder);
      encoder.flush();
      byte[] bytes = byteOutStream.toByteArray();
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  /**
   * Deserializes the {@code value} from the specified {@code in} stream.
   */
  @SuppressWarnings("unchecked")
  private static Object readValue(Schema fieldSchema, ObjectInputStream in) throws IOException {
    boolean isAvro = in.readBoolean();
    if (isAvro) {
      int numBytes = in.readInt();
      byte[] bytes = new byte[numBytes];
      int bytesRead = in.read(bytes);
      assert numBytes == bytesRead;

      ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);
      Decoder decoder = DecoderFactory.get().binaryDecoder(byteInputStream, null);
      DatumReader reader = new SpecificDatumReader(fieldSchema);
      return reader.read(null, decoder);
    }
    else{
      try{
        return in.readObject();
      }catch(ClassNotFoundException cnfe){
        throw new IOException();
      }
    }
  }
}
