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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.kitesdk.data.PartitionStrategy;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Constraints}
 */
public class TestConstraintsSerialization {

  private static final Schema SCHEMA = SchemaBuilder.record("Event").fields()
    .requiredString("id")
    .requiredLong("timestamp")
    .requiredString("color")
    .optionalInt("year")
    .endRecord();

  private static final Schema COMPLEX_SCHEMA = SchemaBuilder.record("Calendar").fields()
    .name("meeting").type().record("appt").fields().requiredString("name").endRecord().noDefault()
    .endRecord();

  @Test
  public void testInSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "id";
    Constraints inConstraint = new Constraints(SCHEMA).with(propertyName, "abc");

    Constraints newIn = serializeAndDeserialize(inConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, equalTo(inConstraint.get(propertyName)));
  }

  @Test
  public void testInAvroSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "meeting";

    Schema schema = COMPLEX_SCHEMA.getField(propertyName).schema();
    GenericData.Record record = new GenericData.Record(schema);
    record.put("name", "MeetingName");

    Constraints inConstraint = new Constraints(COMPLEX_SCHEMA).with(propertyName, record);

    Constraints newIn = serializeAndDeserialize(inConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, equalTo(inConstraint.get(propertyName)));
  }

  @Test
  public void testInUtf8Serialization() throws IOException, ClassNotFoundException {
    String propertyName = "id";
    Constraints inConstraint = new Constraints(SCHEMA).with(propertyName,
        new Utf8("abc"));

    Constraints newIn = serializeAndDeserialize(inConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, equalTo(inConstraint.get(propertyName)));
  }


  @Test
  public void testExistsSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "year";
    Constraints exists = new Constraints(SCHEMA).with(propertyName, new Object[0]);

    Constraints newExists = serializeAndDeserialize(exists);

    Predicate predicate = newExists.get(propertyName);
    assertThat(predicate, equalTo(exists.get(propertyName)));
  }

  @Test
  public void testRangeAvroSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "meeting";

    Schema schema = COMPLEX_SCHEMA.getField(propertyName).schema();
    GenericData.Record record = new GenericData.Record(schema);
    record.put("name", "MeetingName");
    Constraints rangeConstraint = new Constraints(COMPLEX_SCHEMA).from(propertyName, record);

    Constraints newIn = serializeAndDeserialize(rangeConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, equalTo(rangeConstraint.get(propertyName)));
  }

  @Test
  public void testRangeFromSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).from(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testRangeFromAfterSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).fromAfter(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }


  @Test
  public void testRangeToSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).to(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testRangeToBeforeSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).toBefore(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testFromToSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).from(propertyName, 1L).to(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testFromToBeforeSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).from(propertyName, 1L).toBefore(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testFromAfterToSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).from(propertyName, 1L).to(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testFromAfterToBeforeSerialization() throws IOException, ClassNotFoundException {
    String propertyName = "timestamp";
    Constraints range = new Constraints(SCHEMA).fromAfter(propertyName, 1L).toBefore(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, equalTo(range.get(propertyName)));
  }

  @Test
  public void testMultiplePredicatesSerialization() throws IOException, ClassNotFoundException {
    String rangeName = "timestamp";
    Constraints constraints = new Constraints(SCHEMA).fromAfter(rangeName, 1L).toBefore(rangeName, 10L);
    String existsName = "year";
    constraints = constraints.with(existsName);
    String inName = "id";
    constraints = constraints.with(inName, "hello");

    Constraints newConstraints = serializeAndDeserialize(constraints);
    assertThat(newConstraints.get(rangeName), equalTo(constraints.get(rangeName)));
    assertThat(newConstraints.get(existsName), equalTo(constraints.get(existsName)));
    assertThat(newConstraints.get(inName), equalTo(constraints.get(inName)));
  }

  @Test
  public void testPartitionFieldSerialization()
      throws IOException, ClassNotFoundException {
    Constraints constraints = new Constraints(
        SCHEMA, new PartitionStrategy.Builder().identity("color").build());
    constraints = constraints.with("color", "orange", "blue");
    Constraints newConstraints = serializeAndDeserialize(constraints);
    Assert.assertEquals(constraints, newConstraints);
  }

  public Constraints serializeAndDeserialize(Constraints constraints) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);

    out.writeObject(constraints);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bais);

    return (Constraints) in.readObject();
  }
}
