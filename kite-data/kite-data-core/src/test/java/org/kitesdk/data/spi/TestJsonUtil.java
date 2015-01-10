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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.node.NullNode;
import org.junit.Assert;
import org.junit.Test;

public class TestJsonUtil {
  @Test
  public void testSchemaInferencePrimitiveTypes() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredBoolean("aBool")
        .requiredString("aString")
        .requiredInt("anInt")
        .requiredLong("aLong")
        .requiredDouble("aDouble")
        .requiredString("bytes")
        .endRecord();

    String encoded = BinaryNode.valueOf("soap".getBytes("utf-8")).toString();
    String jsonSample = "{" +
        "\"aBool\": false," +
        "\"aString\": \"triangle\"," +
        "\"anInt\": 34," +
        "\"aLong\": 1420502567564," +
        "\"aDouble\": 1420502567564.9," +
        "\"bytes\": " + encoded +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchema(datum, "Test"));

    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aBool", false);
    expected.put("aString", "triangle");
    expected.put("anInt", 34);
    expected.put("aLong", 1420502567564L);
    expected.put("aDouble", 1420502567564.9);
    expected.put("bytes", encoded.substring(1, encoded.length() - 1));
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testSchemaInferencePrimitiveArray() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredString("aString")
        .name("anArray").type().array().items().intType().noDefault()
        .endRecord();

    String jsonSample = "{" +
        "\"aString\": \"triangle\"," +
        "\"anArray\": [ 1, 2, 3, 4 ]" +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchema(datum, "Test"));

    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aString", "triangle");
    expected.put("anArray", Lists.newArrayList(1, 2, 3, 4));
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testSchemaInferenceNullablePrimitiveArray() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredString("aString")
        .name("anArray").type().array().items()
            .unionOf().nullType().and().intType().endUnion().noDefault()
        .endRecord();

    String jsonSample = "{" +
        "\"aString\": \"triangle\"," +
        "\"anArray\": [ null, 1, 2, 3, 4 ]" +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchema(datum, "Test"));

    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aString", "triangle");
    expected.put("anArray", Lists.newArrayList(null, 1, 2, 3, 4));
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testSchemaInferenceMultipleTypes() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredString("aString")
        .name("anArray").type().array().items()
        .unionOf().nullType().and().intType().and().stringType().endUnion().noDefault()
        .endRecord();

    String jsonSample = "{" +
        "\"aString\": \"triangle\"," +
        "\"anArray\": [ null, 1, 2, 3, \"winter\" ]" +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchema(datum, "Test"));

    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aString", "triangle");
    expected.put("anArray", Lists.newArrayList(null, 1, 2, 3, "winter"));
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testSchemaInferenceRecord() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredString("aString")
        .name("aRecord").type().record("aRecord").fields()
            .requiredString("left")
            .requiredString("right")
            .endRecord().noDefault()
        .endRecord();

    String jsonSample = "{" +
        "\"aString\": \"triangle\"," +
        "\"aRecord\": { \"left\": \"timid\", \"right\": \"dictionary\" }" +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchema(datum, "Test"));

    GenericData.Record aRecord = new GenericData.Record(
        recordSchema.getField("aRecord").schema());
    aRecord.put("left", "timid");
    aRecord.put("right", "dictionary");
    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aString", "triangle");
    expected.put("aRecord", aRecord);
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testSchemaInferenceMap() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredString("aString")
        .name("aMap").type().map().values().stringType().noDefault()
        .endRecord();

    String jsonSample = "{" +
        "\"aString\": \"triangle\"," +
        "\"aMap\": { \"left\": \"timid\", \"right\": \"dictionary\" }" +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchemaWithMaps(datum, "Test"));

    Map<String, Object> aMap = Maps.newLinkedHashMap();
    aMap.put("left", "timid");
    aMap.put("right", "dictionary");
    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aString", "triangle");
    expected.put("aMap", aMap);
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testSchemaInferenceNullableMap() throws Exception {
    Schema recordSchema = SchemaBuilder.record("Test").fields()
        .requiredString("aString")
        .name("aMap").type().map().values()
            .unionOf().nullType().and().stringType().endUnion().noDefault()
        .endRecord();

    String jsonSample = "{" +
        "\"aString\": \"triangle\"," +
        "\"aMap\": { \"left\": null, \"right\": \"dictionary\" }" +
        "}";

    JsonNode datum = JsonUtil.parse(jsonSample);
    Assert.assertEquals("Should produce expected schema",
        recordSchema, JsonUtil.inferSchemaWithMaps(datum, "Test"));

    Map<String, Object> aMap = Maps.newLinkedHashMap();
    aMap.put("left", null);
    aMap.put("right", "dictionary");
    GenericData.Record expected = new GenericData.Record(recordSchema);
    expected.put("aString", "triangle");
    expected.put("aMap", aMap);
    Assert.assertEquals("Should convert to record",
        expected, convertGeneric(datum, recordSchema));
  }

  @Test
  public void testJsonStream() throws Exception {
    String jsonSample = "{\"id\": 1}\n{\"id\": 2}";
    Iterator<JsonNode> iter = JsonUtil.parser(
        new ByteArrayInputStream(jsonSample.getBytes("utf-8")));
    Assert.assertEquals("Should produce 2 records",
        2, Lists.newArrayList(iter).size());

    jsonSample = "{\"id\": 1}{\"id\": 2}";
    iter = JsonUtil.parser(
        new ByteArrayInputStream(jsonSample.getBytes("utf-8")));
    Assert.assertEquals("Should produce 2 records",
        2, Lists.newArrayList(iter).size());

    jsonSample = "{\"id\": 1} {\"id\": 2}";
    iter = JsonUtil.parser(
        new ByteArrayInputStream(jsonSample.getBytes("utf-8")));
    Assert.assertEquals("Should produce 2 records",
        2, Lists.newArrayList(iter).size());

    jsonSample = "{\"id\": 1}\t{\"id\": 2}";
    iter = JsonUtil.parser(
        new ByteArrayInputStream(jsonSample.getBytes("utf-8")));
    Assert.assertEquals("Should produce 2 records",
        2, Lists.newArrayList(iter).size());
  }

  @Test
  public void testSimpleSchemaMerge() throws Exception {
    String jsonSample = "{\"id\": 1}\n{\"id\": 2}";

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.create(Schema.Type.INT), null, null)
    ));

    Assert.assertEquals("Should match expected schema",
        schema, mergeSchemas(jsonSample));
  }

  @Test
  public void testSchemaMergeRecords() throws Exception {
    // record schemas are never unnamed, so even if two records appear to not
    // share fields in common, they are considered the same type if the nested
    // names match.
    String jsonSample = "{\"id\": 1, \"record\": {\"jam\": -6.5}}" +
        "{\"id\": 2, \"record\": {\"heist\": 10.0}}";

    Schema inner = Schema.createRecord("record", null, null, false);
    inner.setFields(ImmutableList.of(
        new Schema.Field("jam", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.DOUBLE)
        )), null, NullNode.getInstance()),
        new Schema.Field("heist", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.DOUBLE)
        )), null, NullNode.getInstance())
    ));

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("record", inner, null, null)
    ));

    Assert.assertEquals("Should match expected schema",
        schema, mergeSchemas(jsonSample));
  }

  @Test
  public void testSchemaMergeUnionPrimitiveTypes() throws Exception {
    String jsonSample = "{\"id\": 1}\n{\"id\": 2}{\"id\": \"socket\"}";

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.STRING)
        )), null, null)
    ));

    Assert.assertEquals("Should match expected schema",
        schema, mergeSchemas(jsonSample));
  }

  @Test
  public void testSchemaMergeAddsNullableFields() throws Exception {
    String jsonSample = "{\"id\": 1}\n{\"id\": 2}" +
        "{\"id\": \"socket\", \"sparse\": \"tenfold\"}";

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.STRING)
        )), null, null),
        new Schema.Field("sparse", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING)
        )), null, NullNode.getInstance())
    ));

    Assert.assertEquals("Should match expected schema",
        schema, mergeSchemas(jsonSample));
  }

  @Test
  public void testSchemaMergeArrayTypes() throws Exception {
    String jsonSample = "{\"id\": 1}\n{\"id\": 2}" +
        "{\"id\": \"socket\", \"anArray\": [33, 34, 35]}" +
        "{\"id\": 3, \"anArray\": [\"badger\", \"porcupine\"]}";

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.STRING)
        )), null, null),
        new Schema.Field("anArray", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.NULL),
            Schema.createArray(Schema.createUnion(ImmutableList.of(
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING)
            )))
        )), null, NullNode.getInstance())
    ));

    Assert.assertEquals("Should match expected schema",
        schema, mergeSchemas(jsonSample));
  }

  @Test
  public void testSchemaMergeMapTypes() throws Exception {
    String jsonSample = "{\"id\": 1}\n{\"id\": 2}" +
        "{\"id\": \"socket\", \"aMap\": {\"coffee\": 17}}" +
        "{\"id\": 3, \"aMap\": {\"badger\": \"porcupine\"}}";

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.STRING)
        )), null, null),
        new Schema.Field("aMap", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.NULL),
            Schema.createMap(Schema.createUnion(ImmutableList.of(
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING)
            )))
        )), null, NullNode.getInstance())
    ));

    Assert.assertEquals("Should match expected schema",
        schema, mergeSchemasWithMaps(jsonSample));
  }

  @Test
  public void testSchemaMergeUnionTypes() throws Exception {
    String jsonArraySample = "{\"id\": 1}\n{\"id\": 2}" +
        "{\"id\": \"socket\", \"union\": [33, 34, 35]}" +
        "{\"id\": 3, \"union\": [\"badger\", \"porcupine\"]}";

    String jsonMapSample = "{\"id\": 1}\n{\"id\": 2}" +
        "{\"id\": \"socket\", \"union\": {\"coffee\": 17}}" +
        "{\"id\": 3, \"union\": {\"badger\": \"porcupine\"}}";

    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(ImmutableList.of(
        new Schema.Field("id", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.STRING)
        )), null, null),
        new Schema.Field("union", Schema.createUnion(ImmutableList.of(
            Schema.create(Schema.Type.NULL),
            Schema.createArray(Schema.createUnion(ImmutableList.of(
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING)
            ))),
            Schema.createMap(Schema.createUnion(ImmutableList.of(
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING)
            )))
        )), null, NullNode.getInstance())
    ));

    Assert.assertEquals("Should match expected schema",
        schema,
        SchemaUtil.merge(
            mergeSchemas(jsonArraySample),
            mergeSchemasWithMaps(jsonMapSample))
    );
  }

  private static Schema mergeSchemas(String jsonSample) throws Exception {
    return merge(Iterators.transform(JsonUtil.parser(
            new ByteArrayInputStream(jsonSample.getBytes("utf-8"))),
        new Function<JsonNode, Schema>() {
          @Override
          public Schema apply(JsonNode node) {
            return JsonUtil.inferSchema(node, "Test");
          }
        }));
  }

  private static Schema mergeSchemasWithMaps(String jsonSample) throws Exception {
    return merge(Iterators.transform(JsonUtil.parser(
            new ByteArrayInputStream(jsonSample.getBytes("utf-8"))),
        new Function<JsonNode, Schema>() {
          @Override
          public Schema apply(JsonNode node) {
            return JsonUtil.inferSchemaWithMaps(node, "Test");
          }
        }));
  }

  private static Schema merge(Iterator<Schema> schemas) {
    if (!schemas.hasNext()) {
      return null;
    }

    Schema result = schemas.next();
    while (schemas.hasNext()) {
      result = SchemaUtil.merge(result, schemas.next());
    }

    return result;
  }

  private static Object convertGeneric(JsonNode datum, Schema schema) {
    return JsonUtil.convertToAvro(GenericData.get(), datum, schema);
  }
}
