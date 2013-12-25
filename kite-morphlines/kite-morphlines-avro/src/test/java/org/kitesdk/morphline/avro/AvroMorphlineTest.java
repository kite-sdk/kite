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
package org.kitesdk.morphline.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class AvroMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testToAvroBasic() throws Exception {
    Schema schema = new Parser().parse(new File("src/test/resources/test-avro-schemas/interop.avsc"));
    morphline = createMorphline("test-morphlines/toAvroWithSchemaFile");
    
    byte[] bytes = new byte[] {47, 13};
    byte[] fixed = new byte[16];
    Record jdoc1 = new Record();     
    jdoc1.put("_dataset_descriptor_schema", schema);
    collector.reset();
    assertFalse(morphline.process(jdoc1)); // "has no default value"

    jdoc1.put("intField", "notAnInteger");
    collector.reset();
    assertFalse(morphline.process(jdoc1)); // can't convert

    jdoc1.replaceValues("intField", "20");
    jdoc1.put("longField", "200");
    jdoc1.put("stringField", "abc");
    jdoc1.put("boolField", "true");
    jdoc1.put("floatField", "200");
    jdoc1.put("doubleField","200");
    jdoc1.put("bytesField", bytes);
    jdoc1.put("nullField", null);
    jdoc1.getFields().putAll("arrayField", Arrays.asList(10.0, 20.0));
    jdoc1.put("mapField", 
        new HashMap(ImmutableMap.of("myMap", 
          ImmutableMap.of("label", "car")
        ))
    );
    jdoc1.put("unionField", new ArrayList(Arrays.asList(bytes)));
    jdoc1.put("enumField", "B");
    jdoc1.put("fixedField", fixed);
    jdoc1.put("recordField", 
        ImmutableMap.of(  
            "label", "house",
            "children", new ArrayList(Arrays.asList(bytes)))
    );    
    collector.reset();
    assertTrue(morphline.process(jdoc1));
    
    GenericData.Record actual = (GenericData.Record) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_BODY);
    assertEquals(20, actual.get("intField"));
    assertEquals(123, actual.get("defaultIntField"));    
    assertEquals(200L, actual.get("longField"));
    assertEquals("abc", actual.get("stringField"));
    assertEquals(Boolean.TRUE, actual.get("boolField"));
    assertEquals(200.0f, actual.get("floatField"));
    assertEquals(200.0, actual.get("doubleField"));
    assertEquals(ByteBuffer.wrap(bytes), actual.get("bytesField"));
    assertNull(actual.get("nullField"));
    assertEquals(Arrays.asList(10.0, 20.0), actual.get("arrayField"));
    GenericData.Record expected = new GenericData.Record(schema.getField("mapField").schema().getValueType());
    expected.put("label", "car");
    assertEquals(ImmutableMap.of("myMap", expected), actual.get("mapField"));
    assertEquals(Arrays.asList(ByteBuffer.wrap(bytes)), actual.get("unionField"));
    assertEquals("B", actual.get("enumField"));
    assertEquals(
        new GenericData.Fixed(schema.getField("fixedField").schema(), fixed), 
        actual.get("fixedField"));
    expected = new GenericData.Record(schema.getField("recordField").schema());
    expected.put("label", "house");
    expected.put("children", new ArrayList(Arrays.asList(ByteBuffer.wrap(bytes))));
    assertEquals(expected, actual.get("recordField"));
  }

  @Test
  public void testToAvroWithUnion() throws Exception {
    morphline = createMorphline("test-morphlines/toAvro");
    
    List<Schema> types = Arrays.asList(
            Schema.create(Type.INT), 
            Schema.create(Type.LONG), 
            Schema.create(Type.FLOAT), 
            Schema.create(Type.DOUBLE), 
            Schema.create(Type.BOOLEAN), 
            Schema.create(Type.STRING), 
            Schema.create(Type.NULL));
    
    processAndVerifyUnion(5, 5, types);
    processAndVerifyUnion(5L, 5L, types);
    processAndVerifyUnion(5.0f, 5.0f, types);
    processAndVerifyUnion(5.0, 5.0, types);
    processAndVerifyUnion("5", "5", types);
    processAndVerifyUnion(Boolean.TRUE, Boolean.TRUE, types);
    processAndVerifyUnion(Boolean.FALSE, Boolean.FALSE, types);
    processAndVerifyUnion(null, null, types);
    processAndVerifyUnion(Arrays.asList(1, 2), "[1, 2]", types);

    types = Arrays.asList(
        Schema.create(Type.DOUBLE), 
        Schema.create(Type.INT)
        );
    processAndVerifyUnion("5", 5.0, types);

    
    types = Arrays.asList(
        Schema.create(Type.INT),
        Schema.create(Type.DOUBLE) 
        );
    processAndVerifyUnion("5", 5, types);
    
    
    types = Arrays.asList(
        Schema.create(Type.STRING),
        Schema.create(Type.DOUBLE) 
        );
    processAndVerifyUnion(5, "5", types);

    
    types = Arrays.asList(
        Schema.create(Type.DOUBLE), 
        Schema.create(Type.STRING)
        );
    processAndVerifyUnion(5, 5.0, types);

    
    Schema recordSchema = Schema.createRecord("Rec", "arec", null, false);
    recordSchema.setFields(Arrays.asList(new Field("foo", Schema.create(Type.STRING), null, null))); 
    types = Arrays.asList(
        Schema.create(Type.INT), 
        Schema.createMap(Schema.create(Type.STRING)),
        recordSchema 
        );
    Map<String, String> map = new HashMap(ImmutableMap.of("foo", "bar"));
    processAndVerifyUnion(map, new HashMap(map), types);

    
    types = Arrays.asList(
        Schema.create(Type.INT), 
        recordSchema, 
        Schema.createMap(Schema.create(Type.STRING))
        );
    GenericData.Record avroRecord = new GenericData.Record(recordSchema);
    avroRecord.put("foo", "bar");    
    processAndVerifyUnion(map, avroRecord, types);
  }
  
  private void processAndVerifyUnion(Object input, Object expected, List<Schema> types) {
    Schema documentSchema = Schema.createRecord("Doc", "adoc", null, false);
    Schema unionSchema = Schema.createUnion(types);
    documentSchema.setFields(Arrays.asList(new Field("price", unionSchema, null, null)));        

    GenericData.Record document1 = new GenericData.Record(documentSchema);
    document1.put("price", expected);    

    Record jdoc1 = new Record();     
    jdoc1.put("_dataset_descriptor_schema", documentSchema);
    jdoc1.put("price", input);
    Record expect1 = jdoc1.copy();
    expect1.put(Fields.ATTACHMENT_BODY, document1);
    processAndVerifySuccess(jdoc1, expect1, false);  
  }
  
  @Test
  public void testAvroArrayUnionDocument() throws Exception {
    Schema documentSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();   
    Schema intArraySchema = Schema.createArray(Schema.create(Type.INT));
    Schema intArrayUnionSchema = Schema.createUnion(Arrays.asList(intArraySchema, Schema.create(Type.NULL)));
    Schema itemListSchema = Schema.createArray(intArrayUnionSchema);
    docFields.add(new Field("price", itemListSchema, null, null));
    documentSchema.setFields(docFields);        
//    System.out.println(documentSchema.toString(true));
    
//    // create record0
    GenericData.Record document0 = new GenericData.Record(documentSchema);
      document0.put("price", new GenericData.Array(itemListSchema, Arrays.asList(
          new GenericData.Array(intArraySchema, Arrays.asList(1, 2, 3, 4, 5)),
          new GenericData.Array(intArraySchema, Arrays.asList(10, 20)),
          null,
          null,
//          new GenericData.Array(intArraySchema, Arrays.asList()),
          new GenericData.Array(intArraySchema, Arrays.asList(100, 200)),
          null
//          new GenericData.Array(intArraySchema, Arrays.asList(1000))
      )));    

    GenericData.Record document1 = new GenericData.Record(documentSchema);
    document1.put("price", new GenericData.Array(itemListSchema, Arrays.asList(
        new GenericData.Array(intArraySchema, Arrays.asList(1000))
    )));    
    
    morphline = createMorphline("test-morphlines/extractAvroPaths");        
    {
      deleteAllDocuments();
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, document0);
      startSession();
  //    System.out.println(documentSchema.toString(true));
  //    System.out.println(document0.toString());
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getRecords().size());
      List expected = Arrays.asList(Arrays.asList(Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(10, 20), null, null, Arrays.asList(100, 200), null)); 
      //List expected2 = Arrays.asList(1, 2, 3, 4, 5, 10, 20, 100, 200); 
      assertEquals(expected, collector.getFirstRecord().get("/price"));
      assertEquals(expected, collector.getFirstRecord().get("/price/[]"));
//      assertEquals(expected, collector.getFirstRecord().get("/*"));
//      assertEquals(expected2, collector.getFirstRecord().get("/*/*"));
      assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
    }
    
    {
      deleteAllDocuments();
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, document1);
      startSession();
  //    System.out.println(documentSchema.toString(true));
  //    System.out.println(document1.toString());
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getRecords().size());
      List expected = Arrays.asList(Arrays.asList(Arrays.asList(1000)));
      assertEquals(expected, collector.getFirstRecord().get("/price"));
      assertEquals(expected, collector.getFirstRecord().get("/price/[]"));
      assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
    }
    
    morphline = createMorphline("test-morphlines/extractAvroPathsFlattened");        
    {
      deleteAllDocuments();
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, document0);
      startSession();
//      System.out.println(documentSchema.toString(true));
//      System.out.println(document0.toString());
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getRecords().size());
      List expected = Arrays.asList(1, 2, 3, 4, 5, 10, 20, 100, 200);
      assertEquals(expected, collector.getFirstRecord().get("/price"));
      assertEquals(expected, collector.getFirstRecord().get("/price/[]"));
      assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
    }
    
    ingestAndVerifyAvro(documentSchema, document0);
    ingestAndVerifyAvro(documentSchema, document0, document1);
    
    Record event = new Record();
    event.getFields().put(Fields.ATTACHMENT_BODY, document0);
    morphline = createMorphline("test-morphlines/extractAvroTree");
    deleteAllDocuments();
    System.out.println(document0);
    assertTrue(load(event));
    assertEquals(1, queryResultSetSize("*:*"));
    Record first = collector.getFirstRecord();    
    AbstractParser.removeAttachments(first);
    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 10, 20, 100, 200), first.get("/price"));
    assertEquals(1, first.getFields().asMap().size());
    
    {
      morphline = createMorphline("test-morphlines/toAvro");
      Record jdoc1 = new Record();     
      jdoc1.put("_dataset_descriptor_schema", documentSchema);
      jdoc1.put("price", Arrays.asList(1000));
      Record expect1 = jdoc1.copy();
      expect1.put(Fields.ATTACHMENT_BODY, document1);
      processAndVerifySuccess(jdoc1, expect1, false);
  
      Record jdoc0 = new Record();     
      jdoc0.put("_dataset_descriptor_schema", documentSchema);
      jdoc0.getFields().putAll("price", Arrays.asList(
          Arrays.asList(1, 2, 3, 4, 5),
          Arrays.asList(10, 20),
          null,
          null,
          Arrays.asList(100, 200),
          null
        )
      );
      Record expect0 = jdoc0.copy();
      expect0.put(Fields.ATTACHMENT_BODY, document0);
      processAndVerifySuccess(jdoc0, expect0, false);
    }
  }
  
  @Test
  public void testAvroComplexDocuments() throws Exception {
    Schema documentSchema = Schema.createRecord("Document", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();
    docFields.add(new Field("docId", Schema.create(Type.INT), null, null));
    
      Schema linksSchema = Schema.createRecord("Links", "alink", null, false);
      List<Field> linkFields = new ArrayList<Field>();
      linkFields.add(new Field("backward", Schema.createArray(Schema.create(Type.INT)), null, null));
      linkFields.add(new Field("forward", Schema.createArray(Schema.create(Type.INT)), null, null));
      linksSchema.setFields(linkFields);
      
      docFields.add(new Field("links", Schema.createUnion(Arrays.asList(linksSchema, Schema.create(Type.NULL))), null, null));
//      docFields.add(new Field("links", linksSchema, null, null));
      
      Schema nameSchema = Schema.createRecord("Name", "aname", null, false);
      List<Field> nameFields = new ArrayList<Field>();
      
        Schema languageSchema = Schema.createRecord("Language", "alanguage", null, false);
        List<Field> languageFields = new ArrayList<Field>();
        languageFields.add(new Field("code", Schema.create(Type.STRING), null, null));
//        docFields.add(new Field("links", Schema.createUnion(Arrays.asList(linksSchema, Schema.create(Type.NULL))), null, null));
        languageFields.add(new Field("country", Schema.createUnion(Arrays.asList(Schema.create(Type.STRING), Schema.create(Type.NULL))), null, null));
        languageSchema.setFields(languageFields);
        
      nameFields.add(new Field("language", Schema.createArray(languageSchema), null, null));
      nameFields.add(new Field("url", Schema.createUnion(Arrays.asList(Schema.create(Type.STRING), Schema.create(Type.NULL))), null, null));              
//      nameFields.add(new Field("url", Schema.create(Type.STRING), null, null));             
      nameSchema.setFields(nameFields);
      
    docFields.add(new Field("name", Schema.createArray(nameSchema), null, null));         
    documentSchema.setFields(docFields);    
    
//    System.out.println(documentSchema.toString(true));
    
    
    
    // create record0
    GenericData.Record document0 = new GenericData.Record(documentSchema);
    document0.put("docId", 10);
    
    GenericData.Record links = new GenericData.Record(linksSchema);
      links.put("forward", new GenericData.Array(linksSchema.getField("forward").schema(), Arrays.asList(20, 40, 60)));
      links.put("backward", new GenericData.Array(linksSchema.getField("backward").schema(), Arrays.asList()));

    document0.put("links", links);
      
      GenericData.Record name0 = new GenericData.Record(nameSchema);
      
        GenericData.Record language0 = new GenericData.Record(languageSchema);
        language0.put("code", "en-us");
        language0.put("country", "us");
        
        GenericData.Record language1 = new GenericData.Record(languageSchema);
        language1.put("code", "en");
        
      name0.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList(language0, language1)));
      name0.put("url", "http://A");
        
      GenericData.Record name1 = new GenericData.Record(nameSchema);
      name1.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList()));
      name1.put("url", "http://B");
      
      GenericData.Record name2 = new GenericData.Record(nameSchema);
      
      GenericData.Record language2 = new GenericData.Record(languageSchema);
      language2.put("code", "en-gb");
      language2.put("country", "gb");
            
      name2.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList(language2)));     
      
    document0.put("name", new GenericData.Array(documentSchema.getField("name").schema(), Arrays.asList(name0, name1, name2)));     
//    System.out.println(document0.toString());

    
    // create record1
    GenericData.Record document1 = new GenericData.Record(documentSchema);
    document1.put("docId", 20);
    
      GenericData.Record links1 = new GenericData.Record(linksSchema);
      links1.put("backward", new GenericData.Array(linksSchema.getField("backward").schema(), Arrays.asList(10, 30)));
      links1.put("forward", new GenericData.Array(linksSchema.getField("forward").schema(), Arrays.asList(80)));

    document1.put("links", links1);
      
      GenericData.Record name4 = new GenericData.Record(nameSchema);      
      name4.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList()));
      name4.put("url", "http://C");
        
    document1.put("name", new GenericData.Array(documentSchema.getField("name").schema(), Arrays.asList(name4)));     
    
    morphline = createMorphline("test-morphlines/extractAvroPaths");        
    {
      deleteAllDocuments();
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, document0);
      startSession();
//      System.out.println(documentSchema.toString(true));
//      System.out.println(document0.toString());
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getRecords().size());
      assertEquals(Arrays.asList(10), collector.getFirstRecord().get("/docId"));
      assertEquals(Arrays.asList(Arrays.asList()), collector.getFirstRecord().get("/links/backward"));
      List expected = Arrays.asList(Arrays.asList(20, 40, 60));
      assertEquals(expected, collector.getFirstRecord().get("/links/forward"));
      assertEquals(expected, collector.getFirstRecord().get("/links/forward/[]"));
      assertEquals(expected, collector.getFirstRecord().get("/links/forward[]"));
      assertEquals(Arrays.asList("en-us", "en", "en-gb"), collector.getFirstRecord().get("/name/[]/language/[]/code"));
      assertEquals(Arrays.asList("en-us", "en", "en-gb"), collector.getFirstRecord().get("/name[]/language[]/code"));
      assertEquals(Arrays.asList("us", "gb"), collector.getFirstRecord().get("/name/[]/language/[]/country"));
      assertEquals(Arrays.asList("us", "gb"), collector.getFirstRecord().get("/name[]/language[]/country"));
      assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
    }

    morphline = createMorphline("test-morphlines/extractAvroPathsFlattened");        
    {
      deleteAllDocuments();
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, document0);
      startSession();
//      System.out.println(documentSchema.toString(true));
//      System.out.println(document0.toString());
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getRecords().size());
      assertEquals(Arrays.asList(10), collector.getFirstRecord().get("/docId"));
      assertEquals(Arrays.asList(20, 40, 60), collector.getFirstRecord().get("/links"));    
      assertEquals(Arrays.asList(), collector.getFirstRecord().get("/links/backward"));
      List expected = Arrays.asList(20, 40, 60);
      assertEquals(expected, collector.getFirstRecord().get("/links/forward"));
      assertEquals(expected, collector.getFirstRecord().get("/links/forward/[]"));
      assertEquals(expected, collector.getFirstRecord().get("/links/forward[]"));
      assertEquals(Arrays.asList("en-us", "en", "en-gb"), collector.getFirstRecord().get("/name/[]/language/[]/code"));
      assertEquals(Arrays.asList("en-us", "en", "en-gb"), collector.getFirstRecord().get("/name[]/language[]/code"));
      assertEquals(Arrays.asList("us", "gb"), collector.getFirstRecord().get("/name/[]/language/[]/country"));
      assertEquals(Arrays.asList("us", "gb"), collector.getFirstRecord().get("/name[]/language[]/country"));
      assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
      expected = Arrays.asList("en-us", "us", "en", "http://A", "http://B", "en-gb", "gb");
      assertEquals(expected, collector.getFirstRecord().get("/name"));
    }
    
    ingestAndVerifyAvro(documentSchema, document0);
    ingestAndVerifyAvro(documentSchema, document0, document1);
    
    Record event = new Record();
    event.getFields().put(Fields.ATTACHMENT_BODY, document0);
    morphline = createMorphline("test-morphlines/extractAvroTree");
    deleteAllDocuments();
//    System.out.println(document0);
    assertTrue(load(event));
    assertEquals(1, queryResultSetSize("*:*"));
    Record first = collector.getFirstRecord();
    assertEquals(Arrays.asList("us", "gb"), first.get("/name/language/country"));
    assertEquals(Arrays.asList("en-us", "en", "en-gb"), first.get("/name/language/code"));
    assertEquals(Arrays.asList(20, 40, 60), first.get("/links/forward"));
    assertEquals(Arrays.asList("http://A", "http://B"), first.get("/name/url"));
    assertEquals(Arrays.asList(10), first.get("/docId"));
    AbstractParser.removeAttachments(first);
    assertEquals(5, first.getFields().asMap().size());

    {
      morphline = createMorphline("test-morphlines/toAvro");
      Record jdoc1 = new Record();     
      jdoc1.put("_dataset_descriptor_schema", documentSchema);
      jdoc1.put("docId", 20);
      jdoc1.put("links", 
          ImmutableMap.of(
            "backward", Arrays.asList(10, 30),
            "forward", Arrays.asList(80))
      );
      jdoc1.getFields().putAll("name", 
          Arrays.asList(
            ImmutableMap.of(  
              "language", Arrays.asList(),
              "url", "http://C"))
      );
      Record expect1 = jdoc1.copy();
      expect1.put(Fields.ATTACHMENT_BODY, document1);
      processAndVerifySuccess(jdoc1, expect1, false);
  
      Record jdoc0 = new Record();     
      jdoc0.put("_dataset_descriptor_schema", documentSchema);
      jdoc0.put("docId", 10);
      jdoc0.put("links", 
          ImmutableMap.of(
            "backward", Arrays.asList(),
            "forward", Arrays.asList(20, 40, 60))
      );
      
      jdoc0.getFields().putAll("name", 
          Arrays.asList(
            ImmutableMap.of(  
              "language", new ArrayList(Arrays.asList(
                  ImmutableMap.of("code", "en-us", "country", "us"),
                  ImmutableMap.of("code", "en"))),
              "url", "http://A"),
            ImmutableMap.of(  
              "language", Arrays.asList(),
              "url", "http://B"),
            ImmutableMap.of(  
              "language", new ArrayList(Arrays.asList(
                  ImmutableMap.of("code", "en-gb", "country", "gb")))
               )
          )
      );
      Record expect0 = jdoc0.copy();
      expect0.put(Fields.ATTACHMENT_BODY, document0);
      processAndVerifySuccess(jdoc0, expect0, false);
    }
  }
  
  @Test
  public void testMap() throws Exception {
    Schema schema = new Parser().parse(new File("src/test/resources/test-avro-schemas/intero1.avsc"));
    GenericData.Record document0 = new GenericData.Record(schema);
    Map map = new LinkedHashMap();
    Schema mapRecordSchema = schema.getField("mapField").schema().getValueType();
    GenericData.Record mapRecord = new GenericData.Record(mapRecordSchema); 
    mapRecord.put("label", "nadja");
    map.put(utf8("foo"), mapRecord);
    document0.put("mapField", map);

    morphline = createMorphline("test-morphlines/extractAvroPaths");        
    deleteAllDocuments();
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, document0);
    startSession();
//    System.out.println(schema.toString(true));
//    System.out.println(document0.toString());
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getRecords().size());
    assertEquals(Arrays.asList("nadja"), collector.getFirstRecord().get("/mapField/foo/label"));
    assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));

    morphline = createMorphline("test-morphlines/extractAvroPathsFlattened");        
    deleteAllDocuments();
    record = new Record();
    record.put(Fields.ATTACHMENT_BODY, document0);
    startSession();
//      System.out.println(documentSchema.toString(true));
//      System.out.println(document0.toString());
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getRecords().size());
    assertEquals(Arrays.asList("nadja"), collector.getFirstRecord().get("/mapField/foo/label"));
    assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
    
    ingestAndVerifyAvro(schema, document0);
    
    Record event = new Record();
    event.getFields().put(Fields.ATTACHMENT_BODY, document0);
    morphline = createMorphline("test-morphlines/extractAvroTree");
    deleteAllDocuments();
    //System.out.println(document0);
    assertTrue(load(event));
    assertEquals(1, queryResultSetSize("*:*"));
    Record first = collector.getFirstRecord();
    assertEquals(Arrays.asList("nadja"), first.get("/mapField/foo/label"));
    AbstractParser.removeAttachments(first);
    assertEquals(1, first.getFields().asMap().size());

    {
      morphline = createMorphline("test-morphlines/toAvro");
      Record jdoc0 = new Record();     
      jdoc0.put("_dataset_descriptor_schema", schema);
      jdoc0.put("mapField", new HashMap(ImmutableMap.of(
          utf8("foo"), ImmutableMap.of("label", "nadja")
          )) 
      );
      Record expect0 = jdoc0.copy();
      expect0.put(Fields.ATTACHMENT_BODY, document0);
      processAndVerifySuccess(jdoc0, expect0, false);  
      
      // verify that multiple maps can't be converted to a non-array schema
      jdoc0 = new Record();     
      jdoc0.put("_dataset_descriptor_schema", schema);
      jdoc0.put("mapField", new HashMap(ImmutableMap.of(
          utf8("foo"), ImmutableMap.of("label", "nadja")
          )) 
      );
      jdoc0.put("mapField", new HashMap(ImmutableMap.of(
          utf8("foo"), ImmutableMap.of("label", "nadja")
          )) 
      );
      collector.reset();
      assertFalse(morphline.process(jdoc0));
      
      // verify that an exception is raised if a required field is missing
      jdoc0 = new Record();     
      jdoc0.put("_dataset_descriptor_schema", schema);
      jdoc0.put("mapField", new HashMap(ImmutableMap.of(
          utf8("foo"), ImmutableMap.of()
          )) 
      );
      collector.reset();
      assertFalse(morphline.process(jdoc0));
      
      // verify that default field is used if value is missing
      Schema schema2 = new Parser().parse(new File("src/test/resources/test-avro-schemas/intero2.avsc"));
      jdoc0 = new Record();     
      jdoc0.put("_dataset_descriptor_schema", schema2);
      jdoc0.put("mapField", new HashMap(ImmutableMap.of(
          utf8("foo"), ImmutableMap.of()
          )) 
      );
      collector.reset();
      assertTrue(morphline.process(jdoc0));
      GenericData.Record result = (GenericData.Record) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_BODY);
      GenericData.Record result2 = (GenericData.Record) ((Map)result.get("mapField")).get(utf8("foo"));
      assertEquals("nadja", result2.get("label"));
    }
  }

  private void processAndVerifySuccess(Record input, Record expected, boolean isSame) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));
    assertEquals(expected, collector.getFirstRecord());
    if (isSame) {
      assertSame(input, collector.getFirstRecord());    
    } else {
      assertNotSame(input, collector.getFirstRecord());    
    }
  }
  
  private void ingestAndVerifyAvro(Schema schema, GenericData.Record... records) throws IOException {
    deleteAllDocuments();
    
    GenericDatumWriter datum = new GenericDatumWriter(schema);
    DataFileWriter writer = new DataFileWriter(datum);
    writer.setMeta("Meta-Key0", "Meta-Value0");
    writer.setMeta("Meta-Key1", "Meta-Value1");
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    writer.create(schema, bout);
    for (GenericData.Record record : records) {
      writer.append(record);
    }
    writer.flush();
    writer.close();

    DataFileReader<GenericData.Record> reader = new DataFileReader(new ReadAvroContainerBuilder.ForwardOnlySeekableInputStream(new ByteArrayInputStream(bout.toByteArray())), new GenericDatumReader());
    Schema schema2 = reader.getSchema();
    assertEquals(schema, schema2);
    for (GenericData.Record record : records) {
      assertTrue(reader.hasNext());
      GenericData.Record record2 = reader.next();
      assertEquals(record, record2);
    }
    assertFalse(reader.hasNext());
    reader.close();

    Record event = new Record();
    event.getFields().put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(bout.toByteArray()));
    morphline = createMorphline("test-morphlines/readAvroContainer");
    deleteAllDocuments();
    assertTrue(load(event));
    assertEquals(records.length, queryResultSetSize("*:*"));
        
    GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
    bout = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(bout, null);
    for (GenericData.Record record : records) {
      datumWriter.write(record, encoder);
    }
    encoder.flush();

    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bout.toByteArray()), null);
    DatumReader<GenericData.Record> datumReader = new GenericDatumReader<GenericData.Record>(schema);
    for (int i = 0; i < records.length; i++) {
      GenericData.Record record3 = datumReader.read(null, decoder);
      assertEquals(records[i], record3);
    }
    
    event = new Record();
    event.getFields().put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(bout.toByteArray()));
    File tmp = new File("target/tmp-test-schema.avsc");
    try {
      tmp.deleteOnExit();
      Files.write(schema.toString(true), tmp, Charsets.UTF_8);
      morphline = createMorphline("test-morphlines/readAvroWithExternalSchema");
      deleteAllDocuments();    
      assertTrue(load(event));
      assertEquals(records.length, queryResultSetSize("*:*"));
    } finally {
      tmp.delete();
    }
        
    for (GenericData.Record record : records) {
      event = new Record();
      event.getFields().put(Fields.ATTACHMENT_BODY, record);
      morphline = createMorphline("test-morphlines/extractAvroTree");
      deleteAllDocuments();
      assertTrue(load(event));
      assertEquals(1, queryResultSetSize("*:*"));
    }
    
    String[] formats = new String[] {"", "AndSnappy"};
    for (String format : formats) {
      morphline = createMorphline("test-morphlines/writeAvroToByteArrayWithContainer" + format);
      event = new Record();
      event.getFields().putAll(Fields.ATTACHMENT_BODY, Arrays.asList(records));
      deleteAllDocuments();
      assertTrue(load(event));
      assertEquals(1, collector.getFirstRecord().get(Fields.ATTACHMENT_BODY).size());
      byte[] bytes = (byte[]) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_BODY);
      assertNotNull(bytes);
      reader = new DataFileReader(new ReadAvroContainerBuilder.ForwardOnlySeekableInputStream(new ByteArrayInputStream(bytes)), new GenericDatumReader());
      assertEquals("bar", new String(reader.getMeta("foo"), Charsets.UTF_8));
      assertEquals("Nadja", new String(reader.getMeta("firstName"), Charsets.UTF_8));
      assertEquals(schema, reader.getSchema());
      for (GenericData.Record record : records) {
        assertTrue(reader.hasNext());
        GenericData.Record record2 = reader.next();
        assertEquals(record, record2);
      }
      assertFalse(reader.hasNext());
      reader.close();
    }
    
    formats = new String[] {"Binary", "JSON"};
    for (String format : formats) {
      morphline = createMorphline("test-morphlines/writeAvroToByteArrayWithContainerless" + format);
      event = new Record();
      event.getFields().putAll(Fields.ATTACHMENT_BODY, Arrays.asList(records));
      deleteAllDocuments();
      assertTrue(load(event));
      assertEquals(1, collector.getFirstRecord().get(Fields.ATTACHMENT_BODY).size());
      byte[] bytes = (byte[]) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_BODY);
      assertNotNull(bytes);
      if (format.equals("Binary")) {
        decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null);
      } else {
        decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes));
      }
      datumReader = new GenericDatumReader<GenericData.Record>(schema);
      for (int i = 0; i < records.length; i++) {
        GenericData.Record record3 = datumReader.read(null, decoder);
        assertEquals(records[i], record3);
      }
      try {
        datumReader.read(null, decoder);
        fail();
      } catch (EOFException e) {
        ; // expected
      }
    }
  }

  @Test
  public void testReadAvroWithMissingExternalSchema() throws Exception {
    try {
      morphline = createMorphline("test-morphlines/readAvroWithMissingExternalSchema");
      fail();
    } catch (MorphlineCompilationException e) {
      assertTrue(e.getMessage().startsWith(
          "You must specify an external Avro writer schema because this is required to read containerless Avro"));
    }
  }

  private static final String[] TWEET_FIELD_NAMES = new String[] { 
      "id", 
      "in_reply_to_status_id", 
      "in_reply_to_user_id", 
      "retweet_count",
      "retweeted", 
      "text", 
      "user_description" 
      };

  @Test
  public void testReadAvroTweetsContainer() throws Exception {
    runTweetContainer("test-morphlines/readAvroTweetsContainer", TWEET_FIELD_NAMES);
  }

  @Test
  public void testReadAvroTweetsContainerWithExternalSchema() throws Exception {
    runTweetContainer("test-morphlines/readAvroTweetsContainerWithExternalSchema", TWEET_FIELD_NAMES);    
  }
  
  @Test
  public void testReadAvroTweetsContainerWithExternalSubSchema() throws Exception {
    String[] subSchemaFieldNames = new String[] { 
        "id", 
        "text", 
        };
    runTweetContainer("test-morphlines/readAvroTweetsContainerWithExternalSubSchema", subSchemaFieldNames);    
  }
  
  private void runTweetContainer(String morphlineConfigFile, String[] fieldNames) throws Exception {
    File file = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433-medium.avro");
    morphline = createMorphline(morphlineConfigFile);    
    for (int j = 0; j < 3; j++) { // also test reuse of objects and low level avro buffers
      Record record = new Record();
      byte[] body = Files.toByteArray(file);    
      record.put(Fields.ATTACHMENT_BODY, body);
      collector.reset();
      startSession();
      Notifications.notifyBeginTransaction(morphline);
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getNumStartEvents());
      assertEquals(2104, collector.getRecords().size());
      
      FileReader<GenericData.Record> reader = new DataFileReader(file, new GenericDatumReader());
      int i = 0;
      while (reader.hasNext()) {
        Record actual = collector.getRecords().get(i);
        GenericData.Record expected = reader.next();
        assertTweetEquals(expected, actual, fieldNames, i);
        i++;
      }    
      assertEquals(collector.getRecords().size(), i);
    }
  }
  
  @Test
  public void testReadAvroTweetsWithExternalSchema() throws Exception {
    runTweets("test-morphlines/readAvroTweetsWithExternalSchema", TWEET_FIELD_NAMES);    
  }
  
  @Test
  public void testReadAvroTweetsWithExternalSubSchema() throws Exception {
    String[] subSchemaFieldNames = new String[] { 
        "id", 
        "text", 
        };
    runTweets("test-morphlines/readAvroTweetsWithExternalSubSchema", subSchemaFieldNames);
  }
  
  @Test
  public void testReadAvroJsonTweetsWithExternalSchema() throws Exception {
    runTweets("test-morphlines/readAvroJsonTweetsWithExternalSchema", TWEET_FIELD_NAMES);
  }
  
  @Test
  public void testReadAvroJsonTweetsWithExternalSubSchema() throws Exception {
    String[] subSchemaFieldNames = new String[] { 
        "id", 
        "text", 
        };
    runTweets("test-morphlines/readAvroJsonTweetsWithExternalSubSchema", subSchemaFieldNames);
  }
  
  private void runTweets(String morphlineConfigFile, String[] fieldNames) throws Exception {
    File file = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433-medium.avro");
    List<GenericData.Record> expecteds = new ArrayList();
    FileReader<GenericData.Record> reader = new DataFileReader(file, new GenericDatumReader());
    Schema schema = reader.getSchema();
    while (reader.hasNext()) {
      GenericData.Record expected = reader.next();
      expecteds.add(expected);
    }    
    assertEquals(2104, expecteds.size());

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Encoder encoder;
    if (morphlineConfigFile.contains("Json")) {
      encoder = EncoderFactory.get().jsonEncoder(schema, bout);
    } else {
      encoder = EncoderFactory.get().binaryEncoder(bout, null);
    }
    GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
    for (GenericData.Record record : expecteds) {
      datumWriter.write(record, encoder);
    }
    encoder.flush();

    morphline = createMorphline(morphlineConfigFile);
    for (int j = 0; j < 3; j++) { // also test reuse of objects and low level avro buffers
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, bout.toByteArray());
      collector.reset();
      startSession();
      Notifications.notifyBeginTransaction(morphline);
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getNumStartEvents());
      assertEquals(2104, collector.getRecords().size());
      
      reader = new DataFileReader(file, new GenericDatumReader());
      int i = 0;
      while (reader.hasNext()) {
        Record actual = collector.getRecords().get(i);
        GenericData.Record expected = reader.next();
        assertTweetEquals(expected, actual, fieldNames, i);
        i++;
      }    
      assertEquals(collector.getRecords().size(), i);
      }
  }
  
  private void assertTweetEquals(GenericData.Record expected, Record actual, String[] fieldNames, int i) {
    //  System.out.println("\n\nexpected: " + toString(avroRecord));
    //  System.out.println("actual:   " + actual);
    for (String fieldName : fieldNames) {
      assertEquals(
          i + " fieldName: " + fieldName, 
          expected.get(fieldName).toString(), 
          actual.getFirstValue(fieldName).toString());
    }
    
    for (String fieldName : TWEET_FIELD_NAMES) {
      if (!Arrays.asList(fieldNames).contains(fieldName)) {
        assertFalse(actual.getFields().containsKey(fieldName));
      }
    }
  }

  @Test
  @Ignore
  public void benchmarkAvro() throws Exception {
    benchmarkAvro("test-morphlines/readAvroTweetsWithExternalSchema");
    benchmarkAvro("test-morphlines/readAvroJsonTweetsWithExternalSchema");
    benchmarkAvro("test-morphlines/readAvroTweetsContainer");
  }
  
  private void benchmarkAvro(String morphlineConfigFile) throws Exception {
    System.out.println("Now benchmarking " + morphlineConfigFile + " ...");
    long durationSecs = 10;
    File file = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.avro");
    morphline = createMorphline(morphlineConfigFile);    
    byte[] bytes;
    if (morphlineConfigFile.contains("Container")) {
      bytes = Files.toByteArray(file);
    } else {    
      List<GenericData.Record> expecteds = new ArrayList();
      FileReader<GenericData.Record> reader = new DataFileReader(file, new GenericDatumReader());
      Schema schema = reader.getSchema();
      while (reader.hasNext()) {
        GenericData.Record expected = reader.next();
        expecteds.add(expected);
      }    
      assertEquals(2, expecteds.size());
  
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      Encoder encoder;
      if (morphlineConfigFile.contains("Json")) {
        encoder = EncoderFactory.get().jsonEncoder(schema, bout);
      } else {
        encoder = EncoderFactory.get().binaryEncoder(bout, null);
      }
      GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
      for (GenericData.Record record : expecteds) {
        datumWriter.write(record, encoder);
      }
      encoder.flush();
      bytes = bout.toByteArray();
    }

    long start = System.currentTimeMillis();
    long duration = durationSecs * 1000;
    int iters = 0; 
    while (System.currentTimeMillis() < start + duration) {
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, bytes);      
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      iters++;
    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("Results: iters=" + iters + ", took[secs]=" + secs + ", iters/secs=" + (iters/secs));
  }  

  private boolean load(Record record) {
    startSession();
    return morphline.process(record);
  }
  
  private int queryResultSetSize(String query) {
    return collector.getRecords().size();
  }
  
  private static Utf8 utf8(String str) {
    return new Utf8(str);
  }

  private String toString(GenericData.Record avroRecord) {
    Record record = new Record();
    for (Field field : avroRecord.getSchema().getFields()) {
      record.put(field.name(), avroRecord.get(field.pos()));
    }
    return record.toString(); // prints sorted by key for human readability
  }
  
}
