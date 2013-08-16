// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.cdk.data.hbase.EntitySchema;
import com.cloudera.cdk.data.hbase.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.hbase.KeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.KeySchema;
import com.cloudera.cdk.data.hbase.MappingType;
import com.cloudera.cdk.data.hbase.SchemaValidationException;

/**
 * This implementation parses Avro schemas for both the key and entities. The
 * entities contain metadata in annotations of the Avro record and Avro record
 * fields.
 * 
 * Each field must have a mapping annotation, which specifies how that field is
 * mapped to an HBase column.
 * 
 * Allowed mapping types are "column", "keyAsColumn", and "occVersion".
 * 
 * The column mapping type on a field tells this entity mapper to map that field
 * to the fully_qualified_column.
 * 
 * The keyAsColumn mapping type on a field tells the entity mapper to map each
 * key of the value type to a column in the specified column_family. This
 * annotation is only allowed on map and record types.
 * 
 * The entity record can contain a transactional annotation that tells HBase
 * Common that this entity takes part in transactions
 * 
 * The entity record should also contain a tables annotation, which tells HBase
 * Common which tables this entity can be persisted to.
 * 
 * Here is an example schema:
 * 
 * <pre>
 * 
 * { 
 *   "name": "record_name",
 *   "type": "record",
 *   "tables": ["table1", "table2"],
 *   "transactional": "true",
 *   "fields": [ 
 *     { 
 *       "name": "field1", 
 *       "type": "int", 
 *       "mapping": { "type": "column", "value": "meta:field1" } 
 *     },
 *      
 *     { 
 *       "name": "field2", 
 *       "type": { "type": "map", "values": "string" }, 
 *       "mapping": { "type": "keyAsColumn": "value": "map_family" } 
 *     }
 *     
 *   ]
 * }
 * 
 * </pre>
 * 
 * An Avro instance of this schema would have its field1 value encoded to the
 * meta:field1 column. Each key/value pair of the field2 map type would have its
 * value mapped to the map_family:[key] column. It will also participate in
 * transactions.
 */
public class AvroKeyEntitySchemaParser implements
    KeyEntitySchemaParser<String, KeySchema<String>, EntitySchema<String>> {

  @SuppressWarnings("deprecation")
  @Override
  public AvroKeySchema parseKey(String rawSchema) {
    Schema schema = Schema.parse(rawSchema);
    return new AvroKeySchema(schema, rawSchema);
  }

  @SuppressWarnings("deprecation")
  @Override
  public AvroEntitySchema parseEntity(String rawSchema) {
    Schema schema = Schema.parse(rawSchema);

    // Get the mapping of fields to default values.
    Map<String, Object> defaultValueMap = AvroUtils.getDefaultValueMap(schema);

    // Convert the avroRecordSchemaStr into a JsonNode so we can inspect
    // it's mapping types.
    ObjectMapper mapper = new ObjectMapper();
    JsonNode avroRecordSchemaJson;
    try {
      avroRecordSchemaJson = mapper.readValue(rawSchema, JsonNode.class);
    } catch (IOException e) {
      throw new SchemaValidationException(
          "Could not parse the avro record as JSON.", e);
    }
    JsonNode fields = avroRecordSchemaJson.get("fields");
    if (fields == null) {
      throw new SchemaValidationException(
          "Avro Record Schema must contain fields");
    }

    // Build the fieldMappingMap, which is a mapping of field names to
    // AvroFieldMapping instances (which describe the mapping type of the
    // field).
    List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();
    for (JsonNode recordFieldJson : fields) {
      String fieldName = recordFieldJson.get("name").getTextValue();
      Schema.Type type = schema.getField(fieldName).schema().getType();
      FieldMapping fieldMapping = createFieldMapping(fieldName,
          recordFieldJson, defaultValueMap, type);
      if (fieldMapping != null) {
        fieldMappings.add(fieldMapping);
      }
    }

    boolean transactional = Boolean.valueOf(schema.getProp("transactional"));
    List<String> tables = getTables(avroRecordSchemaJson);
    return new AvroEntitySchema(tables, schema, rawSchema, fieldMappings,
        transactional);
  }

  /**
   * Given a JsonNode representation of an avro record field, return the
   * AvroFieldMapping instance of that field. This instance contains the type of
   * mapping, and the value of that mapping, which will tell the mapping how to
   * map the field to columns in HBase.
   * 
   * @param fieldName
   *          The name of the field
   * @param recordFieldJson
   *          The Avro record field as a JsonNode.
   * @param defaultValueMap
   *          The mapping of fields to default values. Use this to look up
   *          possible default value.
   * @param type
   *          The field's java type
   * @return The AvroFieldMapping of this field.
   */
  private FieldMapping createFieldMapping(String fieldName,
      JsonNode recordFieldJson, Map<String, Object> defaultValueMap,
      Schema.Type type) {
    FieldMapping fieldMapping = null;
    JsonNode mappingNode = recordFieldJson.get("mapping");
    if (mappingNode != null) {
      JsonNode mappingTypeNode = mappingNode.get("type");
      JsonNode mappingValueNode = mappingNode.get("value");
      JsonNode prefixValueNode = mappingNode.get("prefix");
      if (mappingTypeNode == null) {
        String msg = "mapping attribute must contain type.";
        throw new SchemaValidationException(msg);
      }

      MappingType mappingType = null;
      String mappingValue = null;
      String prefix = null;

      if (mappingTypeNode.getTextValue().equals("column")) {
        mappingType = MappingType.COLUMN;
        if (mappingValueNode == null) {
          throw new SchemaValidationException(
              "column mapping type must contain a value.");
        }
        mappingValue = mappingValueNode.getTextValue();
      } else if ((mappingTypeNode.getTextValue().equals("keyAsColumn"))) {
        mappingType = MappingType.KEY_AS_COLUMN;
        if (mappingValueNode == null) {
          throw new SchemaValidationException(
              "keyAsColumn mapping type must contain a value.");
        }
        mappingValue = mappingValueNode.getTextValue();
        if (prefixValueNode != null) {
          prefix = prefixValueNode.getTextValue();
        }
      } else if ((mappingTypeNode.getTextValue().equals("occVersion"))) {
        mappingType = MappingType.OCC_VERSION;
      }
      Object defaultValue = defaultValueMap.get(fieldName);
      boolean incrementable = (type == Schema.Type.INT || type == Schema.Type.LONG);
      fieldMapping = new FieldMapping(fieldName, mappingType, mappingValue,
          defaultValue, prefix, incrementable);
    }
    return fieldMapping;
  }

  private List<String> getTables(JsonNode avroRecordSchemaJson) {
    if (avroRecordSchemaJson.get("tables") == null) {
      return new ArrayList<String>();
    }
    List<String> result = new ArrayList<String>(avroRecordSchemaJson.get(
        "tables").size());
    for (Iterator<JsonNode> it = avroRecordSchemaJson.get("tables")
        .getElements(); it.hasNext();) {
      result.add(it.next().getTextValue());
    }
    return result;
  }
}
