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
package org.kitesdk.data.hbase.avro;

import org.kitesdk.data.DatasetException;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.hbase.impl.BaseEntityMapper;
import org.kitesdk.data.hbase.impl.EntityMapper;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.EntitySerDe;
import org.kitesdk.data.hbase.impl.HBaseUtils;
import org.kitesdk.data.hbase.impl.KeySchema;
import org.kitesdk.data.hbase.impl.KeySerDe;
import org.kitesdk.data.hbase.impl.PutAction;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.generated.ManagedSchemaEntityVersion;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An entity mapper that is able to support multiple Avro schema versions in a
 * table. It uses a special set of columns in the table to determine the schema
 * version of each row, and uses that schema to properly deserialize the row.
 * Writing to tables is tied to a single schema.
 * 
 * @param <ENTITY>
 */
public class VersionedAvroEntityMapper<ENTITY extends IndexedRecord> implements
    EntityMapper<ENTITY> {

  private static Logger LOG = LoggerFactory
      .getLogger(VersionedAvroEntityMapper.class);

  /**
   * The schema parser we'll use to parse managed schemas.
   */
  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  private final SchemaManager schemaManager;
  private final String tableName;
  private final String entityName;
  private final Class<ENTITY> entityClass;
  private final AvroKeySchema keySchema;
  private final AvroEntitySchema entitySchema;
  private final int version;
  private final boolean specific;
  private final ConcurrentHashMap<Integer, EntityMapper<ENTITY>> entityMappers = new ConcurrentHashMap<Integer, EntityMapper<ENTITY>>();
  private final String managedSchemaEntityVersionSchema;
  private EntityMapper<ManagedSchemaEntityVersion> managedSchemaEntityVersionEntityMapper;

  /**
   * Builder for the VersionedAvroEntityMapper. This is the only way to
   * construct one.
   */
  public static class Builder {

    private SchemaManager schemaManager;
    private String tableName;
    private String entityName;
    private boolean specific;
    private String genericSchemaString;

    public Builder setSchemaManager(SchemaManager schemaManager) {
      this.schemaManager = schemaManager;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setEntityName(String entityName) {
      this.entityName = entityName;
      return this;
    }

    public Builder setSpecific(boolean specific) {
      this.specific = specific;
      return this;
    }

    public Builder setGenericSchemaString(String genericSchemaString) {
      this.genericSchemaString = genericSchemaString;
      return this;
    }

    public <E extends IndexedRecord> VersionedAvroEntityMapper<E> build() {
      return new VersionedAvroEntityMapper<E>(this);
    }
  }

  @SuppressWarnings("unchecked")
  private VersionedAvroEntityMapper(Builder builder) {
    this.schemaManager = builder.schemaManager;
    this.tableName = builder.tableName;
    this.entityName = builder.entityName;
    this.specific = builder.specific;
    this.managedSchemaEntityVersionSchema = getManagedSchemaEntityVersionSchema(entityName);

    if (!specific) {
      // Must be a Generic avro record. Tie the key and entity classes to
      // GenericRecords
      entityClass = (Class<ENTITY>) GenericRecord.class;

      // Get the key schema to use from the schema manager.
      keySchema = (AvroKeySchema) schemaManager.getKeySchema(tableName,
          entityName);

      // The entiySchema to use for writes may have been passed into the
      // Builder. If not, use the highest version schema registered in the
      // schema manager.
      if (builder.genericSchemaString != null) {
        entitySchema = schemaParser.parseEntitySchema(builder.genericSchemaString);
      } else {
        entitySchema = (AvroEntitySchema) schemaManager.getEntitySchema(
            tableName, entityName);
      }

      // verify that this schema exists in the managed schema table, and get its
      // version. This method will throw a SchemaNotFoundException if it can't
      // find it.
      version = schemaManager.getEntityVersion(tableName, entityName,
          entitySchema);
    } else {
      // specific record, so initialize the schema, class, and version member
      // variables appropriately.
      try {
        keySchema = (AvroKeySchema) schemaManager.getKeySchema(tableName,
            entityName);
        // Get the highest version schema to determine the key and entity record
        // class names. Initialize the key and entity class members from those
        // names.
        AvroEntitySchema mostRecentEntitySchema = (AvroEntitySchema) schemaManager
            .getEntitySchema(tableName, entityName);
        String entityClassName = mostRecentEntitySchema.getAvroSchema()
            .getFullName();
        entityClass = (Class<ENTITY>) Class.forName(entityClassName);

        // Initialize the entitySchema from the SCHEMA$ field on the class. This
        // will be or schema we'll use to write with.
        String entitySchemaString = entityClass.getField("SCHEMA$").get(null)
            .toString();
        entitySchema = schemaParser.parseEntitySchema(entitySchemaString);

        // verify that this schema exists in the managed schema table, and get
        // its
        // version. This method will throw a SchemaNotFoundException if it can't
        // find it.
        version = schemaManager.getEntityVersion(tableName, entityName,
            entitySchema);
      } catch (ClassNotFoundException e) {
        String msg = "StorageKey or entity class not found. Make sure the specific "
            + "record instances are on the classpath.";
        LOG.error(msg, e);
        throw new DatasetException(msg, e);
      } catch (SecurityException e) {
        String msg = "Cannot access key or entity class.";
        LOG.error(msg, e);
        throw new DatasetException(msg, e);
      } catch (NoSuchFieldException e) {
        String msg = "SCHEMA$ field not found in the entity class";
        LOG.error(msg, e);
        throw new DatasetException(msg, e);
      } catch (IllegalAccessException e) {
        String msg = "Not allowed to access SCHEMA$ field in the entity class";
        LOG.error(msg, e);
        throw new DatasetException(msg, e);
      }
    }

    // Initialize the entity mappers this object wraps. There will be one entity
    // mapper per version of the schema. When deserializing a row, we'll use the
    // entity mapper that configured for that schema version.
    updateEntityMappers();

    // Initialize the entity mapper used to deserialize the special
    // ManagedSchemaEntityVersion record from each row.
    initializeEntityVersionEntityMapper();
  }

  @Override
  public PartitionKey mapToKey(ENTITY entity) {
    return entityMappers.get(version).mapToKey(entity);
  }

  @Override
  public ENTITY mapToEntity(Result result) {
    ManagedSchemaEntityVersion versionRecord = managedSchemaEntityVersionEntityMapper
        .mapToEntity(result);
    int resultVersion = 0;
    if (versionRecord != null) {
      resultVersion = versionRecord.getSchemaVersion();
    }
    if (entityMappers.containsKey(resultVersion)) {
      return entityMappers.get(resultVersion).mapToEntity(result);
    } else {
      schemaManager.refreshManagedSchemaCache(tableName, entityName);
      updateEntityMappers();
      if (entityMappers.containsKey(resultVersion)) {
        return entityMappers.get(resultVersion).mapToEntity(result);
      } else {
        String msg = "Could not find schema for " + tableName + ", "
            + entityName + ", with version " + resultVersion;
        LOG.error(msg);
        throw new SchemaNotFoundException(msg);
      }
    }
  }

  @Override
  public PutAction mapFromEntity(ENTITY entity) {
    EntityMapper<ENTITY> entityMapper = entityMappers.get(version);
    PutAction entityPut = entityMapper.mapFromEntity(entity);

    ManagedSchemaEntityVersion versionRecord = ManagedSchemaEntityVersion
        .newBuilder().setSchemaVersion(version).build();
    PutAction versionPut = managedSchemaEntityVersionEntityMapper
        .mapFromEntity(versionRecord);
    
    byte[] keyBytes = entityPut.getPut().getRow();
    versionPut = HBaseUtils.mergePutActions(keyBytes, Arrays.asList(versionPut));
    return HBaseUtils.mergePutActions(keyBytes,
        Arrays.asList(entityPut, versionPut));
  }

  @Override
  public Increment mapToIncrement(PartitionKey key, String fieldName, long incrementValue) {
    return entityMappers.get(version).mapToIncrement(key, fieldName,
        incrementValue);
  }

  @Override
  public long mapFromIncrementResult(Result result, String fieldName) {
    return entityMappers.get(version).mapFromIncrementResult(result, fieldName);
  }

  @Override
  public Set<String> getRequiredColumns() {
    Set<String> requiredColumns = entityMappers.get(version)
        .getRequiredColumns();
    requiredColumns.addAll(managedSchemaEntityVersionEntityMapper
        .getRequiredColumns());
    return requiredColumns;
  }

  @Override
  public Set<String> getRequiredColumnFamilies() {
    Set<String> requiredColumns = entityMappers.get(version)
        .getRequiredColumnFamilies();
    requiredColumns.addAll(managedSchemaEntityVersionEntityMapper
        .getRequiredColumnFamilies());
    return requiredColumns;
  }

  @Override
  public KeySchema getKeySchema() {
    return keySchema;
  }

  @Override
  public EntitySchema getEntitySchema() {
    return entitySchema;
  }

  @Override
  public KeySerDe getKeySerDe() {
    return entityMappers.get(version).getKeySerDe();
  }

  @Override
  public EntitySerDe<ENTITY> getEntitySerDe() {
    return entityMappers.get(version).getEntitySerDe();
  }

  /**
   * Initialize the entity mapper we'll use to convert the schema version
   * metadata in each row to a ManagedSchemaEntityVersion record.
   */
  private void initializeEntityVersionEntityMapper() {
    AvroEntitySchema avroEntitySchema = schemaParser
        .parseEntitySchema(managedSchemaEntityVersionSchema);
    avroEntitySchema = AvroUtils.mergeSpecificStringTypes(
        ManagedSchemaEntityVersion.class, avroEntitySchema);
    AvroEntityComposer<ManagedSchemaEntityVersion> entityComposer = new AvroEntityComposer<ManagedSchemaEntityVersion>(
        avroEntitySchema, true);
    AvroEntitySerDe<ManagedSchemaEntityVersion> entitySerDe = new AvroEntitySerDe<ManagedSchemaEntityVersion>(
        entityComposer, avroEntitySchema, avroEntitySchema, true);
    this.managedSchemaEntityVersionEntityMapper = new BaseEntityMapper<ManagedSchemaEntityVersion>(
        avroEntitySchema, entitySerDe);
  }

  /**
   * Update the map of wrapped entity mappers to reflect the most recent entity
   * schema metadata returned by the schemaManager.
   */
  private void updateEntityMappers() {
    for (Entry<Integer, EntitySchema> entry : schemaManager.getEntitySchemas(
        tableName, entityName).entrySet()) {
      if (!entityMappers.containsKey(entry.getKey())) {
        AvroEntitySchema writtenSchema = (AvroEntitySchema) entry.getValue();
        EntityMapper<ENTITY> entityMapper = constructWrappedEntityMapper(
            keySchema, entitySchema, writtenSchema, entityClass);
        entityMappers.put(entry.getKey(), entityMapper);
      }
    }
  }

  /**
   * 
   * @param keySchema
   * @param readSchema
   * @param writeSchema
   * @param entityClass
   * @return
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private EntityMapper<ENTITY> constructWrappedEntityMapper(
      AvroKeySchema keySchema, AvroEntitySchema readSchema,
      AvroEntitySchema writeSchema, Class entityClass) {
    if (specific) {
      keySchema = AvroUtils.mergeSpecificStringTypes(entityClass, keySchema);
      readSchema = AvroUtils.mergeSpecificStringTypes(entityClass, readSchema);
      AvroEntityComposer entityComposer = new AvroEntityComposer(readSchema,
          true);
      AvroEntitySerDe entitySerDe = new AvroEntitySerDe(entityComposer,
          readSchema, writeSchema, true);
      KeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema(), keySchema.getPartitionStrategy());
      return new BaseEntityMapper(keySchema, readSchema, keySerDe, entitySerDe);
    } else {
      KeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema(), keySchema.getPartitionStrategy());
      AvroEntityComposer entityComposer = new AvroEntityComposer(readSchema,
          false);
      AvroEntitySerDe entitySerDe = new AvroEntitySerDe(entityComposer,
          readSchema, writeSchema, false);
      return new BaseEntityMapper(keySchema, readSchema, keySerDe, entitySerDe);
    }
  }
  
  private static String getManagedSchemaEntityVersionSchema(String entityName) {
    String avroSchemaString = AvroUtils
        .inputStreamToString(VersionedAvroEntityMapper.class
            .getResourceAsStream("/ManagedSchemaEntityVersion.avsc"));

    JsonNode jsonNode = rawSchemaAsJsonNode(avroSchemaString);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode mappingNode = mapper.createObjectNode();
    mappingNode.put("type", "column");
    mappingNode.put("value", "_s:sv_" + entityName);
    ((ObjectNode)jsonNode.get("fields").get(0)).put("mapping", mappingNode);
    return jsonNode.toString();
  }
  
  private static JsonNode rawSchemaAsJsonNode(String rawSchema) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode avroRecordSchemaJson;
    try {
      avroRecordSchemaJson = mapper.readValue(rawSchema, JsonNode.class);
    } catch (IOException e) {
      throw new ValidationException(
          "Could not parse the avro record as JSON.", e);
    }
    return avroRecordSchemaJson;
  }
}
