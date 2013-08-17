// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.ConcurrentSchemaModificationException;
import com.cloudera.cdk.data.hbase.EntityManager;
import com.cloudera.cdk.data.hbase.EntityMapper;
import com.cloudera.cdk.data.hbase.EntitySchema;
import com.cloudera.cdk.data.hbase.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.hbase.EntitySerDe;
import com.cloudera.cdk.data.hbase.HBaseUtils;
import com.cloudera.cdk.data.hbase.IncompatibleSchemaException;
import com.cloudera.cdk.data.hbase.KeySchema;
import com.cloudera.cdk.data.hbase.KeySerDe;
import com.cloudera.cdk.data.hbase.PutAction;
import com.cloudera.cdk.data.hbase.SchemaNotFoundException;
import com.cloudera.cdk.data.hbase.SchemaValidationException;
import com.cloudera.cdk.data.hbase.avro.AvroManagedSchemaDao.ManagedKeySchemaPair;

/**
 * An EntityManager implementation for Avro entities. The Avro schemas are
 * stored in an HBase table.
 * 
 * This class is abstract. Concrete implementations must override the
 * createEntityMapper method. This is so we can support an entity mapper that
 * creates specific records, and one that creates generic records.
 */
public abstract class AvroEntityManager implements
    EntityManager<String, AvroKeySchema, AvroEntitySchema> {

  private static Log LOG = LogFactory.getLog(AvroEntityManager.class);

  /**
   * The schema parser we'll use to parse managed schemas.
   */
  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  /**
   * The Avro schema (represented as a string) which represents an entity
   * version.
   */
  private static final String managedSchemaEntityVersionSchema;

  /**
   * A mapping of managed schema row keys to the managed schema entities.
   */
  private volatile ConcurrentHashMap<ManagedSchemaKey, ManagedSchema> managedSchemaMap;

  /**
   * A DAO which is used to access the managed key and entity schemas
   */
  private AvroManagedSchemaDao managedSchemaDao;

  static {
    managedSchemaEntityVersionSchema = AvroUtils
        .inputStreamToString(AvroEntityManager.class
            .getResourceAsStream("/ManagedSchemaEntityVersion.avsc"));
  }

  /**
   * Constructor which uses the default managed schema table name, which is
   * managed_schemas.
   * 
   * @param tablePool
   *          The pool of HBase tables
   */
  public AvroEntityManager(HTablePool tablePool) {
    this(new AvroManagedSchemaHBaseDao(tablePool));
  }

  public AvroEntityManager(HTablePool tablePool, String managedSchemaTable) {
    this(new AvroManagedSchemaHBaseDao(tablePool, managedSchemaTable));
  }

  public AvroEntityManager(AvroManagedSchemaDao managedSchemaDao) {
    this.managedSchemaDao = managedSchemaDao;
  }

  /**
   * Populate the managedSchemaMap with all of the managed schemas returned by
   * the managedSchemaDao.
   */
  private void populateManagedSchemaMap() {
    for (ManagedKeySchemaPair keyEntity : managedSchemaDao.getManagedSchemas()) {
      getManagedSchemaMap().put(keyEntity.getKey(),
          keyEntity.getManagedSchema());
    }
  }

  /**
   * Update the managedSchemaMap for the entry defined by tableName and
   * entityName.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   */
  private void updateManagedSchemaMap(String tableName, String entityName) {
    ManagedKeySchemaPair keyEntity = managedSchemaDao.getManagedSchema(
        tableName, entityName);
    if (keyEntity != null) {
      getManagedSchemaMap().put(keyEntity.getKey(),
          keyEntity.getManagedSchema());
    }
  }

  /**
   * Get the managedSchemaMap, lazily loading it if it hasn't been initialized
   * yet. Members of this class should never access the managedSchemaMap
   * directly, but should always access it through this method.
   * 
   * @return The managedSchemaMap
   */
  private ConcurrentHashMap<ManagedSchemaKey, ManagedSchema> getManagedSchemaMap() {
    if (managedSchemaMap == null) {
      synchronized (this) {
        if (managedSchemaMap == null) {
          managedSchemaMap = new ConcurrentHashMap<ManagedSchemaKey, ManagedSchema>();
          populateManagedSchemaMap();
        }
      }
    }
    return managedSchemaMap;
  }

  /**
   * Get the managed schema from the managedSchemaMap, using the
   * getManagedSchemaMap() accessor.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @return The ManagedSchema instance, or null if one doesn't exist.
   */
  private ManagedSchema getManagedSchemaFromSchemaMap(String tableName,
      String entityName) {
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    return getManagedSchemaMap().get(key);
  }

  /**
   * Get a map of schema versions for a managed schemas.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The managed schema version map.
   * @throws SchemaNotFoundException
   *           Thrown if a managed schema can't be found for the table name
   *           entity name.
   */
  private Map<Integer, String> getManagedSchemaVersions(String tableName,
      String entityName) {

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    Map<Integer, String> returnMap = new HashMap<Integer, String>();
    for (Map.Entry<String, String> versionsEntry : managedSchema
        .getEntitySchemas().entrySet()) {
      returnMap.put(Integer.parseInt(versionsEntry.getKey()),
          versionsEntry.getValue());
    }
    return returnMap;
  }

  /**
   * Get the ManagedSchema entity for the tableName, entityName managed schema.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The ManagedSchema entity
   * @throws SchemaNotFoundException
   */
  private ManagedSchema getManagedSchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchemaFromSchemaMap(tableName,
        entityName);
    if (managedSchema == null) {
      updateManagedSchemaMap(tableName, entityName);
      managedSchema = getManagedSchemaFromSchemaMap(tableName, entityName);
      if (managedSchema == null) {
        String msg = "Could not find managed schemas for " + tableName + ", "
            + entityName;
        throw new SchemaNotFoundException(msg);
      }
    }
    return managedSchema;
  }

  /**
   * Constructs the ManagedEntityMapper, which is able to map entities defined
   * by the managed schema identified by the table name, and entity name.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @param keyClass
   *          The class representing the key of the managed schema
   * @param recordFactory
   *          The factory that knows how to create entities of the managed
   *          schema
   * @return The entity mapper
   * @throws SchemaNotFoundException
   * @throws SchemaValidationException
   */
  @SuppressWarnings({ "rawtypes" })
  protected EntityMapper internalCreateEntityMapper(String tableName,
      String entityName, Class keyClass, Class entityClass) {
    int version = this.getEntityVersion(tableName, entityName,
        this.getEntitySchema(tableName, entityName));
    return internalCreateEntityMapper(tableName, entityName, keyClass,
        entityClass, version);
  }

  /**
   * Constructs the ManagedEntityMapper, which is able to map entities defined
   * by the managed schema identified by the table name, and entity name.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @param keyClass
   *          The class representing the key of the managed schema
   * @param recordFactory
   *          The factory that knows how to create entities of the managed
   *          schema
   * @param version
   *          The version of the managed schema
   * @return The entity mapper
   * @throws SchemaNotFoundException
   * @throws SchemaValidationException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected EntityMapper internalCreateEntityMapper(String tableName,
      String entityName, Class keyClass, Class entityClass, int version) {
    return new ManagedEntityMapper(tableName, entityName, keyClass,
        entityClass, version);
  }

  @SuppressWarnings({ "rawtypes" })
  protected abstract EntityMapper constructWrappedEntityMapper(
      AvroKeySchema keySchema, AvroEntitySchema readSchema,
      AvroEntitySchema writeSchema, Class keyClass, Class entityClass);

  /**
   * An entity mapper implementation that will use the managed schema's entity
   * schema versions to map entities to and from HBase.
   * 
   * @param <KEY>
   * @param <ENTITY>
   */
  private class ManagedEntityMapper<KEY extends IndexedRecord, ENTITY extends IndexedRecord>
      implements EntityMapper<KEY, ENTITY> {

    private final String tableName;
    private final String entityName;
    private final AvroKeySchema keySchema;
    private final AvroEntitySchema entitySchema;
    private final EntityMapper<KEY, ManagedSchemaEntityVersion> managedSchemaEntityVersionEntityMapper;
    private final ConcurrentHashMap<Integer, EntityMapper<KEY, ENTITY>> entityMappers = new ConcurrentHashMap<Integer, EntityMapper<KEY, ENTITY>>();
    private final int version;
    private final Class<KEY> keyClass;
    private final Class<ENTITY> entityClass;

    public ManagedEntityMapper(String tableName, String entityName,
        Class<KEY> keyClass, Class<ENTITY> entityClass, int version) {
      this.tableName = tableName;
      this.entityName = entityName;
      this.keySchema = AvroEntityManager.this.getKeySchema(tableName,
          entityName);
      this.entitySchema = AvroEntityManager.this.getEntitySchema(tableName,
          entityName, version);
      this.version = version;
      this.keyClass = keyClass;
      this.entityClass = entityClass;

      updateEntityMappers(entitySchema);

      AvroKeySerDe<KEY> keySerDe = null;
      for (EntityMapper<KEY, ENTITY> entityMapper : entityMappers.values()) {
        keySerDe = (AvroKeySerDe<KEY>) entityMapper.getKeySerDe();
        break;
      }
      AvroEntitySchema avroEntitySchema = schemaParser
          .parseEntity(managedSchemaEntityVersionSchema);
      avroEntitySchema = AvroUtils.mergeSpecificStringTypes(ManagedSchemaEntityVersion.class, avroEntitySchema);
      AvroEntityComposer<ManagedSchemaEntityVersion> entityComposer = new AvroEntityComposer<ManagedSchemaEntityVersion>(
          avroEntitySchema, true);
      AvroEntitySerDe<ManagedSchemaEntityVersion> entitySerDe = new AvroEntitySerDe<ManagedSchemaEntityVersion>(
          entityComposer, avroEntitySchema, avroEntitySchema, true);
      this.managedSchemaEntityVersionEntityMapper = new BaseEntityMapper<KEY, ManagedSchemaEntityVersion>(
          keySchema, avroEntitySchema, keySerDe, entitySerDe);
    }

    protected void updateEntityMappers(AvroEntitySchema readSchema) {
      for (Entry<Integer, String> entry : getManagedSchemaVersions(tableName,
          entityName).entrySet()) {
        if (!entityMappers.containsKey(entry.getKey())) {
          AvroEntitySchema writeSchema = schemaParser.parseEntity(entry
              .getValue());
          @SuppressWarnings("unchecked")
          EntityMapper<KEY, ENTITY> entityMapper = constructWrappedEntityMapper(
              keySchema, readSchema, writeSchema, keyClass, entityClass);
          entityMappers.put(entry.getKey(), entityMapper);
        }
      }
    }

    @Override
    public KeyEntity<KEY, ENTITY> mapToEntity(Result result) {
      KeyEntity<KEY, ManagedSchemaEntityVersion> versionRecord = managedSchemaEntityVersionEntityMapper
          .mapToEntity(result);
      int resultVersion = 0;
      if (versionRecord != null
          && versionRecord.getEntity() != null
          && versionRecord.getEntity().getSchemaVersion()
              .containsKey(entityName)) {
        resultVersion = versionRecord.getEntity().getSchemaVersion()
            .get(entityName);
      }
      if (entityMappers.containsKey(resultVersion)) {
        return entityMappers.get(resultVersion).mapToEntity(result);
      } else {
        updateManagedSchemaMap(tableName, entityName);
        try {
          updateEntityMappers(AvroEntityManager.this.getEntitySchema(tableName,
              entityName, version));
        } catch (SchemaNotFoundException e) {
          LOG.error("Schema not found for table " + tableName + ", "
              + entityName, e);
        }
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
    public PutAction mapFromEntity(KEY key, ENTITY entity) {
      EntityMapper<KEY, ENTITY> entityMapper = entityMappers.get(version);
      PutAction entityPut = entityMapper.mapFromEntity(key, entity);

      ManagedSchemaEntityVersion versionRecord = ManagedSchemaEntityVersion
          .newBuilder().setSchemaVersion(new HashMap<String, Integer>())
          .build();
      versionRecord.getSchemaVersion().put(entityName, version);
      PutAction versionPut = managedSchemaEntityVersionEntityMapper
          .mapFromEntity(key, versionRecord);

      byte[] keyBytes = entityMapper.getKeySerDe().serialize(key);
      return HBaseUtils.mergePutActions(keyBytes,
          Arrays.asList(entityPut, versionPut));
    }

    @Override
    public Increment mapToIncrement(KEY key, String fieldName,
        long incrementValue) {
      return entityMappers.get(version).mapToIncrement(key, fieldName,
          incrementValue);
    }

    @Override
    public long mapFromIncrementResult(Result result, String fieldName) {
      return entityMappers.get(version).mapFromIncrementResult(result,
          fieldName);
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
    public KeySchema<?> getKeySchema() {
      return keySchema;
    }

    @Override
    public EntitySchema<?> getEntitySchema() {
      return entitySchema;
    }

    @Override
    public KeySerDe<KEY> getKeySerDe() {
      return entityMappers.get(version).getKeySerDe();
    }

    @Override
    public EntitySerDe<ENTITY> getEntitySerDe() {
      return entityMappers.get(version).getEntitySerDe();
    }
  }

  @Override
  public boolean hasManagedSchema(String tableName, String entityName) {
    ManagedSchema managedSchema;
    try {
      managedSchema = getManagedSchema(tableName, entityName);
    } catch (SchemaNotFoundException e) {
      managedSchema = null;
    }
    return managedSchema != null;
  }

  @Override
  public AvroKeySchema getKeySchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchemaFromSchemaMap(tableName,
        entityName);
    if (managedSchema == null) {
      String msg = "Could not find managed schemas for " + tableName + ", "
          + entityName;
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    } else {
      return schemaParser.parseKey(managedSchema.getKeySchema());
    }
  }

  @Override
  public AvroEntitySchema getEntitySchema(String tableName, String entityName) {

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);

    int greatestVersion = -1;
    String greatestVersionedSchema = null;
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      int version = Integer.parseInt(entry.getKey());
      if (version > greatestVersion) {
        greatestVersion = version;
        greatestVersionedSchema = entry.getValue();
      }
    }

    if (greatestVersionedSchema == null) {
      String msg = "No schema versions for " + tableName + ", " + entityName;
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }
    return schemaParser.parseEntity(greatestVersionedSchema);
  }

  @Override
  public AvroEntitySchema getEntitySchema(String tableName, String entityName,
      int version) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    String schema = managedSchema.getEntitySchemas().get(
        String.valueOf(version));
    if (schema != null) {
      return schemaParser.parseEntity(schema);
    } else {
      String msg = "Could not find managed schema for " + tableName + ", "
          + entityName + ", and version " + Integer.toString(version);
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }
  }

  @Override
  public int getEntityVersion(String tableName, String entityName,
      AvroEntitySchema entitySchema) {
    for (Entry<Integer, String> entry : getManagedSchemaVersions(tableName,
        entityName).entrySet()) {
      AvroEntitySchema schema = schemaParser.parseEntity(entry.getValue());
      if (schema.getAvroSchema().getFields().size() != entitySchema
          .getAvroSchema().getFields().size()) {
        continue;
      }

      boolean equal = true;
      for (Field field : schema.getAvroSchema().getFields()) {
        Field entitySchemaField = entitySchema.getAvroSchema().getFields()
            .get(field.pos());
        if (!fieldsEqual(field, schema.getFieldMapping(field.name()),
            entitySchemaField,
            entitySchema.getFieldMapping(entitySchemaField.name()))) {
          equal = false;
          break;
        }
      }
      if (equal) {
        return entry.getKey();
      }
    }
    return -1;
  }

  /**
   * Create a completely new entity schema. There must be no current version of
   * this entity
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @param keySchemaStr
   *          The key schema for the new entity
   * @param entitySchemaStr
   *          The entity schema for the new entity
   * @throws IncompatibleSchemaException
   *           Thrown if a previous schema existed
   * @throws ConcurrentSchemaModificationException
   * @throws SchemaValidationException
   *           Thrown if the new schema is not a valid HBase Common Avro Schema.
   */
  @Override
  public void createSchema(String tableName, String entityName,
      String keySchemaStr, String entitySchemaStr) {

    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    updateManagedSchemaMap(tableName, entityName);

    AvroKeySchema keySchema = schemaParser.parseKey(keySchemaStr);
    AvroEntitySchema entitySchema = schemaParser.parseEntity(entitySchemaStr);

    ManagedSchema managedSchema = getManagedSchemaFromSchemaMap(tableName,
        entityName);
    if (managedSchema != null) {
      throw new IncompatibleSchemaException(
          "Cannot create schema when one already exists");
    }

    managedSchema = ManagedSchema.newBuilder().setName(entityName)
        .setTable(tableName).setKeySchema(keySchema.getRawSchema())
        .setEntitySchemas(new HashMap<String, String>()).build();

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put("0", entitySchema.getRawSchema());
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    if (!managedSchemaDao.save(key, managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
    getManagedSchemaMap().put(key, managedSchema);
  }

  /**
   * Create a new schema version. The following rules apply:
   * 
   * 1. New record fields can be added and removed, only if the record field
   * being added has a default value, and the record field being removed has a
   * default value.
   * 
   * 2. The schema must not already exist.
   * 
   * 3. The mapping component of each field doesn't change.
   * 
   * If these rules are not followed, an IncompatibleSchemaException is thrown.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @param newSchemaStr
   *          The new Avro schema to migrate to
   * @throws SchemaNotFoundException
   *           Thrown if the managed schema identified by the tableName and
   *           entityName does not exist.
   * @throws IncompatibleSchemaException
   *           Thrown if the new schema is incompatible with the old schemas.
   * @throws ConcurrentSchemaModificationException
   *           Thrown if the schema is updated by another person/process while
   *           this migration is being performed.
   * @throws SchemaValidationException
   *           Thrown if the new schema is not a valid HBase Common Avro Schema.
   */
  public void migrateSchema(String tableName, String entityName,
      String newSchemaStr) {

    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    updateManagedSchemaMap(tableName, entityName);

    // validate it's a valid avro schema by parsing it
    AvroEntitySchema newSchema = schemaParser.parseEntity(newSchemaStr);

    // verify that the newSchema isn't a duplicate of a previous schema version.
    int existingVersion = getEntityVersion(tableName, entityName, newSchema);
    if (existingVersion != -1) {
      throw new IncompatibleSchemaException(
          "Schema already exists as version: "
              + Integer.toString(existingVersion));
    }

    // validate that, for each version of the schema, this schema is
    // compatible with those schema version. That means the field mapping
    // hasn't changed, and we can read old schemas, and processes that
    // are configured with old schemas can read new schemas.
    int greatestSchemaVersion = 0;
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      int version = Integer.parseInt(entry.getKey());
      if (version > greatestSchemaVersion) {
        greatestSchemaVersion = version;
      }
      String schemaString = entry.getValue();
      if (!avroSchemasCompatible(schemaString, newSchema.getRawSchema())) {
        String msg = "Avro schema not compatible with version "
            + Integer.toString(version) + ": Old schema: " + schemaString
            + " New schema: " + newSchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
      if (!mappingCompatible(schemaString, newSchema.getRawSchema())) {
        String msg = "Incompatible mapping with version "
            + Integer.toString(version) + ": Old schema: " + schemaString
            + " New schema: " + newSchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
    }

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put(
        Integer.toString(greatestSchemaVersion + 1), newSchema.getRawSchema());
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    if (!managedSchemaDao.save(key, managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
  }

  /**
   * Ensure that the two avro schemas are compatible with each other.
   * Compatibility means either schema can be a read or write schema, following
   * the avro specification.
   * 
   * @param schemaString1
   * @param schemaString2
   * @return True if compatible, false if not.
   */
  private boolean avroSchemasCompatible(String schemaString1,
      String schemaString2) {
    Schema schema1 = schemaParser.parseEntity(schemaString1).getAvroSchema();
    Schema schema2 = schemaParser.parseEntity(schemaString2).getAvroSchema();
    if (!avroReadWriteSchemasCompatible(schema1, schema2)) {
      return false;
    }
    if (!avroReadWriteSchemasCompatible(schema2, schema1)) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if the writer and reader schema are compatible with each
   * other, following the avro specification.
   * 
   * @param writer
   *          writer schema
   * @param reader
   *          reader schema
   * @return True if compatible, false if not.
   */
  private boolean avroReadWriteSchemasCompatible(Schema writer, Schema reader) {
    Symbol rootSymbol;
    try {
      ResolvingGrammarGenerator g = new ResolvingGrammarGenerator();
      rootSymbol = g.generate(writer, reader);
    } catch (IOException e) {
      LOG.error("Unexpected IOException while resolving schemas.");
      throw new RuntimeException("IOException while generating grammar.", e);
    }

    return !hasErrorSymbol(rootSymbol);
  }

  /**
   * Determine if the symbol tree has an error symbol in it. This would indicate
   * that the two schemas are not compatible.
   * 
   * @param rootSymbol
   *          The root symbol to traverse from to look for an error symbol.
   * @return true if an error symbol exists in the tree.
   */
  private boolean hasErrorSymbol(Symbol rootSymbol) {
    if (rootSymbol.production == null) {
      return false;
    }
    for (Symbol s : rootSymbol.production) {
      if (s == rootSymbol) {
        continue;
      }
      if (s.getClass().equals(Symbol.ErrorAction.class)) {
        return true;
      } else {
        if (s.production != null) {
          for (Symbol subSymbol : s.production) {
            if (hasErrorSymbol(subSymbol)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /**
   * Ensure that the field mappings haven't changed between the oldSchemaString
   * and the newSchemaString.
   * 
   * @param oldSchemaString
   * @param newSchemaString
   * @return true if the mappings are compatible, false if not.
   */
  private boolean mappingCompatible(String oldSchemaString,
      String newSchemaString) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode oldSchema;
    JsonNode newSchema;
    try {
      oldSchema = mapper.readValue(oldSchemaString, JsonNode.class);
      newSchema = mapper.readValue(newSchemaString, JsonNode.class);
    } catch (IOException e) {
      throw new SchemaValidationException(
          "Schemas not proper JSON in mappingCompatible", e);
    }

    JsonNode oldSchemaFields = oldSchema.get("fields");
    JsonNode newSchemaFields = newSchema.get("fields");
    for (JsonNode oldSchemaField : oldSchemaFields) {
      String oldSchemaFieldName = oldSchemaField.get("name").getTextValue();
      for (JsonNode newSchemaField : newSchemaFields) {
        if (oldSchemaFieldName
            .equals(newSchemaField.get("name").getTextValue())) {
          if (!oldSchemaField.get("mapping").equals(
              newSchemaField.get("mapping"))) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private boolean fieldsEqual(Field field1, FieldMapping field1Mapping,
      Field field2, FieldMapping field2Mapping) {
    // if names aren't equal, return false
    if (!field1.name().equals(field2.name())) {
      return false;
    }
    // if schema types aren't equal, return false
    if (field1.schema().getType() != field2.schema().getType()) {
      return false;
    }
    // if field mappings aren't equal, return false
    if (!field1Mapping.equals(field2Mapping)) {
      return false;
    }
    // if one default value is null and the other isn't, return false
    if ((field1.defaultValue() != null && field2.defaultValue() == null)
        || (field1.defaultValue() == null && field2.defaultValue() != null)) {
      return false;
    }
    // if both default values are not null, and the default values are not
    // equal, return false
    if ((field1.defaultValue() != null && field2.defaultValue() != null)
        && !field1.defaultValue().equals(field2.defaultValue())) {
      return false;
    }
    // Fields are equal, return true
    return true;
  }
}
