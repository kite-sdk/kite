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
package org.kitesdk.data.hbase.manager;

import com.google.common.collect.Lists;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.HTablePool;
import org.kitesdk.data.ConcurrentSchemaModificationException;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.KeyEntitySchemaParser;
import org.kitesdk.data.hbase.impl.KeySchema;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.generated.ManagedSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Default SchemaManager implementation. It uses a ManagedSchemaDao
 * implementation, passed in the constructor, to read schema metadata, and
 * persist schema metadata for schema creations and migrations.
 */
public class DefaultSchemaManager implements SchemaManager {

  private static Logger LOG = LoggerFactory
      .getLogger(DefaultSchemaManager.class);

  /**
   * A mapping of managed schema row keys to the managed schema entities.
   */
  private volatile ConcurrentHashMap<String, ManagedSchema> managedSchemaMap;

  /**
   * A DAO which is used to access the managed key and entity schemas
   */
  private ManagedSchemaDao managedSchemaDao;

  /**
   * A map of schema parsers that can be used to parse different schema types.
   */
  private ConcurrentHashMap<String, KeyEntitySchemaParser<?, ?>> schemaParsers = new ConcurrentHashMap<String, KeyEntitySchemaParser<?, ?>>();

  /**
   * Constructor which uses the default managed schema table name, which is
   * managed_schemas.
   * 
   * @param tablePool
   *          The pool of HBase tables
   */
  public DefaultSchemaManager(HTablePool tablePool) {
    this(new ManagedSchemaHBaseDao(tablePool));
  }

  public DefaultSchemaManager(HTablePool tablePool, String managedSchemaTable) {
    this(new ManagedSchemaHBaseDao(tablePool, managedSchemaTable));
  }

  public DefaultSchemaManager(ManagedSchemaDao managedSchemaDao) {
    this.managedSchemaDao = managedSchemaDao;
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
  public KeySchema getKeySchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    String greatestVersionedSchema = getGreatestEntitySchemaString(managedSchema);
    return schemaParser.parseKeySchema(greatestVersionedSchema);
  }

  @Override
  public EntitySchema getEntitySchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    String greatestVersionedSchema = getGreatestEntitySchemaString(managedSchema);
    return schemaParser.parseEntitySchema(greatestVersionedSchema);
  }

  private String getGreatestEntitySchemaString(ManagedSchema managedSchema) {
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
      String msg = "No schema versions for " + managedSchema.getTable() + ", "
          + managedSchema.getName();
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }

    return greatestVersionedSchema;
  }

  @Override
  public EntitySchema getEntitySchema(String tableName, String entityName,
      int version) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    if (!managedSchema.getEntitySchemas().containsKey(String.valueOf(version))) {
      // didn't contain the schema version, refresh the schema cache and refetch
      // the managed schema.
      refreshManagedSchemaCache(tableName, entityName);
      managedSchema = getManagedSchema(tableName, entityName);
    }
    String schema = managedSchema.getEntitySchemas().get(
        String.valueOf(version));
    if (schema != null) {
      return schemaParser.parseEntitySchema(schema);
    } else {
      String msg = "Could not find managed schema for " + tableName + ", "
          + entityName + ", and version " + Integer.toString(version);
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }
  }

  @Override
  public Map<Integer, EntitySchema> getEntitySchemas(String tableName,
      String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    Map<Integer, EntitySchema> retMap = new HashMap<Integer, EntitySchema>();
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      EntitySchema entitySchema = schemaParser.parseEntitySchema(entry
          .getValue());
      retMap.put(Integer.parseInt(entry.getKey()), entitySchema);
    }
    return retMap;
  }

  @Override
  public int getEntityVersion(String tableName, String entityName,
      EntitySchema schema) {
    for (Entry<Integer, EntitySchema> entry :
        getEntitySchemas(tableName, entityName).entrySet()) {
      if (schema.equals(entry.getValue())) {
        return entry.getKey();
      }
    }
    String msg = "Could not find managed version for " + tableName + ", " +
        entityName + " that matches " + schema;
    LOG.error(msg);
    throw new SchemaNotFoundException(msg);
  }

  @Override
  public boolean hasSchemaVersion(String tableName, String entityName, EntitySchema schema) {
    for (Entry<Integer, EntitySchema> entry :
        getEntitySchemas(tableName, entityName).entrySet()) {
      if (entry.getValue().equals(schema)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void createSchema(String tableName, String entityName,
      String entitySchemaStr, String schemaParserType, String keySerDeType,
      String entitySerDeType) {

    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(schemaParserType);
    KeySchema keySchema = schemaParser.parseKeySchema(entitySchemaStr);
    EntitySchema entitySchema = schemaParser.parseEntitySchema(entitySchemaStr);

    try {
      ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
      if (managedSchema != null) {
        throw new IncompatibleSchemaException(
            "Cannot create schema when one already exists");
      }
    } catch (SchemaNotFoundException e) {
      // we want the schema to not be found, continue
    }

    // Validate that this schema is compatible with other schemas registered
    // with this same table.
    validateCompatibleWithTableSchemas(tableName, keySchema, entitySchema);

    ManagedSchema managedSchema = ManagedSchema.newBuilder()
        .setName(entityName).setTable(tableName)
        .setEntitySchemas(new HashMap<String, String>())
        .setSchemaType(schemaParserType).setEntitySerDeType(entitySerDeType)
        .setKeySerDeType(keySerDeType).build();

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put("0", entitySchema.getRawSchema());
    if (!managedSchemaDao.save(managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
    getManagedSchemaMap().put(
        getManagedSchemaMapKey(managedSchema.getTable(),
            managedSchema.getName()), managedSchema);
  }

  @Override
  public void migrateSchema(String tableName, String entityName,
      String newSchemaStr) {
    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());

    // validate it's a valid avro schema by parsing it
    EntitySchema newEntitySchema = schemaParser.parseEntitySchema(newSchemaStr);
    KeySchema newKeySchema = schemaParser.parseKeySchema(newSchemaStr);

    // verify that the newSchema isn't a duplicate of a previous schema version.
    if (hasSchemaVersion(tableName, entityName, newEntitySchema)) {
      throw new IncompatibleSchemaException(
          "Schema already exists as version: "
              + getEntityVersion(tableName, entityName, newEntitySchema));
    }

    // validate that, for each version of the schema, this schema is
    // compatible with those schema version. That means the field mapping
    // hasn't changed, and we can read old schemas, and processes that
    // are configured with old schemas can read new schemas.
    int greatestSchemaVersion = 0;
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      int version = Integer.parseInt(entry.getKey());
      if (version > greatestSchemaVersion) {
        greatestSchemaVersion = version;
      }
      String schemaString = entry.getValue();
      KeySchema keySchema = schemaParser.parseKeySchema(schemaString);
      EntitySchema entitySchema = schemaParser.parseEntitySchema(schemaString);
      if (!newKeySchema.compatible(keySchema)) {
        String msg = "StorageKey fields of entity schema not compatible with version "
            + Integer.toString(version)
            + ": Old schema: "
            + schemaString
            + " New schema: " + newEntitySchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
      if (!newEntitySchema.compatible(entitySchema)) {
        String msg = "Avro schema not compatible with version "
            + Integer.toString(version) + ": Old schema: " + schemaString
            + " New schema: " + newEntitySchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
    }

    // Validate that this schema is compatible with other schemas registered
    // with this same table.
    validateCompatibleWithTableSchemas(tableName, newKeySchema, newEntitySchema);

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put(
        Integer.toString(greatestSchemaVersion + 1),
        newEntitySchema.getRawSchema());
    if (!managedSchemaDao.save(managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
  }

  @Override
  public void deleteSchema(String tableName, String entityName) {
    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);

    if (!managedSchemaDao.delete(managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }

    getManagedSchemaMap().remove(
        getManagedSchemaMapKey(managedSchema.getTable(),
            managedSchema.getName()));
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
  @Override
  public void refreshManagedSchemaCache(String tableName, String entityName) {
    ManagedSchema managedSchema = managedSchemaDao.getManagedSchema(tableName,
        entityName);
    if (managedSchema != null) {
      getManagedSchemaMap().put(
          getManagedSchemaMapKey(managedSchema.getTable(),
              managedSchema.getName()), managedSchema);
    }
  }

  @Override
  public List<String> getEntityNames(String tableName) {
    List<String> names = Lists.newArrayList();
    for (ManagedSchema managedSchema : managedSchemaMap.values()) {
      if (managedSchema.getTable().equals(tableName)) {
        names.add(managedSchema.getName());
      }
    }
    return names;
  }

  @Override
  public Collection<String> getTableNames() {
    Set<String> tables = Sets.newHashSet();
    for (ManagedSchema managedSchema : managedSchemaDao.getManagedSchemas()) {
      tables.add(managedSchema.getTable());
    }
    return tables;
  }

  /**
   * Get the managedSchemaMap, lazily loading it if it hasn't been initialized
   * yet. Members of this class should never access the managedSchemaMap
   * directly, but should always access it through this method.
   * 
   * @return The managedSchemaMap
   */
  private ConcurrentHashMap<String, ManagedSchema> getManagedSchemaMap() {
    if (managedSchemaMap == null) {
      synchronized (this) {
        if (managedSchemaMap == null) {
          managedSchemaMap = new ConcurrentHashMap<String, ManagedSchema>();
          populateManagedSchemaMap();
        }
      }
    }
    return managedSchemaMap;
  }

  /**
   * Populate the managedSchemaMap with all of the managed schemas returned by
   * the managedSchemaDao.
   */
  private void populateManagedSchemaMap() {
    Collection<ManagedSchema> schemas = managedSchemaDao.getManagedSchemas();
    for (ManagedSchema managedSchema : schemas) {
      getManagedSchemaMap().put(
          getManagedSchemaMapKey(managedSchema.getTable(),
              managedSchema.getName()), managedSchema);
    }
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
    return getManagedSchemaMap().get(
        getManagedSchemaMapKey(tableName, entityName));
  }

  /**
   * Get the schema parser by its classname. This method will cache the
   * constructed schema parsers.
   * 
   * @param schemaParserClassName
   *          The class name of the schema parser
   * @return The constructed schema parser.
   */
  @SuppressWarnings("unchecked")
  private KeyEntitySchemaParser<?, ?> getSchemaParser(
      String schemaParserClassName) {
    if (schemaParsers.contains(schemaParserClassName)) {
      return schemaParsers.get(schemaParserClassName);
    } else {
      try {
        Class<KeyEntitySchemaParser<?, ?>> schemaParserClass = (Class<KeyEntitySchemaParser<?, ?>>) Class
            .forName(schemaParserClassName);
        KeyEntitySchemaParser<?, ?> schemaParser = schemaParserClass
            .getConstructor().newInstance();
        schemaParsers.putIfAbsent(schemaParserClassName, schemaParser);
        return schemaParser;
      } catch (Exception e) {
        throw new DatasetException(
            "Could not instantiate schema parser class: "
                + schemaParserClassName, e);
      }
    }
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
      refreshManagedSchemaCache(tableName, entityName);
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
   * Validate that a KeySchema and EntitySchema will be compatible with the
   * other schemas registered with a table. This includes making sure that the
   * schema doesn't overlap with columns other schemas for the table map to, and
   * validates that the key schemas are the same.
   * 
   * If validation fails, an IncompatibleSchemaException is thrown. Otherwise,
   * no exception is thrown.
   * 
   * @param tableName
   *          The name of the table who's schemas we want to validate against.
   * @param keySchema
   *          The KeySchema to validate against other KeySchemas registered with
   *          this table
   * @param entitySchema
   *          The EntitySchema to validate against other EntitySchemas
   *          registered with thsi table.
   */
  private void validateCompatibleWithTableSchemas(String tableName,
      KeySchema keySchema, EntitySchema entitySchema) {
    List<ManagedSchema> entitiesForTable = new ArrayList<ManagedSchema>();
    for (Entry<String, ManagedSchema> entry : getManagedSchemaMap().entrySet()) {
      if (entry.getKey().startsWith(tableName + ":")) {
        entitiesForTable.add(entry.getValue());
      }
    }
    for (ManagedSchema managedSchema : entitiesForTable) {
      if (!managedSchema.getName().equals(entitySchema.getName())) {
        KeyEntitySchemaParser<?, ?> parser = getSchemaParser(managedSchema
            .getSchemaType());
        for (String schema : managedSchema.getEntitySchemas().values()) {
          EntitySchema otherEntitySchema = parser.parseEntitySchema(schema);
          KeySchema otherKeySchema = parser.parseKeySchema(schema);
          if (!keySchema.compatible(otherKeySchema)) {
            String msg = "StorageKey fields of schema not compatible with other schema for the table. "
                + "Table: "
                + tableName
                + ". Other schema: "
                + otherEntitySchema.getRawSchema()
                + " New schema: "
                + entitySchema.getRawSchema();
            throw new IncompatibleSchemaException(msg);
          }
          if (!validateCompatibleWithTableColumns(entitySchema,
              otherEntitySchema)) {
            String msg = "Column mappings of schema not compatible with other schema for the table. "
                + "Table: "
                + tableName
                + ". Other schema: "
                + otherEntitySchema.getRawSchema()
                + " New schema: "
                + entitySchema.getRawSchema();
            throw new IncompatibleSchemaException(msg);
          }
          if (!validateCompatibleWithTableOccVersion(entitySchema,
              otherEntitySchema)) {
            String msg = "OCCVersion mapping of schema not compatible with other schema for the table. "
                + "Only one schema in the table can have one."
                + "Table: "
                + tableName
                + ". Other schema: "
                + otherEntitySchema.getRawSchema()
                + " New schema: "
                + entitySchema.getRawSchema();
            throw new IncompatibleSchemaException(msg);
          }
        }
      }
    }
  }

  /**
   * Validate that two schemas for a table don't overlap in columns.
   * 
   * @param entitySchema1
   *          The first schema to compare
   * @param entitySchema2
   *          The second schema to compare
   * @return True if compatible, otherwise False.
   */
  private boolean validateCompatibleWithTableColumns(
      EntitySchema entitySchema1, EntitySchema entitySchema2) {

    // Populate two collections of field mappings. One that contains all
    // of the column mappings, and one that contains the keyAsColumn
    // mappings from the first schema. These will be used to compare
    // against the second schema.
    Set<String> entitySchema1Columns = new HashSet<String>();
    List<String> entitySchema1KeyAsColumns = new ArrayList<String>();
    for (FieldMapping fieldMapping1 : entitySchema1
        .getColumnMappingDescriptor().getFieldMappings()) {
      if (fieldMapping1.getMappingType() == MappingType.COLUMN ||
          fieldMapping1.getMappingType() == MappingType.COUNTER) {
        entitySchema1Columns.add(getColumnValue(fieldMapping1));
      } else if (fieldMapping1.getMappingType() == MappingType.KEY_AS_COLUMN) {
        entitySchema1KeyAsColumns.add(getColumnValue(fieldMapping1));
      }
    }

    // For each field mapping in the second entity schema, we want to
    // validate the following:
    //
    // 1. That each column mapping in it doesn't map to the same column
    // as a column mapping in the first schema.
    //
    // 2. That each column mapping in it doesn't "startsWith()" with a
    // keyAsColumn mapping in the first schema, where the keyAsColumn
    // mapping value is "columnfamily:prefix".
    //
    // 3. That each keyAsColumn mapping in it isn't a prefix of one of
    // the first schema's column mappings.
    //
    // 4. That each keyAsColumn mapping in it isn't a prefix of one fo
    // the first schema's keyAsColumn mappings, and one of the first
    // schema's mappings isn't a prefix of this schema's keyAsColumn
    // mappings.
    for (FieldMapping fieldMapping2 : entitySchema2
        .getColumnMappingDescriptor().getFieldMappings()) {
      if (fieldMapping2.getMappingType() == MappingType.COLUMN ||
          fieldMapping2.getMappingType() == MappingType.COUNTER) {
        String value = getColumnValue(fieldMapping2);
        if (entitySchema1Columns.contains(value)) {
          LOG.warn("Field: " + fieldMapping2.getFieldName()
              + " has a table column conflict with a column mapped field in "
              + entitySchema1.getName());
          return false;
        }
        for (String keyAsColumn : entitySchema1KeyAsColumns) {
          if (value.startsWith(keyAsColumn)) {
            LOG.warn("Field: "
                + fieldMapping2.getFieldName()
                + " has a table column conflict with a keyAsColumn mapped field in "
                + entitySchema1.getName());
            return false;
          }
        }
      } else if (fieldMapping2.getMappingType() == MappingType.KEY_AS_COLUMN) {
        String entitySchema2KeyAsColumn = getColumnValue(fieldMapping2);
        for (String entitySchema1KeyAsColumn : entitySchema1KeyAsColumns) {
          if (entitySchema1KeyAsColumn.startsWith(entitySchema2KeyAsColumn)) {
            LOG.warn("Field "
                + fieldMapping2.getFieldName()
                + " has a table keyAsColumn conflict with a keyAsColumn mapped field in "
                + entitySchema1.getName());
            return false;
          }
        }
        for (String entitySchema1Column : entitySchema1Columns) {
          if (entitySchema1Column.startsWith(entitySchema2KeyAsColumn)) {
            LOG.warn("Field "
                + fieldMapping2.getFieldName()
                + " has a table keyAsColumn conflict with a column mapped field in "
                + entitySchema1.getName());
            return false;
          }
        }
      }
    }
    return true;
  }

  private static String getColumnValue(FieldMapping fm) {
    switch (fm.getMappingType()) {
      case COLUMN:
      case COUNTER:
        return fm.getFamilyAsString() + ":" + fm.getQualifierAsString();
      case KEY_AS_COLUMN:
        return fm.getFamilyAsString() + ":" +
            (fm.getPrefix() == null ? "" : fm.getPrefix());
      default:
        return null;
    }
  }

  /**
   * Only one schema for a table should contain an OCCVersion field mapping.
   * This method will compare two schemas and return true if only one has an
   * OCC_VERSION field.
   * 
   * @param entitySchema1
   *          The first schema to compare
   * @param entitySchema2
   *          The second schema to compare
   * @return True if compatible, otherwise False.
   */
  private boolean validateCompatibleWithTableOccVersion(
      EntitySchema entitySchema1, EntitySchema entitySchema2) {
    boolean foundOccMapping = false;
    for (FieldMapping fieldMapping : entitySchema1.getColumnMappingDescriptor()
        .getFieldMappings()) {
      if (fieldMapping.getMappingType() == MappingType.OCC_VERSION) {
        foundOccMapping = true;
        break;
      }
    }
    if (foundOccMapping) {
      for (FieldMapping fieldMapping : entitySchema2
          .getColumnMappingDescriptor().getFieldMappings()) {
        if (fieldMapping.getMappingType() == MappingType.OCC_VERSION) {
          LOG.warn("Field: " + fieldMapping.getFieldName() + " in schema "
              + entitySchema2.getName()
              + " conflicts with an occVersion field in "
              + entitySchema1.getName());
          return false;
        }
      }
    }
    return true;
  }

  private String getManagedSchemaMapKey(String tableName, String entityName) {
    return tableName + ":" + entityName;
  }
}
