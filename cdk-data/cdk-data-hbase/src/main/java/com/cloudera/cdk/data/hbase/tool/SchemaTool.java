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
package com.cloudera.cdk.data.hbase.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.data.dao.Constants;
import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.KeySchema;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.dao.SchemaValidationException;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;

/**
 * Utility class for managing Managed Schemas in HBase Common.
 */
public class SchemaTool {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaTool.class);

  private static final String CLASSPATH_PREFIX = "classpath:";

  private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final JsonFactory factory = mapper.getJsonFactory();

  private final SchemaManager schemaManager;

  private final HBaseAdmin hbaseAdmin;

  public SchemaTool(HBaseAdmin hbaseAdmin, SchemaManager entityManager) {
    this.hbaseAdmin = hbaseAdmin;
    this.schemaManager = entityManager;
  }

  /**
   * Scans the schemaDirectory for avro schemas, and creates or migrates HBase
   * Common managed schemas managed by this instances entity manager.
   * 
   * @param schemaDirectory
   *          The directory to recursively scan for avro schema files. This
   *          directory can be a directory on the classpath, including a
   *          directory that is embeddded in a jar on the classpath. In both of
   *          those cases, the schemaDirectory should be prefixed with
   *          classpath:
   * @param createTableAndFamilies
   *          If true, will create the table for each schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchemaDirectory(String schemaDirectory,
      boolean createTableAndFamilies) {
    List<String> schemaStrings;
    if (schemaDirectory.startsWith(CLASSPATH_PREFIX)) {
      URL dirURL = getClass().getClassLoader().getResource(
          schemaDirectory.substring(CLASSPATH_PREFIX.length()));
      if (dirURL != null && dirURL.getProtocol().equals("file")) {
        try {
          schemaStrings = getSchemaStringsFromDir(new File(dirURL.toURI()));
        } catch (URISyntaxException e) {
          throw new HBaseCommonException(e);
        }
      } else if (dirURL != null && dirURL.getProtocol().equals("jar")) {
        String jarPath = dirURL.getPath().substring(5,
            dirURL.getPath().indexOf("!"));
        schemaStrings = getSchemaStringsFromJar(jarPath,
            schemaDirectory.substring(CLASSPATH_PREFIX.length()));
      } else {
        String msg = "Could not find classpath resource: " + schemaDirectory;
        LOG.error(msg);
        throw new HBaseCommonException(msg);
      }
    } else {
      schemaStrings = getSchemaStringsFromDir(new File(schemaDirectory));
    }

    Map<String, String> tableKeySchemaMap = new HashMap<String, String>();
    Map<String, List<String>> tableEntitySchemaMap = new HashMap<String, List<String>>();
    for (String schemaString : schemaStrings) {
      String name = getEntityNameFromSchemaString(schemaString);
      List<String> tables = getTablesFromSchemaString(schemaString);
      if (name.endsWith("Key")) {
        for (String table : tables) {
          if (tableKeySchemaMap.containsKey(table)) {
            String msg = "Multiple keys for table: " + table;
            LOG.error(msg);
            throw new SchemaValidationException(msg);
          }
          LOG.debug("Adding key to tableKeySchemaMap for table: " + table
              + ". " + schemaString);
          tableKeySchemaMap.put(table, schemaString);
        }
      } else {
        for (String table : tables) {
          if (tableEntitySchemaMap.containsKey(table)) {
            tableEntitySchemaMap.get(table).add(schemaString);
          } else {
            List<String> entityList = new ArrayList<String>();
            entityList.add(schemaString);
            tableEntitySchemaMap.put(table, entityList);
          }
        }
      }
    }

    for (Entry<String, List<String>> entry : tableEntitySchemaMap.entrySet()) {
      String table = entry.getKey();
      String keySchema = tableKeySchemaMap.get(table);
      List<String> entitySchemas = entry.getValue();
      if (!tableKeySchemaMap.containsKey(table)) {
        String msg = "No Key Schema For Table: " + table;
        LOG.error(msg);
        throw new HBaseCommonException(msg);
      }
      if (entitySchemas.size() == 0) {
        String msg = "Key, but no entity schemas for Table: " + table;
        LOG.error(msg);
        throw new SchemaValidationException(msg);
      }
      for (String entitySchema : entry.getValue()) {
        createOrMigrateSchema(table, keySchema, entitySchema,
            createTableAndFamilies);
      }
    }
  }

  /**
   * Creates a new managed schema, or migrates an existing one if one exists for
   * the table name, entity name pair.
   * 
   * @param tableName
   *          The name of the table we'll be creating or migrating a schema for.
   * @param keySchemaFilePath
   *          The absolute file path to the key schema file.
   * @param entitySchemaFilePath
   *          The absolute file path to the entity schema file.
   * @param createTableAndFamilies
   *          If true, will create the table for this schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchemaFile(String tableName,
      String keySchemaFilePath, String entitySchemaFilePath,
      boolean createTableAndFamilies) {
    createOrMigrateSchemaFile(tableName, new File(keySchemaFilePath), new File(
        entitySchemaFilePath), createTableAndFamilies);
  }

  /**
   * Creates a new managed schema, or migrates an existing one if one exists for
   * the table name, entity name pair.
   * 
   * @param tableName
   *          The name of the table we'll be creating or migrating a schema for.
   * @param keySchemaFile
   *          The key schema file.
   * @param entitySchemaFile
   *          The entity schema file.
   * @param createTableAndFamilies
   *          If true, will create the table for this schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchemaFile(String tableName, File keySchemaFile,
      File entitySchemaFile, boolean createTableAndFamilies) {
    createOrMigrateSchema(tableName, getSchemaStringFromFile(keySchemaFile),
        getSchemaStringFromFile(entitySchemaFile), createTableAndFamilies);
  }

  /**
   * Creates a new managed schema, or migrates an existing one if one exists for
   * the table name, entity name pair.
   * 
   * @param tableName
   *          The name of the table we'll be creating or migrating a schema for.
   * @param keySchemaString
   *          The key schema
   * @param entitySchemaString
   *          The entity schema
   * @param createTableAndFamilies
   *          If true, will create the table for this schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchema(String tableName, String keySchemaString,
      String entitySchemaString, boolean createTableAndFamilies) {
    String entityName = getEntityNameFromSchemaString(entitySchemaString);
    AvroEntitySchema entitySchema = parser.parseEntity(entitySchemaString);
    AvroKeySchema keySchema = parser.parseKey(keySchemaString);
    if (schemaManager.hasManagedSchema(tableName, entityName)) {
      KeySchema currentKeySchema = schemaManager.getKeySchema(tableName,
          entityName);
      if (!keySchema.equals(currentKeySchema)) {
        String msg = "Migrating schema with different keys. Current: "
            + currentKeySchema.getRawSchema() + " New: "
            + keySchema.getRawSchema();
        LOG.error(msg);
        throw new SchemaValidationException(msg);
      }
      if (schemaManager.getEntityVersion(tableName, entityName, entitySchema) == -1) {
        LOG.info("Migrating Schema: (" + tableName + ", " + entityName + ")");
        schemaManager.migrateSchema(tableName, entityName, entitySchemaString);
      } else {
        // don't set createTableAndFamilies to false, becasue we may still need
        // to update the table to support what exists in the meta store.
        LOG.info("Schema hasn't changed, not migrating: (" + tableName + ", "
            + entityName + ")");
      }
    } else {
      LOG.info("Creating Schema: (" + tableName + ", " + entityName + ")");
      parser.parseEntity(entitySchemaString).getRequiredColumnFamilies();
      schemaManager.createSchema(tableName, entityName, keySchemaString,
          entitySchemaString,
          "com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser",
          "com.cloudera.cdk.data.hbase.avro.impl.AvroKeySerDe",
          "com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySerDe");
    }

    if (createTableAndFamilies) {
      try {
        if (!hbaseAdmin.tableExists(tableName)) {
          HTableDescriptor desc = new HTableDescriptor(tableName);
          desc.addFamily(new HColumnDescriptor(Constants.SYS_COL_FAMILY));
          desc.addFamily(new HColumnDescriptor(Constants.OBSERVABLE_COL_FAMILY));
          for (String columnFamily : entitySchema.getRequiredColumnFamilies()) {
            desc.addFamily(new HColumnDescriptor(columnFamily));
          }
          hbaseAdmin.createTable(desc);
        } else {
          Set<String> familiesToAdd = entitySchema.getRequiredColumnFamilies();
          familiesToAdd.add(new String(Constants.SYS_COL_FAMILY));
          familiesToAdd.add(new String(Constants.OBSERVABLE_COL_FAMILY));
          HTableDescriptor desc = hbaseAdmin.getTableDescriptor(tableName
              .getBytes());
          for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
            String familyName = columnDesc.getNameAsString();
            if (familiesToAdd.contains(familyName)) {
              familiesToAdd.remove(familyName);
            }
          }
          if (familiesToAdd.size() > 0) {
            hbaseAdmin.disableTable(tableName);
            try {
              for (String family : familiesToAdd) {
                hbaseAdmin.addColumn(tableName, new HColumnDescriptor(family));
              }
            } finally {
              hbaseAdmin.enableTable(tableName);
            }
          }
        }
      } catch (IOException e) {
        throw new HBaseCommonException(e);
      }
    }
  }

  /**
   * Will return the contents of schemaFile as a string
   * 
   * @param schemaFile
   *          The file who's contents should be returned.
   * @return The contents of schemaFile
   */
  private String getSchemaStringFromFile(File schemaFile) {
    String schemaString;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(schemaFile);
      schemaString = AvroUtils.inputStreamToString(fis);
    } catch (IOException e) {
      throw new HBaseCommonException(e);
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
        }
      }
    }
    return schemaString;
  }

  private List<String> getTablesFromSchemaString(String schema) {
    JsonNode node;
    try {
      JsonParser jp = factory.createJsonParser(schema);
      node = mapper.readTree(jp);
      if (node.get("tables") == null) {
        return new ArrayList<String>();
      }
      List<String> result = new ArrayList<String>(node.get("tables").size());
      for (Iterator<JsonNode> it = node.get("tables").getElements(); it
          .hasNext();) {
        result.add(it.next().getTextValue());
      }
      return result;
    } catch (JsonParseException e) {
      throw new SchemaValidationException(e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  private String getEntityNameFromSchemaString(String schema) {
    JsonNode node;
    try {
      JsonParser jp = factory.createJsonParser(schema);
      node = mapper.readTree(jp);
      if (node.get("name") == null) {
        return null;
      }
      return node.get("name").getTextValue();
    } catch (JsonParseException e) {
      throw new SchemaValidationException(e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  /**
   * Gets the list of HBase Common Avro schema strings from dir. It recursively
   * searches dir to find files that end in .avsc to locate those strings.
   * 
   * @param dir
   *          The dir to recursively search for schema strings
   * @return The list of schema strings
   */
  private List<String> getSchemaStringsFromDir(File dir) {
    List<String> schemaStrings = new ArrayList<String>();
    Collection<File> schemaFiles = FileUtils.listFiles(dir,
        new SuffixFileFilter(".avsc"), TrueFileFilter.INSTANCE);
    for (File schemaFile : schemaFiles) {
      schemaStrings.add(getSchemaStringFromFile(schemaFile));
    }
    return schemaStrings;
  }

  /**
   * Gets the list of HBase Common Avro schema strings from a directory in the
   * Jar. It recursively searches the directory in the jar to find files that
   * end in .avsc to locate thos strings.
   * 
   * @param jarPath
   *          The path to the jar to search
   * @param directoryPath
   *          The directory in the jar to find avro schema strings
   * @return The list of schema strings.
   */
  private List<String> getSchemaStringsFromJar(String jarPath,
      String directoryPath) {
    LOG.info("Getting schema strings in: " + directoryPath + ", from jar: "
        + jarPath);
    JarFile jar;
    try {
      jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new HBaseCommonException(e);
    } catch (IOException e) {
      throw new HBaseCommonException(e);
    }
    Enumeration<JarEntry> entries = jar.entries();
    List<String> schemaStrings = new ArrayList<String>();
    while (entries.hasMoreElements()) {
      JarEntry jarEntry = entries.nextElement();
      if (jarEntry.getName().startsWith(directoryPath)
          && jarEntry.getName().endsWith(".avsc")) {
        LOG.info("Found schema: " + jarEntry.getName());
        InputStream inputStream;
        try {
          inputStream = jar.getInputStream(jarEntry);
        } catch (IOException e) {
          throw new HBaseCommonException(e);
        }
        String schemaString = AvroUtils.inputStreamToString(inputStream);
        schemaStrings.add(schemaString);
      }
    }
    return schemaStrings;
  }
}
