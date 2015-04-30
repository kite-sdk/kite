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
package org.kitesdk.data.hbase.tool;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MultiHashMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.hbase.avro.AvroEntitySchema;
import org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser;
import org.kitesdk.data.hbase.avro.AvroKeySchema;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.impl.Constants;
import org.kitesdk.data.hbase.impl.KeySchema;
import org.kitesdk.data.hbase.impl.SchemaManager;

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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for managing Managed Schemas in HBase Common.
 */
public class SchemaTool {
  // Wait for 600 seconds (10 minutes) for all the tables to be available
  private static final int MAX_SECOND_WAIT_FOR_TABLE_CREATION = 600;

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
      boolean createTableAndFamilies) throws InterruptedException {
    List<String> schemaStrings;
    if (schemaDirectory.startsWith(CLASSPATH_PREFIX)) {
      URL dirURL = getClass().getClassLoader().getResource(
          schemaDirectory.substring(CLASSPATH_PREFIX.length()));
      if (dirURL != null && dirURL.getProtocol().equals("file")) {
        try {
          schemaStrings = getSchemaStringsFromDir(new File(dirURL.toURI()));
        } catch (URISyntaxException e) {
          throw new DatasetException(e);
        }
      } else if (dirURL != null && dirURL.getProtocol().equals("jar")) {
        String jarPath = dirURL.getPath().substring(5,
            dirURL.getPath().indexOf("!"));
        schemaStrings = getSchemaStringsFromJar(jarPath,
            schemaDirectory.substring(CLASSPATH_PREFIX.length()));
      } else {
        String msg = "Could not find classpath resource: " + schemaDirectory;
        LOG.error(msg);
        throw new DatasetException(msg);
      }
    } else {
      schemaStrings = getSchemaStringsFromDir(new File(schemaDirectory));
    }

    Map<String, List<String>> tableEntitySchemaMap = new HashMap<String, List<String>>();
    for (String schemaString : schemaStrings) {
      List<String> tables = getTablesFromSchemaString(schemaString);
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

    // Validate if for every key schema there is atleast one entity schemas
    for (Entry<String, List<String>> entry : tableEntitySchemaMap.entrySet()) {
      String table = entry.getKey();
      List<String> entitySchemas = entry.getValue();
      if (entitySchemas.size() == 0) {
        String msg =
            "Table requested, but no entity schemas for Table: " + table;
        LOG.error(msg);
        throw new ValidationException(msg);
      }
    }

    // Migrate the schemas in a batch, collect all the table descriptors
    // that require a schema migration
    Collection<HTableDescriptor> tableDescriptors = Lists.newArrayList();
    for (Entry<String, List<String>> entry : tableEntitySchemaMap.entrySet()) {
      String table = entry.getKey();
      for (String entitySchemaString : entry.getValue()) {
        boolean migrationRequired = prepareManagedSchema(table, entitySchemaString);
        // Optimization: If no migration is req, then no change in the table
        if (migrationRequired) {
          tableDescriptors.add(
              prepareTableDescriptor(table, entitySchemaString));
        }
      }
    }

    if (createTableAndFamilies) {
      createTables(tableDescriptors);
    }
  }

  /**
   * Creates a new managed schema, or migrates an existing one if one exists for
   * the table name, entity name pair.
   * 
   * @param tableName
   *          The name of the table we'll be creating or migrating a schema for.
   * @param entitySchemaFilePath
   *          The absolute file path to the entity schema file.
   * @param createTableAndFamilies
   *          If true, will create the table for this schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchemaFile(String tableName,
      String entitySchemaFilePath, boolean createTableAndFamilies)
      throws InterruptedException {
    createOrMigrateSchemaFile(tableName, new File(entitySchemaFilePath),
        createTableAndFamilies);
  }

  /**
   * Creates a new managed schema, or migrates an existing one if one exists for
   * the table name, entity name pair.
   * 
   * @param tableName
   *          The name of the table we'll be creating or migrating a schema for.
   * @param entitySchemaFile
   *          The entity schema file.
   * @param createTableAndFamilies
   *          If true, will create the table for this schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchemaFile(String tableName,
      File entitySchemaFile, boolean createTableAndFamilies)
      throws InterruptedException {
    createOrMigrateSchema(tableName, getSchemaStringFromFile(entitySchemaFile),
        createTableAndFamilies);
  }

  /**
   * Creates a new managed schema, or migrates an existing one if one exists for
   * the table name, entity name pair.
   * 
   * @param tableName
   *          The name of the table we'll be creating or migrating a schema for.
   * @param entitySchemaString
   *          The entity schema
   * @param createTableAndFamilies
   *          If true, will create the table for this schema if it doesn't
   *          exist, and will create families if they don't exist.
   */
  public void createOrMigrateSchema(String tableName, String entitySchemaString,
      boolean createTableAndFamilies) throws InterruptedException {
    boolean migrationRequired = prepareManagedSchema(tableName,
        entitySchemaString);
    if (migrationRequired && createTableAndFamilies) {
      try {
        HTableDescriptor descriptor = prepareTableDescriptor(tableName,
            entitySchemaString);
        if (hbaseAdmin.isTableAvailable(tableName)) {
          modifyTable(tableName, descriptor);
        } else {
          createTable(descriptor);
        }
      } catch (IOException e) {
        throw new DatasetException(e);
      }
    }
  }

  /**
   * Prepare managed schema for this entitySchema
   */
  private boolean prepareManagedSchema(String tableName,
      String entitySchemaString) {
    String entityName = getEntityNameFromSchemaString(entitySchemaString);
    AvroEntitySchema entitySchema = parser
        .parseEntitySchema(entitySchemaString);
    AvroKeySchema keySchema = parser.parseKeySchema(entitySchemaString);
    // Verify there are no ambiguities with the managed schemas
    if (schemaManager.hasManagedSchema(tableName, entityName)) {
      KeySchema currentKeySchema = schemaManager
          .getKeySchema(tableName, entityName);
      if (!keySchema.equals(currentKeySchema)) {
        String msg =
            "Migrating schema with different keys. Current: " + currentKeySchema
                .getRawSchema() + " New: " + keySchema.getRawSchema();
        LOG.error(msg);
        throw new ValidationException(msg);
      }
      if (!schemaManager
          .hasSchemaVersion(tableName, entityName, entitySchema)) {
        LOG.info("Migrating Schema: (" + tableName + ", " + entityName + ")");
        schemaManager.migrateSchema(tableName, entityName, entitySchemaString);
      } else {
        LOG.info("Schema hasn't changed, not migrating: (" + tableName + ", "
            + entityName + ")");
        return false;
      }
    } else {
      LOG.info("Creating Schema: (" + tableName + ", " + entityName + ")");
      parser.parseEntitySchema(entitySchemaString).getColumnMappingDescriptor()
          .getRequiredColumnFamilies();
      schemaManager.createSchema(tableName, entityName, entitySchemaString,
          "org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser",
          "org.kitesdk.data.hbase.avro.AvroKeySerDe",
          "org.kitesdk.data.hbase.avro.AvroEntitySerDe");
    }
    return true;
  }

  /**
   * Prepare the Table descriptor for the given entity Schema
   */
  private HTableDescriptor prepareTableDescriptor(String tableName,
      String entitySchemaString) {
    HTableDescriptor descriptor = new HTableDescriptor(
        Bytes.toBytes(tableName));
    AvroEntitySchema entitySchema = parser
        .parseEntitySchema(entitySchemaString);
    Set<String> familiesToAdd = entitySchema.getColumnMappingDescriptor()
        .getRequiredColumnFamilies();
    familiesToAdd.add(new String(Constants.SYS_COL_FAMILY));
    familiesToAdd.add(new String(Constants.OBSERVABLE_COL_FAMILY));
    for (String familyToAdd : familiesToAdd) {
      if (!descriptor.hasFamily(familyToAdd.getBytes())) {
        descriptor.addFamily(new HColumnDescriptor(familyToAdd));
      }
    }
    return descriptor;
  }

  /**
   * Create the tables asynchronously with the HBase
   */
  private void createTables(Collection<HTableDescriptor> tableDescriptors)
      throws InterruptedException {
    try {
      Set<String> tablesCreated = Sets.newHashSet();
      Multimap<String, HTableDescriptor> pendingTableUpdates = ArrayListMultimap
          .create();
      for (HTableDescriptor tableDescriptor : tableDescriptors) {
        String tableName = Bytes.toString(tableDescriptor.getName());
        if (tablesCreated.contains(tableName)) {
          // We have to wait for the table async creation to modify
          // Just add the required columns to be added
          pendingTableUpdates.put(tableName, tableDescriptor);
        } else {
          LOG.info("Creating table " + tableName);
          hbaseAdmin.createTableAsync(tableDescriptor, new byte[][] {});
          tablesCreated.add(tableName);
        }
      }

      // Wait for the tables to be online
      for (int waitCount = 0;
           waitCount < MAX_SECOND_WAIT_FOR_TABLE_CREATION; waitCount++) {
        Iterator<String> iterator = tablesCreated.iterator();
        while (iterator.hasNext()) {
          String table = iterator.next();
          if (hbaseAdmin.isTableAvailable(table)) {
            // Perform any updates scheduled on the table
            if (pendingTableUpdates.containsKey(table)) {
              for (HTableDescriptor tableDescriptor : pendingTableUpdates
                  .get(table)) {
                // Add the new columns - synchronous calls
                modifyTable(table, tableDescriptor);
              }
            }
            iterator.remove();
          }
        }
        // If all tables are available, then break
        if (tablesCreated.isEmpty()) {
          break;
        }
        // Sleep for a second before checking again
        Thread.sleep(1000);
      }
    } catch (IOException e) {
      throw new DatasetException(e);
    }
  }

  /**
   * add the column families which are not already present to the given table
   */
  private void modifyTable(String tableName, HTableDescriptor newDescriptor) {
    LOG.info("Modifying table " + tableName);
    HColumnDescriptor[] newFamilies = newDescriptor.getColumnFamilies();
    try {
      List<HColumnDescriptor> columnsToAdd = Lists.newArrayList();
      HTableDescriptor currentFamilies = hbaseAdmin
          .getTableDescriptor(Bytes.toBytes(tableName));
      for (HColumnDescriptor newFamily : newFamilies) {
        if (!currentFamilies.hasFamily(newFamily.getName())) {
          columnsToAdd.add(new HColumnDescriptor(newFamily.getName()));
        }
      }
      // Add all the necessary column families
      if (!columnsToAdd.isEmpty()) {
        hbaseAdmin.disableTable(tableName);
        try {
          for (HColumnDescriptor columnToAdd : columnsToAdd) {
            hbaseAdmin.addColumn(tableName, columnToAdd);
          }
        } finally {
          hbaseAdmin.enableTable(tableName);
        }
      }
    } catch (IOException e) {
      throw new DatasetException(e);
    }
  }

  /**
   * Create a single column asynchronously
   */
  private void createTable(HTableDescriptor tableDescriptor)
      throws InterruptedException {
    createTables(ImmutableList.of(tableDescriptor));
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
      throw new DatasetException(e);
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
      for (Iterator<JsonNode> it = node.get("tables").elements(); it
          .hasNext();) {
        result.add(it.next().textValue());
      }
      return result;
    } catch (JsonParseException e) {
      throw new ValidationException(e);
    } catch (IOException e) {
      throw new ValidationException(e);
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
      return node.get("name").textValue();
    } catch (JsonParseException e) {
      throw new ValidationException(e);
    } catch (IOException e) {
      throw new ValidationException(e);
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
      throw new DatasetException(e);
    } catch (IOException e) {
      throw new DatasetException(e);
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
          throw new DatasetException(e);
        }
        String schemaString = AvroUtils.inputStreamToString(inputStream);
        schemaStrings.add(schemaString);
      }
    }
    return schemaStrings;
  }
}
