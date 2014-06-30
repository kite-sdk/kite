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
package org.kitesdk.data.hbase.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.kitesdk.compat.DynMethods;

/**
 * Static utility functions for working with the HBase API.
 */
public class HBaseUtils {

  /**
   * Interface for Internal HBase Operations
   */
  private static interface Operation {

    // Add a Column to an Operation
    void addColumn(byte[] family, byte[] column);

    // Add a Family to an Operation
    void addFamily(byte[] family);

  }
  
  // CDK-506 Add compat for hbase-0.98
  private static final DynMethods.UnboundMethod GET_FAMILY_MAP_METHOD = 
      new DynMethods.Builder("getFamilyMap").impl(Put.class).build(); 
  
  /**
   * Given a list of puts, create a new put with the values in each put merged
   * together. It is expected that no puts have a value for the same fully
   * qualified column. Return the new put.
   * 
   * @param key
   *          The key of the new put.
   * @param putList
   *          The list of puts to merge
   * @return the new Put instance
   */
  public static Put mergePuts(byte[] keyBytes, List<Put> putList) {
    Put put = new Put(keyBytes);
    for (Put putToMerge : putList) {
      Map<byte[], List<KeyValue>> familyMap = 
          (Map<byte[], List<KeyValue>>) GET_FAMILY_MAP_METHOD.invoke(putToMerge);
      
      for (List<KeyValue> keyValueList : familyMap.values()) {
        for (KeyValue keyValue : keyValueList) {
          // don't use put.add(KeyValue) since it doesn't work with HBase 0.96 onwards
          put.add(keyValue.getFamily(), keyValue.getQualifier(),
              keyValue.getTimestamp(), keyValue.getValue());
        }
      }
    }
    return put;
  }

  /**
   * Given a list of PutActions, create a new PutAction with the values in each
   * put merged together. It is expected that no puts have a value for the same
   * fully qualified column. Return the new PutAction.
   * 
   * @param key
   *          The key of the new put.
   * @param putActionList
   *          The list of PutActions to merge
   * @return the new PutAction instance
   */
  public static PutAction mergePutActions(byte[] keyBytes,
      List<PutAction> putActionList) {
    VersionCheckAction checkAction = null;
    List<Put> putsToMerge = new ArrayList<Put>();
    for (PutAction putActionToMerge : putActionList) {
      putsToMerge.add(putActionToMerge.getPut());
      VersionCheckAction checkActionToMerge = putActionToMerge.getVersionCheckAction();
      if (checkActionToMerge != null) {
        checkAction = checkActionToMerge;
      }
    }
    Put put = mergePuts(keyBytes, putsToMerge);
    return new PutAction(put, checkAction);
  }

  /**
   * Add a Collection of Columns to an Operation, Only Add Single Columns
   * If Their Family Isn't Already Being Added.
   * @param columns
   *          Collection of columns to add to the operation
   * @param operation
   *          The HBase operation to add the columns to
   */
  private static void addColumnsToOperation(Collection<String> columns, Operation operation) {
    // Keep track of whole family additions
    Set<String> familySet = new HashSet<String>();

    // Iterate through each of the required columns
    for (String column : columns) {

      // Split the column by : (family : column)
      String[] familyAndColumn = column.split(":");

      // Check if this is a family only
      if (familyAndColumn.length == 1) {
        // Add family to whole family additions, and add to scanner
        familySet.add(familyAndColumn[0]);
        operation.addFamily(Bytes.toBytes(familyAndColumn[0]));
      } else {
        // Add this column, as long as it's entire family wasn't added.
        if (!familySet.contains(familyAndColumn[0])) {
          operation.addColumn(Bytes.toBytes(familyAndColumn[0]), Bytes.toBytes(familyAndColumn[1]));
        }
      }
    }
  }

  /**
   * Add a Collection of Columns to a Scanner, Only Add Single Columns
   * If Their Family Isn't Already Being Added.
   * @param columns
   *          Collection of columns to add to the scan
   * @param scan
   *          The scanner object to add the columns to
   */
  public static void addColumnsToScan(Collection<String> columns, final Scan scan) {
    addColumnsToOperation(columns, new Operation() {
      @Override
      public void addColumn(byte[] family, byte[] column) {
        scan.addColumn(family, column);
      }

      @Override
      public void addFamily(byte[] family) {
        scan.addFamily(family);
      }
    });
  }

  /**
   * Add a Collection of Columns to a Get, Only Add Single Columns
   * If Their Family Isn't Already Being Added.
   * @param columns
   *          Collection of columns to add to the Get
   * @param get
   *          The Get object to add the columns to
   */
  public static void addColumnsToGet(Collection<String> columns, final Get get) {
    addColumnsToOperation(columns, new Operation() {
      @Override
      public void addColumn(byte[] family, byte[] column) {
        get.addColumn(family, column);
      }

      @Override
      public void addFamily(byte[] family) {
        get.addFamily(family);
      }
    });
  }
}
