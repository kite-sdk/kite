/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.hive;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;

public abstract class HiveTestUtils {
  public static void assertTableExists(
      HiveMetaStoreClient client, String db, String name)
      throws MetaException, TException {
    Assert.assertTrue("Table should exist db:" + db + " table:" + name,
        client.tableExists(db, name));
  }

  public static void assertTableIsExternal(
      HiveMetaStoreClient client, String db, String name)
      throws MetaException, TException {
    final Table table = client.getTable(db, name);
    Assert.assertTrue("Table should be external db:" + db + " table:" + table,
        MetaStoreUtils.isExternalTable(table) &&
        TableType.EXTERNAL_TABLE.toString().equals(table.getTableType()));
  }

  public static void assertTableIsManaged(
      HiveMetaStoreClient client, String db, String name)
      throws MetaException, TException {
    final Table table = client.getTable(db, name);
    Assert.assertTrue("Table should be external db:" + db + " table:" + table,
        !MetaStoreUtils.isExternalTable(table) &&
        !MetaStoreUtils.isIndexTable(table) &&
        TableType.MANAGED_TABLE.toString().equals(table.getTableType()));
  }
}
