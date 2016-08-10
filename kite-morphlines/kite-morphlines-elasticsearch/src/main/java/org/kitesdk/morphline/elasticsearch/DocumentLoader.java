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

package org.kitesdk.morphline.elasticsearch;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;

public interface DocumentLoader {

  /**
   * Begins the transaction.
   * 
   * @throws IOException
   */
  void beginTransaction() throws IOException;

  /**
   * Sends bulk to the elasticsearch cluster.
   * Sends any outstanding documents to the destination elasticsearch server and waits for a
   * positive or negative response. Depending on the outcome the caller should then commit or rollback the
   * current flume transaction correspondingly.
   *
   * @throws Exception
   */
  void commitTransaction() throws Exception;

  /**
   * Performs a rollback of all non-committed documents pending.
   *
   * @throws IOException
   */
  void rollbackTransaction() throws IOException;

  /**
   * Add new event to the bulk.
   *
   * @param document Document represented by elasticsearch class BytesReference
   * @param index Index/Collection name
   * @param indexType Index/Collection type
   * @param ttlMs Time to live expressed in milliseconds
   *
   * @throws java.lang.Exception
   */
  void addDocument(BytesReference document, String index, String indexType, int ttlMs) throws Exception;

  /**
   * Shutdown
   *
   * @throws Exception
   */
  void shutdown() throws Exception;
}
