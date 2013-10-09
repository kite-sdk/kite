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
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.PartitionKey;

/**
 * This class handles key serialization and deserialization.
 * 
 * @param <K>
 *          The type of the key
 */
public interface KeySerDe {

  /**
   * Serialize the key to bytes.
   * 
   * @param key
   *          The key to serialize
   * @return The byte array
   */
  public byte[] serialize(PartitionKey partitionKey);
  
  public byte[] serialize(Object... keyPartValues);

  /**
   * Deserialize the key from a byte array.
   * 
   * @param keyBytes
   *          The byte array to deserialize the key from.
   * @return The key
   */
  public PartitionKey deserialize(byte[] keyBytes);
}
