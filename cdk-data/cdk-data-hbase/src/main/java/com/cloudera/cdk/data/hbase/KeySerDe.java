// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * This class handles key serialization and deserialization.
 * 
 * @param <K>
 *          The type of the key
 */
public interface KeySerDe<K> {

  /**
   * Serialize the key to bytes.
   * 
   * @param key
   *          The key to serialize
   * @return The byte array
   */
  public byte[] serialize(K key);

  /**
   * Serialize a partial key to bytes.
   * 
   * @param partialKey
   *          The partial key to serialize
   * @return The byte array
   */
  public byte[] serializePartial(PartialKey<K> partialKey);

  /**
   * Deserialize the key from a byte array.
   * 
   * @param keyBytes
   *          The byte array to deserialize the key from.
   * @return The key
   */
  public K deserialize(byte[] keyBytes);
}
