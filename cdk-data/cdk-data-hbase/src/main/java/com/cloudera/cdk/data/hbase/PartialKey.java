// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import java.util.ArrayList;
import java.util.List;

/**
 * A partial key, which can be used to create table scanners that start from the
 * bytes that make up only part of the key.
 * 
 * @param <K>
 *          The Key type
 */
public class PartialKey<K> {

  private final List<KeyPartNameValue> partList = new ArrayList<KeyPartNameValue>();

  private PartialKey(Builder<K> builder) {
    partList.addAll(builder.partList);
  }

  /**
   * Get the list of KeyPartNameValues.
   * 
   * @return The list of KeyPartNameValues that make this partial key.
   */
  public List<KeyPartNameValue> getPartList() {
    return partList;
  }

  /**
   * Get a part of the key by its name.
   * 
   * @param name
   *          The name of the key part to get, or null if it doesn't exist.
   * @return The KeyPartNameValue
   */
  public KeyPartNameValue getKeyPartByName(String name) {
    for (KeyPartNameValue value : partList) {
      if (name.equals(value.getName())) {
        return value;
      }
    }
    return null;
  }

  /**
   * Builder for the PartialKey
   * 
   * @param <K>
   */
  public static class Builder<K> {

    private final List<KeyPartNameValue> partList = new ArrayList<KeyPartNameValue>();

    public PartialKey<K> build() {
      return new PartialKey<K>(this);
    }

    public Builder<K> addKeyPart(String partName, Object value) {
      partList.add(new KeyPartNameValue(partName, value));
      return this;
    }
  }

  /**
   * A class that encapsulates a Key part. A Key part consists of a name value
   * pair, where the name is of type String, and the value is an Object.
   */
  public static class KeyPartNameValue {

    private final String name;
    private final Object value;

    public KeyPartNameValue(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }
  }
}
