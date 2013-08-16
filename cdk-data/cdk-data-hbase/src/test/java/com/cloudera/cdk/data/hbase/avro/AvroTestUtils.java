package com.cloudera.cdk.data.hbase.avro;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class AvroTestUtils {

  public static NavigableMap<byte[], byte[]> createFamilyMap() {
    NavigableMap<byte[], byte[]> familyMap = new TreeMap<byte[], byte[]>(
        new Comparator<byte[]>() {
          public int compare(byte[] left, byte[] right) {
            for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
              int a = (left[i] & 0xff);
              int b = (right[j] & 0xff);
              if (a != b) {
                return a - b;
              }
            }
            return left.length - right.length;
          }
        });
    return familyMap;
  }
}
