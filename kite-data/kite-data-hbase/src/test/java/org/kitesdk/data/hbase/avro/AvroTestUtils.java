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
package org.kitesdk.data.hbase.avro;

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
