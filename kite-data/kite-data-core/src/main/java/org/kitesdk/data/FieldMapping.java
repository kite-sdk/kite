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
package org.kitesdk.data;

import com.google.common.base.Objects;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.ColumnMappingParser;

/**
 * Represents how to store a Schema field.
 *
 * @since 0.14.0
 */
@Immutable
public class FieldMapping {

  /**
   * The supported Mapping Types, which control how an entity field maps to
   * columns in an HBase table.
   *
   * @since 0.14.0
   */
  public static enum MappingType {

    // Maps a value to a part of the row key
    KEY,

    // Maps a value to a single column.
    COLUMN,

    // Maps a map or record value to columns
    // in a column family.
    KEY_AS_COLUMN,

    // Maps a field to one that can be incremented
    COUNTER,

    // The field will be populated with the
    // current version of the entity. This
    // allows the version to be checked if this
    // same entity is persisted back, to make sure
    // it hasn't changed.
    OCC_VERSION
  }

  private static final String SYS_COL_FAMILY = "_s";
  private static final String OCC_QUALIFIER = "w";

  public static FieldMapping key(String name) {
    return new FieldMapping(name, MappingType.KEY, null, null, null);
  }

  public static FieldMapping column(String name, String family, String qualifier) {
    return new FieldMapping(name, MappingType.COLUMN, family, qualifier, null);
  }

  public static FieldMapping keyAsColumn(String name, String family) {
    return new FieldMapping(
        name, MappingType.KEY_AS_COLUMN, family, null, null);
  }

  public static FieldMapping keyAsColumn(String name, String family,
                                         @Nullable String qualifierPrefix) {
    return new FieldMapping(
        name, MappingType.KEY_AS_COLUMN, family, null, qualifierPrefix);
  }

  public static FieldMapping counter(String name, String family, String qualifier) {
    return new FieldMapping(name, MappingType.COUNTER, family, qualifier, null);
  }

  public static FieldMapping occ(String name) {
    return new FieldMapping(
        name, MappingType.OCC_VERSION, SYS_COL_FAMILY, OCC_QUALIFIER, null);
  }

  public static FieldMapping version(String name) {
    return occ(name);
  }

  private final String fieldName;
  private final MappingType mappingType;
  private final String prefix;
  private final String familyString;
  private final byte[] family;
  private final String qualifierString;
  private final byte[] qualifier;

  private FieldMapping(String fieldName, MappingType mappingType,
      @Nullable String family, @Nullable String qualifier,
      @Nullable String prefix) {
    this.fieldName = fieldName;
    this.mappingType = mappingType;
    this.familyString = family;
    if (family != null) {
      this.family = encodeUtf8(family);
    } else {
      this.family = null;
    }
    this.qualifierString = qualifier;
    if (qualifier != null) {
      this.qualifier = encodeUtf8(qualifier);
    } else {
      this.qualifier = null;
    }
    if (prefix != null && !prefix.isEmpty()) {
      this.prefix = prefix;
    } else {
      this.prefix = null;
    }
  }

  public String getFieldName() {
    return fieldName;
  }

  public MappingType getMappingType() {
    return mappingType;
  }

  public String getPrefix() {
    return prefix;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="EI_EXPOSE_REP",
      justification="Defensive copy is needlessly expensive")
  public byte[] getFamily() {
    return family;
  }

  public String getFamilyAsString() {
    return familyString;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="EI_EXPOSE_REP",
      justification="Defensive copy is needlessly expensive")
  public byte[] getQualifier() {
    return qualifier;
  }

  public String getQualifierAsString() {
    return qualifierString;
  }

  private static byte[] encodeUtf8(String str) {
    if (str == null) {
      return new byte[0];
    }
    try {
      return str.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new DatasetIOException("[FATAL] Cannot decode UTF-8", e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        fieldName, mappingType, Arrays.hashCode(family),
        Arrays.hashCode(qualifier), prefix);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FieldMapping other = (FieldMapping) obj;
    return (Objects.equal(fieldName, other.fieldName) &&
        Objects.equal(mappingType, other.mappingType) &&
        Arrays.equals(family, other.family) &&
        Arrays.equals(qualifier, other.qualifier) &&
        Objects.equal(prefix, other.prefix));
  }

  @Override
  public String toString() {
    return ColumnMappingParser.toString(this);
  }
}
