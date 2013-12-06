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

/**
 * An interface to construct AvroRecordBuilders. The avro entity mappers need to
 * be able to create new AvroRecordBuilders of the type they are configured to
 * construct.
 * 
 * @param <T>
 *          The type of AvroRecord the builder will create.
 */
public interface AvroRecordBuilderFactory<T> {

  /**
   * Get a new AvroRecordBuilder instance.
   * 
   * @return The AvroRecordBuilder instance.
   */
  public AvroRecordBuilder<T> getBuilder();

  /**
   * Get the class of record the AvroRecordBuilder this factory returns will
   * construct.
   * 
   * @return The class.
   */
  public Class<T> getRecordClass();
}
