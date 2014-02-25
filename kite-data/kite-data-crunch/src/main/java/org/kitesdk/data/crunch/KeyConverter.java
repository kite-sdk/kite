/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.crunch;

import org.apache.crunch.types.Converter;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.io.NullWritable;

class KeyConverter<E> implements Converter<E, NullWritable, E, Iterable<E>> {

  private AvroType<E> ptype;

  public KeyConverter(AvroType<E> ptype) {
    this.ptype = ptype;
  }

  @Override
  public E convertInput(E entity, NullWritable nullWritable) {
    return entity;
  }

  @Override
  public Iterable<E> convertIterableInput(E entity, Iterable<NullWritable> nullWritables) {
    throw new UnsupportedOperationException("Should not be possible");
  }

  @Override
  public E outputKey(E entity) {
    return entity;
  }

  @Override
  public NullWritable outputValue(E entity) {
    return NullWritable.get();
  }

  @Override
  public Class<E> getKeyClass() {
    return ptype.getTypeClass();
  }

  @Override
  public Class<NullWritable> getValueClass() {
    return NullWritable.class;
  }

  @Override
  public boolean applyPTypeTransforms() {
    return true;
  }
}
