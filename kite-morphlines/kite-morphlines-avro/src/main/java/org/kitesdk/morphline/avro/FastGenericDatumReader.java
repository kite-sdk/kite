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
package org.kitesdk.morphline.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

/**
 * Performance enhancement for GenericDatumReader for our use case.
 * 
 * If the writer schema changes, or if the identity of the calling thread changes (like for
 * MorphlineInterceptor), the underlying GenericDatumReader.getResolver() recreates a new
 * ResolvingDecoder for each record, which is very expensive. This hack helps to avoids that.
 */
final class FastGenericDatumReader<D> extends GenericDatumReader<D> {
  
  private ResolvingDecoder resolver;
  
  /** Construct where the writer's and reader's schemas are the same. */
  public FastGenericDatumReader(Schema schema) {
    super(schema);
  }

  /** Construct given writer's and reader's schema. */
  public FastGenericDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
  }
  
  /** Call this whenever appropriate */
  public void setResolver(ResolvingDecoder resolver) {
    this.resolver = resolver;
  }
  
  // method getResolver(getSchema(), getExpected()) is final so instead we override read()
  @Override
  @SuppressWarnings("unchecked")
  public D read(D reuse, Decoder in) throws IOException {    
    resolver.configure(in);
    D result = (D) read(reuse, getExpected(), resolver);
    resolver.drain();
    return result;
  }

}
