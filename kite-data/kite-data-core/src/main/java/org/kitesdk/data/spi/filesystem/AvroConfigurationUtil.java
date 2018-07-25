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
package org.kitesdk.data.spi.filesystem;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.DataModelUtil;

public class AvroConfigurationUtil {

  // Constant from AvroJob copied here so we can set it on the Configuration
  // given to this class.
  private static final String AVRO_SCHEMA_INPUT_KEY = "avro.schema.input.key";

  // this is required for 1.7.4 because setDataModelClass is not available
  private static final DynMethods.StaticMethod setModel =
      new DynMethods.Builder("setDataModelClass")
          .impl(AvroSerialization.class, Configuration.class, Class.class)
          .defaultNoop()
          .buildStatic();

  public static void configure(Configuration conf, Format format, Schema schema, Class<?> type) {
    GenericData model = DataModelUtil.getDataModelForType(type);
    if (Formats.AVRO.equals(format)) {
      setModel.invoke(conf, model.getClass());
      conf.set(AVRO_SCHEMA_INPUT_KEY, schema.toString());

    } else if (Formats.PARQUET.equals(format)) {
      // TODO: update to a version of Parquet with setAvroDataSupplier
      //AvroReadSupport.setAvroDataSupplier(conf,
      //    DataModelUtil.supplierClassFor(model));
      AvroReadSupport.setAvroReadSchema(conf, schema);
    }
  }

}
