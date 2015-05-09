/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data

import com.databricks.spark.avro.SchemaSupport
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, Row}
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat

import scala.language.existentials

package object spark extends SchemaSupport {

  implicit class KiteDatasetContext(sqlContext: org.apache.spark.sql.SQLContext) {
    def kiteDatasetFile(dataSet: Dataset[_], minPartitions: Int = 0): DataFrame = {
      val job = Job.getInstance()
      DatasetKeyInputFormat.configure(job).readFrom(dataSet).withType(classOf[GenericRecord])
      val rdd = sqlContext.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[DatasetKeyInputFormat[GenericRecord]], classOf[GenericRecord], classOf[Void])
      val converter = createConverter(sqlContext, dataSet.getDescriptor.getSchema)
      val rel = rdd.map(record => converter(record._1).asInstanceOf[Row])
      sqlContext.createDataFrame(rel, getSchemaType(dataSet.getDescriptor.getSchema))
    }
  }

}
