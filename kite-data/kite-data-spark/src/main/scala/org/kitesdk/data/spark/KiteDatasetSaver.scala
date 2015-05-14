/*
 * Copyright 2015 Cloudera, Inc.
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
package org.kitesdk.data.spark

import java.net.URI

import com.databricks.spark.avro.SchemaSupport
import org.apache.avro.generic.GenericData.Record
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat

object KiteDatasetSaver extends SchemaSupport {
  private def rowsToAvro(rows: Iterator[Row], schema: StructType): Iterator[(Record, Null)] = {
    val converter = createConverter(schema, "topLevelRecord")
    rows.map(x => (converter(x).asInstanceOf[Record], null))
  }

  def saveAsKiteDataset(dataFrame: DataFrame, uri: URI, format: Format = Formats.AVRO, compressionType: CompressionType = CompressionType.Snappy, partitionStrategy: Option[PartitionStrategy] = None): Dataset[Record] = {
    assert(URIBuilder.DATASET_SCHEME == uri.getScheme, s"Not a dataset or view URI: $uri" + "")
    val job = Job.getInstance()

    //TODO this is an hack, the partitioning strategy only works with primitive type not nullable, if the schema has been built using reflection from a case class for example, the primitive tyoe are nullable in the avro schema
    //breaking the partition strategy, so in case the partition strategy is defined I change nullable to false, it seems working.
    val schema = partitionStrategy.fold(dataFrame.schema)(_ => StructType(dataFrame.schema.iterator.map(field => if (field.nullable) field.copy(nullable = false) else field).toArray))
    val avroSchema = getSchema(schema)
    val descriptor = partitionStrategy.fold(new DatasetDescriptor.Builder().schema(avroSchema).format(format).compressionType(compressionType).build())(p => new DatasetDescriptor.Builder().schema(avroSchema).format(format).compressionType(compressionType).partitionStrategy(p).build())
    val dataset = Datasets.create[Record, Dataset[Record]](uri, descriptor, classOf[Record])
    DatasetKeyOutputFormat.configure(job).writeTo(dataset)
    dataFrame.mapPartitions(rowsToAvro(_, schema)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    dataset
  }
}
