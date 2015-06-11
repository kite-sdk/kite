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

import java.net.{URI, URL}

import com.databricks.spark.avro.SchemaSupport
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat
import org.kitesdk.data.spark.WriteMode.WriteMode
import org.kitesdk.data.spi.DefaultConfiguration

object KiteDatasetSaver extends SchemaSupport {

  private def rowsToAvro(rows: Iterator[Row],
                         schema: StructType): Iterator[(GenericRecord, Null)] = {
    val converter = createConverter(schema, "topLevelGenericRecord")
    rows.map(x => (converter(x).asInstanceOf[GenericRecord], null))
  }

  def saveAsKiteDataset(sparkDatasetDescriptor: SparkDatasetDescriptor,
                        uri: URI): Dataset[GenericRecord] = {
    assert(URIBuilder.DATASET_SCHEME == uri.getScheme,
      s"Not a dataset or view URI: $uri" + "")
    val job = Job.getInstance(DefaultConfiguration.get())
    /*
    TODO this is an hack, the partitioning strategy only works with primitive
    type not nullable, if the schema has been built using reflection from a
    case class for example, the primitive types are nullable in the avro schema
    breaking the partition strategy, so in case the partition strategy is defined
    I change nullable to false, it seems working.
    */
    val schema = if (sparkDatasetDescriptor.isPartitioned)
      StructType(sparkDatasetDescriptor.dataFrame.schema.iterator.map(field =>
        if (field.nullable) field.copy(nullable = false) else field).toArray)
    else
      sparkDatasetDescriptor.dataFrame.schema

    val avroSchema = getSchema(schema)

    def toURI(url: URL): URI = if (url == null)
      null
    else
      URI.create(url.toExternalForm)

    import collection.JavaConversions._
    val properties = sparkDatasetDescriptor.listProperties().
      map(prop => (prop, sparkDatasetDescriptor.getProperty(prop))).toMap

    val descriptor = new DatasetDescriptor(
      avroSchema,
      toURI(sparkDatasetDescriptor.getSchemaUrl),
      sparkDatasetDescriptor.getFormat,
      sparkDatasetDescriptor.getLocation,
      properties,
      if (sparkDatasetDescriptor.isPartitioned)
        sparkDatasetDescriptor.getPartitionStrategy
      else
        null,
      if (sparkDatasetDescriptor.isColumnMapped)
        sparkDatasetDescriptor.getColumnMapping
      else
        null,
      sparkDatasetDescriptor.getCompressionType
    )

    val dataset = Datasets.create[GenericRecord, Dataset[GenericRecord]](
      uri,
      descriptor,
      classOf[GenericRecord]
    )

    DatasetKeyOutputFormat.configure(job).writeTo(dataset)

    sparkDatasetDescriptor.dataFrame.
      mapPartitions(rowsToAvro(_, schema)).
      saveAsNewAPIHadoopDataset(job.getConfiguration)
    dataset

  }

  def saveAsKiteDataset(dataFrame: DataFrame,
                        dataset: Dataset[GenericRecord],
                        writeMode: WriteMode): Dataset[GenericRecord] = {
    val job = Job.getInstance(DefaultConfiguration.get())

    val schema = if (dataset.getDataset.getDescriptor.isPartitioned)
      StructType(dataFrame.schema.iterator.map(field =>
        if (field.nullable) field.copy(nullable = false) else field).toArray)
    else
      dataFrame.schema

    val avroSchema = getSchema(schema)

    writeMode match {
      case WriteMode.DEFAULT =>
        throw new RuntimeException(s"Dataset/view already exists: ${dataset.toString}")
      case WriteMode.APPEND =>
        DatasetKeyOutputFormat.configure(job).writeTo(dataset).appendTo(dataset)
      case WriteMode.OVERWRITE =>
        DatasetKeyOutputFormat.configure(job).writeTo(dataset).overwrite(dataset)
    }

    dataFrame.
      mapPartitions(rowsToAvro(_, schema)).
      saveAsNewAPIHadoopDataset(job.getConfiguration)
    dataset
  }

}
