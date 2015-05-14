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
package org.kitesdk.data.spark;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestSparkDataFrame {

    private static void cleanup() throws Exception {
        Configuration conf = new Configuration();
        Path dir = new Path(System.getProperty("user.dir") + "/tmp/");
        FileSystem fileSystem = dir.getFileSystem(conf);
        if (fileSystem.exists(dir))
            fileSystem.delete(dir, true);
    }

    private static Dataset generateDataset(Format format, CompressionType compressionType) throws Exception {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").compressionType(compressionType).format(format).build(); //Snappy compression is the default
        Dataset products = Datasets.create("dataset:file://" + System.getProperty("user.dir") + "/tmp/test/products", descriptor, GenericRecord.class);
        DatasetWriter writer = products.newWriter();
        GenericRecordBuilder builder = new GenericRecordBuilder(descriptor.getSchema());
        for (long i = 1; i <= 100; ++i) {
            GenericData.Record record = builder.set("name", "product-" + i).set("id", i).build();
            writer.write(record);
        }
        writer.close();
        return products;
    }

    public static class Func implements Function<Row, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> call(Row row) throws Exception {
            return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
        }
    }

    private void testReadToDataFrame(Format format) throws Exception {

        cleanup();

        SparkConf conf = new SparkConf().
                setAppName("spark-kite-spec-test").
                set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                setMaster("local[16]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);

        Dataset<GenericRecord> products = generateDataset(format, CompressionType.Snappy);

        DataFrame data = KiteDatasetReader.readAsDataFrame(sqlContext, products, 0);

        data.registerTempTable("product");

        DataFrame res = sqlContext.sql("select * from product where id < 10");

        List<Tuple2<String, Long>> tuples = res.toJavaRDD().map(new Func()).collect();

        List<Tuple2<String, Long>> expected = new ArrayList<Tuple2<String, Long>>();

        for (long i = 1; i <= 9; ++i) {
            expected.add(new Tuple2<String, Long>("product-" + i, i));
        }

        Assert.assertArrayEquals(tuples.toArray(), expected.toArray());

        sparkContext.stop();

        cleanup();

    }

    @Test
    public void testKiteParquetToDataframe() throws Exception {
        testReadToDataFrame(Formats.PARQUET);
    }

    @Test
    public void testKiteAvroToDataframe() throws Exception {
        testReadToDataFrame(Formats.AVRO);
    }
}
