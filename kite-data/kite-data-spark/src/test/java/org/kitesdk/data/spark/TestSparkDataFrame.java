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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.*;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestSparkDataFrame {

    public static class Person implements Serializable {
        final String name;
        final int age;

        public  Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }
    }

    private static void cleanup() throws Exception {
        Configuration conf = new Configuration();
        Path dir = new Path(System.getProperty("user.dir") + "/tmp/");
        FileSystem fileSystem = dir.getFileSystem(conf);
        if (fileSystem.exists(dir))
            fileSystem.delete(dir, true);
    }

    private static Dataset<GenericRecord> generateDataset(Format format, CompressionType compressionType) throws Exception {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").compressionType(compressionType).format(format).build(); //Snappy compression is the default
        Dataset<GenericRecord> products = Datasets.create("dataset:file://" + System.getProperty("user.dir") + "/tmp/test/products", descriptor, GenericRecord.class);
        DatasetWriter<GenericRecord> writer = products.newWriter();
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

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(SparkTestHelper.getSparkContext());

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

    private void testWriteToKite(Format format) throws Exception {

        cleanup();

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(SparkTestHelper.getSparkContext());

        URI datasetURI = URIBuilder.build("repo:file:////" + System.getProperty("user.dir") + "/tmp", "test", "persons");

        List<Person> peopleList = new ArrayList<Person>();
        peopleList.add(new Person("David", 50));
        peopleList.add(new Person("Ruben", 14));
        peopleList.add(new Person("Giuditta", 12));
        peopleList.add(new Person("Vita", 19));

        DataFrame people = sqlContext.createDataFrame(SparkTestHelper.getSparkContext().parallelize(peopleList), Person.class);
        people.registerTempTable("people");

        DataFrame teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19");

        Dataset<GenericData.Record> dataset = KiteDatasetSaver.saveAsKiteDataset(teenagers, datasetURI, format, CompressionType.Snappy);

        DatasetReader<GenericData.Record> reader = dataset.newReader();

        List<String> res = new ArrayList<String>();
        for (GenericData.Record record : reader) {
            res.add(record.toString());
            System.out.println(record.toString());
        }
        reader.close();

        Assert.assertEquals(res.size(), 2);
        Assert.assertTrue(res.contains("{\"age\": 14, \"name\": \"Ruben\"}"));
        Assert.assertTrue(res.contains("{\"age\": 19, \"name\": \"Vita\"}"));

        cleanup();

    }

    @Test
    public void testWriteToParquetKite() throws Exception {
        testWriteToKite(Formats.PARQUET);
    }

    @Test
    public void testWriteToAvroKite() throws Exception {
        testWriteToKite(Formats.AVRO);
    }

}
