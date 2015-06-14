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
package org.kitesdk.data.camel;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.*;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

public class KiteTest extends CamelTestSupport {

    private static void cleanup() throws Exception {
        Configuration conf = new Configuration();
        Path dir = new Path(System.getProperty("user.dir") + "/target/tmp/");
        FileSystem fileSystem = dir.getFileSystem(conf);
        if (fileSystem.exists(dir))
            fileSystem.delete(dir, true);
    }

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Test
    public void createDataset() throws Exception {

        cleanup();

        RouteBuilder rb = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test").to("kite:dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/products?schema=resource:product.avsc&format=avro&compressionType=snappy");
                from("direct:testlazy").to("kite:dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/productslazy?format=avro&compressionType=snappy");
            }
        };
        context.addRoutes(rb);
        context.start();

        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("product.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        List<GenericRecord> data = new LinkedList<GenericRecord>();
        for (long i = 0; i < 10; ++i) {
            GenericRecord record = builder.set("name", "product-" + i).set("id", i).build();
            data.add(record);
        }
        for (GenericRecord record : data) {
            sendBodies("direct:test", record);
            sendBodies("direct:testlazy", record);
        }

        context.stop();

        Dataset<GenericRecord> products = Datasets.load("dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/products", GenericRecord.class);
        DatasetReader<GenericRecord> reader = products.newReader();

        List<GenericRecord> data2Compare = new LinkedList<GenericRecord>();
        for (GenericRecord record : reader)
            data2Compare.add(record);
        reader.close();

        Assert.assertArrayEquals(data.toArray(), data2Compare.toArray());

        Dataset<GenericRecord> productslazy = Datasets.load("dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/productslazy", GenericRecord.class);
        DatasetReader<GenericRecord> readerlazy = productslazy.newReader();

        List<GenericRecord> data2CompareLazy = new LinkedList<GenericRecord>();
        for (GenericRecord record : readerlazy)
            data2CompareLazy.add(record);
        reader.close();

        Assert.assertArrayEquals(data.toArray(), data2CompareLazy.toArray());

        cleanup();
    }

    @Test
    public void appendToDataset() throws Exception {

        cleanup();

        DatasetDescriptor datasetDescriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").build();
        Dataset<GenericRecord> dataset = Datasets.create("dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/products", datasetDescriptor, GenericRecord.class);

        DatasetWriter<GenericRecord> writer = dataset.newWriter();

        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("product.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        List<GenericRecord> data = new LinkedList<GenericRecord>();
        for (long i = 0; i < 20; ++i) {
            GenericRecord record = builder.set("name", "product-" + i).set("id", i).build();
            data.add(record);
        }

        Iterator<GenericRecord> iter = data.iterator();
        for (int i = 0; i < 10; ++i)
            writer.write(iter.next());
        writer.close();

        RouteBuilder rb = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test").to("kite:dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/products?append=true");
            }
        };
        context.addRoutes(rb);
        context.start();

        for (int i = 10; i < 20; ++i)
            sendBodies("direct:test", iter.next());
        context.stop();

        Dataset<GenericRecord> products = Datasets.load("dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/products", GenericRecord.class);
        DatasetReader<GenericRecord> reader = products.newReader();

        List<GenericRecord> data2Compare = new LinkedList<GenericRecord>();
        for (GenericRecord record : reader)
            data2Compare.add(record);
        reader.close();

        Assert.assertArrayEquals(data.toArray(), data2Compare.toArray());

        cleanup();
    }

    @Test
    public void createDatasetFromClass() throws Exception {

        cleanup();

        RouteBuilder rb = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test").to("kite:dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/persons?schema=class:org.kitesdk.data.camel.Person&format=avro&compressionType=snappy");
            }
        };
        context.addRoutes(rb);
        context.start();

        Random random = new Random();
        List<Person> data = new LinkedList<Person>();
        for (long i = 0; i < 10; ++i) {
            Person record = new Person("person-" + i, random.nextInt(80));
            data.add(record);
        }
        for (Person record : data)
            sendBodies("direct:test", record);

        context.stop();

        Dataset<Person> products = Datasets.load("dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/persons", Person.class);
        DatasetReader<Person> reader = products.newReader();

        List<Person> data2Compare = new LinkedList<Person>();
        for (Person record : reader)
            data2Compare.add(record);
        reader.close();

        Assert.assertArrayEquals(data.toArray(), data2Compare.toArray());

        cleanup();
    }

    @Test
    public void createPartitionedDataset() throws Exception {

        cleanup();

        RouteBuilder rb = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test1").to("kite:dataset:file://" +
                        System.getProperty("user.dir") +
                        "/target/tmp/test/persons1?" +
                        "schema=class:org.kitesdk.data.camel.User&format=avro&" +
                        "compressionType=snappy&" +
                        "partitionStrategy=literal:[{\"name\":\"favorite_color\",\"source\":\"favoriteColor\",\"type\":\"identity\"}]");

                URI uri = new File("src/test/resources/partitionStrategy.json").toURI();

                from("direct:test2").to("kite:dataset:file://" +
                        System.getProperty("user.dir") +
                        "/target/tmp/test/persons2?" +
                        "schema=class:org.kitesdk.data.camel.User&format=avro&" +
                        "compressionType=snappy&" +
                        "partitionStrategy=" + uri.toString());

                from("direct:test3").to("kite:dataset:file://" +
                        System.getProperty("user.dir") +
                        "/target/tmp/test/persons3?" +
                        "schema=class:org.kitesdk.data.camel.User&format=avro&" +
                        "compressionType=snappy&" +
                        "partitionStrategy=resource:partitionStrategy.json");

            }
        };
        context.addRoutes(rb);
        context.start();

        String[] colors = new String[]{
                "green", "blue", "pink", "brown", "yellow"
        };
        Random random = new Random();
        List<User> data = new LinkedList<User>();
        for (long i = 0; i < 10; ++i) {
            User record = new User("user-" + i, System.currentTimeMillis(), colors[random.nextInt(colors.length)]);
            data.add(record);
        }
        for (User record : data) {
            sendBodies("direct:test1", record);
            sendBodies("direct:test2", record);
            sendBodies("direct:test3", record);
        }

        context.stop();

        compare(data.toArray(), 1);

        compare(data.toArray(), 2);

        compare(data.toArray(), 3);

        cleanup();

    }

    private void compare(Object[] data, int datasetId) {
        Dataset<User> products = Datasets.load("dataset:file://" + System.getProperty("user.dir") + "/target/tmp/test/persons" + datasetId, User.class);
        DatasetReader<User> reader = products.newReader();

        SortedSet<User> data2Compare1 = new TreeSet<User>();
        for (User record : reader)
            data2Compare1.add(record);
        reader.close();
        Assert.assertArrayEquals(data, data2Compare1.toArray());
    }
}