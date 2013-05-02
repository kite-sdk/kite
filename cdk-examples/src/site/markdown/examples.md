# Cloudera Development Kit - Examples Module

The Examples Module is a collection of examples for the CDK.

## Example - User Dataset

This example shows basic usage of the CDK Data API for performing streaming writes
to (and reads from) a dataset.

From the examples module, build with:

```bash
mvn compile
```

Then create the dataset with:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.CreateUserDatasetPojo"
```

You can look at the files that were created with:

```bash
find /tmp/data
```

Read the entities in the dataset:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadUserDatasetPojo"
```

Finally, drop the dataset:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.DropUserDataset"
```

### Generic records vs. POJOs

The previous examples used POJOs, since they are the most familiar data transfer
obejcts for most Java programmers. Avro supports generic records too,
which are more efficient, since they don't require reflection,
and also don't require either the reader or writer to have the POJO class available.

Run the following to use the generic writer and reader:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.CreateUserDatasetGeneric"
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadUserDatasetGeneric"
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.DropUserDataset"
```

### Partitioning

The API supports partitioning, so that records are written to different partition files
according to the value of particular partition fields.

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.CreateUserDatasetGenericPartitioned"
find /tmp/data # see how partitioning affects the data layout
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadUserDatasetGeneric"
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadUserDatasetGenericOnePartition"
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.DropUserDataset"
```

### Parquet Columar Format

Parquet is a new columnar format for data. Columnar formats provide performance
advantages over row-oriented formats like Avro data files (which is the default in CDK),
when the number of columns is large (dozens) and the typical queries that you perform
over the data only retrieve a small number of the columns.

Note that Parquet support is still experimental in this release.

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.CreateUserDatasetGenericParquet"
find /tmp/data # see the parquet file extension
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadUserDatasetGeneric"
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.DropUserDataset"
```

### HCatalog

So far all metadata has been stored in the _.metadata_ directory on the filesystem.
It's possible to store metadata in HCatalog so that other HCatalog-aware applications
like Hive can make use of it.

Run the following to create the dataset:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.CreateHCatalogUserDatasetGeneric"
```

Hive/HCatalog's metastore directory is set to _/tmp/user/hive/warehouse/_ (see
_resources/hive-site.xml_), which is where the data is written to:

```bash
find /tmp/user/hive/warehouse/
```

Notice that there is no metadata stored there, since the metadata is stored in
Hive/HCatalog's metastore:

```bash
hive -e 'describe users'
```

You can use Hive to query the data directly:

```bash
hive -e 'select * from users'
```

Alternatively, you can use the Java API to read the data:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadHCatalogUserDatasetGeneric"
```

Dropping the dataset deletes the metadata from the metastore and the data from the
filesystem:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.DropHCatalogUserDataset"
```

## Scala

Run the equivalent example with:

```bash
scala -cp "$(mvn dependency:build-classpath | grep -v '^\[')" src/main/scala/createpojo.scala
```

Or for the generic example:

```bash
scala -cp "$(mvn dependency:build-classpath | grep -v '^\[')" src/main/scala/creategeneric.scala
```

The Java examples can be used to read (and drop) the dataset written from Scala:

```bash
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.ReadUserDatasetGeneric"
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.examples.data.DropUserDataset"
```
