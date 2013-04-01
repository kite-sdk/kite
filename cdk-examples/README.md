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

***

[CDK Data Module](../cdk-data/README.md)