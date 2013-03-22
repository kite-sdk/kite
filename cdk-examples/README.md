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