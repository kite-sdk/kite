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
