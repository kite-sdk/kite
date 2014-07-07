This page explains the schema URL warning:
> The Dataset is using a schema literal rather than a URL which will be attached to every message.

This warning means that the Dataset has been configured using an avro schema string, schema object, or by reflection. Configuring with a HDFS URL where the schema can be found instead of the other options allows certain components to pass the schema URL rather than the schema's string literal, which cuts down on the size of headers that must be sent with each message.

### Fixing the problem

The following java code demonstrates how to change the descriptor to use a schema URL instead of a schema literal:
```java
// a path in HDFS where schemas should be stored
Path schemaFolder = new Path("hdfs:/data/schemas");
FileSystem fs = FileSystem.get(schemaFolder.toUri(), new Configuration());

// open the repository (use the correct repository URI)
DatasetRepository repo = DatasetRepositories.open("repo:hive");
Dataset dataset = repo.load("datasetName"); // load your Dataset

// write the schema to the schema folder
Path schemaPath = new Path(schemaFolder, dataset.getName() + ".avsc");
FSDataOutputStream schemaFile = fs.create(schemaPath);
schemaFile.write(dataset.getDescriptor().getSchema().toString(true).getBytes(Charset.forName("UTF-8")));
schemaFile.close();

// update the Dataset to use the schema URI
DatasetDescriptor newDescriptor = new DatasetDescriptor.Builder(dataset.getDescriptor())
    .schemaUri(schemaPath.toUri())
    .build();
repo.update(dataset.getName(), newDescriptor);
```