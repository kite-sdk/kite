---
layout: page
---
## Inferring a Schema from an Avro Data File

You can use the `DatasetDescriptor.Builder.schemaFromAvroDataFile` method to use the schema of an existing data file in Avro format. The source can be a local file, an InputStream, or a URI.

An Avro data file is not meant to be human readable (though my superhuman co-workers tell me that they can read them). Imagine that you have a file named 'movies.avro'. If you pass it to the method, as shown:

```java
DatasetDescriptor movieDesc = new DatasetDescriptor.Builder()
    .schemaFromAvroDataFile("movies.avro")
    .build()
```

The method returns a dataset descriptor, configured to use the same Avro schema as the sample file, ready to be used for creating or updating a dataset.

***
```json
{
  "type":"record",
  "name":"Movie",
  "namespace":"org.kitesdk.examples.data",
  "fields":[
    {"name":"id","type":"int"},
    {"name":"title","type":"string"},
    {"name":"releaseDate","type":"string"},
    {"name":"imdbUrl","type":"string"}
  ]
}
```

