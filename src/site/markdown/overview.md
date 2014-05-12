#Data Module Overview

The Kite Data module is a set of APIs for interacting with data in Hadoop, specifically direct reading and writing of datasets in storage subsystems such as the Hadoop Distributed FileSystem (HDFS).

These APIs do not replace or supersede any of the existing Hadoop APIs. Instead, the Data module streamlines application of those APIs. You still use HDFS and Avro APIs directly when needed. The Kite Data module reflects best practices for default choices, data organization, and metadata system integration.

The Data module makes specific design choices that guide you toward well known patterns that make sense for most use cases. If you have advanced or niche use cases, this module might not be the best choice.

Limiting your options is not the goal. The Kite Data module is designed to be immediately useful, obvious, and in line with what most users do most frequently. Whenever revealing an option creates complexity, or otherwise requires you to research and assess additional choices, the option is omitted.

These APIs easily fit in with dependency injection frameworks such as Spring and Google Guice. You can use constructor injection when using these kinds of systems. Alternatively, if you prefer not to use DI frameworks, you can use the builder-style helper classes that come with many of the critical classes. By convention, these builders are always inner static classes named Builder, contained within their constituent classes.

The primary actors in the Data module are:
* entities
* schemas
* datasets
* partitioning strategies
* dataset descriptors
* dataset repositories

Many of these objects are interfaces, permitting multiple implementations, each with different functionality. The current release contains an implementation of each of these components for the Hadoop FileSystem abstraction (found in the org.kitesdk.data.filesystem package), for Hive (found in the org.kitesdk.data.hcatalog package), and for HBase (see the section about Dataset Repository URIs for how to access it).

While, in theory, any implementation of Hadoop’s FileSystem abstract class is supported by the Kite Data module, only the local and HDFS filesystem implementations are tested and officially supported.

#Entities

An entity is a single record in a dataset. The name _entity_ is a better term than _record_, because _record_ sounds as if it is a simple list of primitives, while _entity_ sounds more like a Plain Old Java Object you would find in a JPA class (see [JPA Entity](https://en.wikipedia.org/wiki/Java_Persistence_API#Entities) in Wikipedia.org). That said, _entity_ and _record_ are often used interchangeably when talking about datasets. 

Entities can be simple types, representing data structures with a few string attributes, or as complex as required.

Best practices are to define the output for your system, identifying all of the field values required to produce the report or analytics results you need. Once you identify your required fields, you define one or more related entities where you store the information you need to create your output. Define the format and structure for your entities using a schema.

#Schemas

A schema defines the field names and datatypes for a dataset. Kite relies on an Apache Avro schema definition for each dataset. For example, this is the schema definition for a table listing movies from the [put in all the disclaimers and stuff] dataset.

***
```
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
***
The goal is to get the schema into the *.avsc format and store it in the Hadoop file system. There are several ways to get the schema into the correct format. The following links provide examples for some of these approaches.

Java API | Command Line Interface
---------|-----------------------
[Inferring a schema from a Java class](1.1-Inferring-a-Schema-from-a-Java-Class) | [Inferring a schema from a Java object.](https://github.com/kite-sdk/kite/wiki/2.-Kite-Dataset-Command-Line-Interface#objSchema)
[Inferring a schema from an Avro data file](1.2-Inferring-a-schema-from-an-Avro-data-file) | [Inferring a schema from a CSV file.](https://github.com/kite-sdk/kite/wiki/2.-Kite-Dataset-Command-Line-Interface#csvSchema)
Importing an Avro *.avsc schema | &nbsp;
Creating a schema from a string | &nbsp;

#Datasets
A dataset is a collection of zero or more entities, represented by the interface Dataset. The relational database analog of a dataset is a table.

The HDFS implementation of a dataset is stored as Snappy-compressed Avro data files by default. The HDFS implementation is made up of zero or more files in a directory. You also have the option of storing your dataset in the column-oriented Parquet file format.

You can work with a subset of dataset entities using the Views API.

##Dataset Descriptor

All datasets have a name and an associated dataset descriptor. The dataset descriptor describes all aspects of the dataset. Primarily, the descriptor information is the dataset’s required schema and format. It might also store the dataset location (a repository URI) and a partition strategy. You must provide a descriptor at the time you create a dataset. You define the schema using Avro Schema APIs.

Entities must all conform to the same schema.

Datasets are represented by the `org.kitesdk.data.Dataset` interface. The parameters of `Dataset` correspond to the Java type of the entities it is used to read and write.

##Partitioned Datasets

You can partition your dataset on one or more attributes of an entity. Proper partitioning helps hadoop store information for improved performance. You can partition your records using hash, identity, or date (year, month, day, hour) strategies.

Imagine that you are working the registration desk at a conference. There are 100 attendees. You might decide you can hand out the name badges more efficiently if you have two lines that distribute the badges alphabetically, from A-M and N-Z. The attendee knows to go to one line or the other. The conference worker at the N-Z station doesn't bother looking through the badges A-M, because she knows that the attendee is going to be in her set of badges. It's more efficient, because she only has to search half of the items to find the right one.

However, that assumes that there is an equal distribution of names across all letters of the alphabet. It could be that there just happen to be an unusually high number of attendees whose names start with "B," resulting in long waits in the first line and little activity in the second line. Breaking out into three tables, A-I, J-S, T-Z might provide greater efficiency, sharing the workload and reducing the search time at any one table.

Fastest of all might be to have 100 stations where each individual could pick up her badge, but that would be a waste of time and effort for most of the staff. There is a "sweet spot" that maximizes efficiency with minimal resources.

When you store records in HDFS, the data is stored in "buckets" that provide coarse-grained organization. You can improve performance by providing hints to the system using a partitioning strategy. For example, if you most often retrieve your data using time-based queries, you can define the partitioning strategy by year, month, day, and hour, depending on the frequency with which you're capturing data. Your queries will run more quickly if the buckets used to organize your data correspond with the types of queries you make most often.

##Defining a Partitioning Strategy

Partitioning strategies are described in JSON format. You have the option of providing a partitioning strategy when you first create a dataset (you cannot apply a partitioning strategy to an existing dataset after you create it).

For example, here is a schema for a dataset that keeps track of visitors to a casino who belong to a loyalty club called the "High Rollers." The dataset stores information about their activity them UserID, recording when they enter the casino and how long they stay each time they visit.

####HighRollers.avsc
***
```
{
  "type" : "record",
  "name" : "HighRollersClub",
  "doc" : "Schema generated by Kite",
  "fields" : [ {
    "name" : "UserID",
    "type" : [ "null", "long" ],
    "doc" : "Unique customer identifier."
  }, {
    "name" : "EntryTime",
    "type" : "long",
    "doc" : "Timestamp at entry in milliseconds since Unix epoch"
  }, {
    "name" : "Duration",
    "type" : [ "null", "long" ],
    "doc" : "Time interval in milliseconds"
  } ]
}
```
***

Since the most common query against this data is time-based, you can define a partitioning strategy that gives Hadoop a hint that it should partition the information by year, month, and day. The partitioning strategy looks like this.

####HighRollers.json
***
```
[ {
  "source" : "EntryTime",
  "type" : "year",
  "name" : "year"
}, {
  "source" : "EntryTime",
  "type" : "month",
  "name" : "month"
}, {
  "source" : "EntryTime",
  "type" : "day",
  "name" : "day"
} ]
```
***

You can also use the command line interface command `partition-config` to generate the JSON file. See [partition-config](https://github.com/kite-sdk/kite/wiki/2.-Kite-Dataset-Command-Line-Interface#partition-config).

###Creating a Dataset That Uses a Partition Strategy

When you create a new dataset, you can specify the partition strategy along with the schema in the command line arguments. You can apply a partition strategy only when creating the dataset. You cannot apply a partition strategy to an existing dataset.

For example, you can create a dataset for our HighRollers club using this command.
```
dataset create HighRollersClub -s HighRollers.avsc -p HighRollers.json 
```
See [create](https://github.com/kite-sdk/kite/wiki/2.-Kite-Dataset-Command-Line-Interface#create) for more options when creating a dataset.
You can also use Kite to manage datasets in HBase, using the same tools and APIs. HBase datasets work differently than datasets backed by files in HDFS in two ways. First, dataset partitioning is handled by HBase and configuring it is a little different. Second, HBase stores data as a group of values, or cells, so you will need to configure how Kite divides your records into separate cells.

## Background: HBase storage cells

HBase stores data as a group of values, or cells, uniquely identified by a key. Using a key, you can look up the data for records stored in HBase very quickly, and also insert, modify, or delete records in the middle of a dataset. HBase makes this possible by keeping data organized by storage key.

While HDFS writes files into statically configured partitions, HBase dynamically groups keys, as needed, into files. When a group of records (a __region__) grows too large, HBase splits it into two regions. As more data is added, regions grow more specific, and the boundary between regions could be between any two keys.

Data cells are organized by column family, and then column qualifier. The cells form columns and groups of columns in a table structure. For example, a user's data can be stored using the e-mail address for a key, then a "name" column family with "first" and "last" qualifiers. We end up with a view that looks like this:

```
|  key           | name family      |
| (e-mail)       | first|   last    |
| -------------- | -----| --------- |
| buzz@pixar.com | Buzz | Lightyear |
```

## HBase partitioning

Kite uses a dataset's partitioning strategy to make storage keys for records. In HDFS, the key identifies a directory where the record is stored along with others with the same key (for example, events that happened on the same day). In HBase, keys are unique, making it very fast to find a particular record. A key in HBase might be an ID number or an e-mail address, as in the example above.

When configuring a partitioning strategy for HBase, always include a field that uniquely identifies the record.

### Organizing data

Good performance comes from being able to ignore as much of a dataset as possible. HBase partitioning works just like HDFS, even though you don't know where the partition boundaries are. The same guidelines for performance still apply: your partitioning strategy should start with information that helps eliminate the most data.

For example, storing events in HDFS by year, then month, and then day, allows Kite to ignore files that can't have data in a given time range. A similar partition strategy for HBase would include the entire timestamp, because the region boundaries are not statically set ahead of time and might be small.

### Balancing keys

TBD

#Dataset Repository

A _dataset repository_ is a physical storage location for datasets. Keeping with the relational database analogy, a dataset repository is the equivalent of a database of tables.

You can organize datasets into different dataset repositories for reasons related to logical grouping, security and access control, backup policies, and so on.

A dataset repository is represented by instances of the `org.kitesdk.data.DatasetRepository` interface in the Kite Data module. An instance of `DatasetRepository` acts as a factory for datasets, supplying methods for creating, loading, and deleting datasets.

Each dataset belongs to exactly one dataset repository. Kite doesn't provide built-in support for moving or copying datasets between repositories. MapReduce and other execution engines provide copy functionality, if you need it.

##Loading data from CSV

You can load comma separated value data into a dataset repository using the command line interface function [csv-import](http://kitesdk.org/docs/current/kite/kitedatasetcli.html#csv-import). 

#Viewing Your Data

Datasets you create Kite are no different than any other Hadoop dataset in your system, once created. You can query the data with Hive or view it using Impala.

For quick verification that your data has loaded properly, you can view the top_n_ records in your dataset using the command line interface function [show](http://kitesdk.org/docs/current/kite/kitedatasetcli.html#show). 
