# CDK Data Reference Guide

## About This Guide

This reference guide is the primary source of documentation for the CDK Data
module. It covers the high level organization of the APIs,
primary classes and interfaces, intended usage, available extension points
for customization, and implementation information where helpful and
appropriate.

From here on, this guide assumes you are already familiar with the basic
design and functionality of HDFS, Hadoop MapReduce, and Java SE 6. Users
who are also familiar with [Avro][avro], data serialization techniques,
common compression algorithms (e.g. gzip, snappy), advanced Hadoop MapReduce
topics (e.g. input split calculation), and traditional data management
topics (e.g. partitioning schemes, metadata management) will benefit even more.

[avro]: http://avro.apache.org "Apache Avro"

## What's New

### Version 0.2.0

Version 0.2.0 has two major additions:

* Experimental support for reading and writing datasets in
  [Parquet format](https://github.com/Parquet/parquet-format).
* Support for storing dataset metadata in a Hive/HCatalog metastore.

The examples module has example code for both of these usages.

### Version 0.1.0

Version 0.1.0 is the first release of the CDK Data module. This is considered a
_beta_ release. As a sub-1.0.0 release, this version is _not_ subject to the
normal API compatibility guarantees. See the _Compatibility Statement_ for
information about API compatibility guarantees.

## Overview of the Data Module

The CDK Data module is a set of APIs for interacting with datasets in the
Hadoop ecosystem. Specifically built to simplify direct reading and writing
of datasets in storage subsystems such as the Hadoop Distributed FileSystem
(HDFS), the Data module provides familiar, stream-oriented APIs, that remove the
complexity of data serialization, partitioning, organization, and metadata
system integration. These APIs do not replace or supersede any of the existing
Hadoop APIs. Instead, the Data module acts as a targetted application of those
APIs for its stated use case. In other words, many applications will still use
the HDFS or Avro APIs directly when the developer has use cases outside of
direct dataset create, drop, read, and write operations. On the other hand, for
users building applications or systems such as data integration services, the
Data module will usually be superior in its default choices, data organization,
and metadata system integration, when compared to custom built code.

In keeping with the overarching theme and principles of the CDK, the Data module
is prescriptive. Rather than present a do-all Swiss Army knife library, this
module makes specific design choices that guide users toward well-known patterns
that make sense for many, if not all, cases. It is likely that advanced users
with niche use cases or applications will find it difficult, suboptimal, or even
impossible to do unusual things. Limiting the user is not a goal, but when
revealing an option creates significant opportunity for complexity, or would
otherwise require the user to delve into a rathole of additional choices or
topics to research, such a tradeoff has been made. The Data module is designed
to be immediately useful, obvious, and in line with what most users want, out of
the box.

These APIs are designed to easily fit in with dependency injection frameworks
like [Spring][spring] and [Google Guice][guice]. Users can use constructor
injection when using these kinds of systems. Alternatively, users who prefer
not to use DI frameworks will almost certainly prefer the builder-style helper
classes that come with many of the critical classes. By convention, these
builders are always inner static classes named `Builder`, contained within their
constituent classes.

[spring]: http://www.springsource.org/spring-framework "Spring Framework"
[guice]: http://code.google.com/p/google-guice/ "Google Guice"

The primary actors in the Data module are _entities_, _dataset repositories_,
_datasets_, dataset _readers_ and _writers_, and _metadata providers_. Most of
these objects are interfaces, permitting multiple implementations, each with
different functionality. Today, there exists an implementation of each of these
components for the Hadoop FileSystem abstraction, found in the
`com.cloudera.data.filesystem` package. While, in theory, this means any
implementation of Hadoop's `FileSystem` abstract class is supported by the Data
module, only the local and HDFS filesystem implementations are tested and
officially supported. For the remainder of this guide, you can assume the
implementation of the Data module interfaces being described is the Hadoop
`FileSystem` implementation.

If you're not already familiar with [Avro][avro] schemas, now is a good time to
go read a little [more about them][avro-s]. You need not concern yourself with
the details of how objects are serialized, but the ability to specify the schema
to which entities of a dataset must conform is critical. The rest of this guide
will assume you have a basic understanding of how to define a schema.

[avro-s]: http://avro.apache.org/docs/current/spec.html "Avro Specification"

## Dataset Repositories and Metadata Providers

A _dataset repository_ is a physical storage location for datasets. In keeping
with the relational database analogy, a dataset repository is the equivalent of
a database of tables. Developers may organize datasets into different dataset
repositories for reasons related to logical grouping, security and access
control, backup policies, and so forth. A dataset repository is represented by
instances of the `com.cloudera.data.DatasetRepository` interface in the Data
module. An instance of a `DatasetRepository` acts as a factory for datasets,
supplying methods for creating, loading, and dropping datasets. Each dataset
belongs to exactly one dataset repository. There's no built in support for
moving or copying datasets bewteen repositories. MapReduce and other execution
engines can easily provide copy functionality, if desired.

_DatasetRepository Interface_

    Dataset create(String, DatasetDescriptor);
    Dataset get(String);
    boolean drop(String);

The Data module ships with a `DatasetRepository` implementation
`com.cloudera.data.filesystem.FileSystemDatasetRepository` built for operating
on datasets stored in a filesystem supported by Hadoop's `FileSystem`
abstraction. This implementation requires an instance of a Hadoop `FileSystem`,
a root directory under which datasets will be (or are) stored, and an optional
_metadata provider_ (described later) to be supplied upon instantiation. Once
complete, users can freely interact with datasets while the implementation
worries about managing files and directories in the underlying filesystem. The
supplied metadata provider is used to resolve dataset schemas and other related
information.

Instantiating FileSystemDatasetRepository is straight forward.

    DatasetRepository repo = new FileSystemDatasetRepository(
      FileSystem.get(new Configuration()),
      new Path("/data")
    );

This example uses the currently configured Hadoop `FileSystem` implementation,
typically an HDFS cluster. Since Hadoop's also supports a "local" implementation
of `FileSystem`, it's possible to use the Data APIs to interact with data
residing on a local OS filesystem. This is especially useful during development
and basic functional testing of your code.

Once an instance of a `DatasetRepository` implementation has been created,
new datasets can easily be created, and existing datasets can be loaded or
dropped. Here's a more complete example of creating a dataset to store
application event data. You'll notice a few new classes; don't worry about them
for now. We'll cover them later.

    /*
     * Instantiate a FileSystemDatasetRepository with a Hadoop FileSystem object
     * based on the current configuration. The FileSystem and Configuration
     * classes come from Hadoop. The Path object tells the dataset repository
     * under what path in the configured filesystem datasets reside.
     */
    DatasetRepository repo = new FileSystemDatasetRepository(
      FileSystem.get(new Configuration()),
      new Path("/data")
    );

    /*
     * Create the dataset "users" with the schema defined in the file User.avsc.
     */
    Dataset users = repo.create(
      "users",
      new DatasetDescriptor.Builder()
        .schema(new File("User.avsc"))
        .get()
    );

Related to the dataset repository, the _metadata provider_ is a service provider
interface used to interact with the service that provides dataset metadata
information to the rest of the Data APIs. The
`com.cloudera.data.MetadataProvider` interface defines the contract metadata
services must provide to the library, and specifically, the `DatasetRepository`.

_MetadataProvider Interface_

    void save(String, DatasetDescriptor);
    DatasetDescriptor load(String);
    boolean delete(String);

`MetadataProvider` implementations act as a bridge between the Data module and
centralized metadata repositories. An obvious example of this (in the Hadoop
ecosystem) is [HCatalog][hcat] and the Hive metastore. By providing an
implementation that makes the necessary API calls to HCatalog's REST service,
any and all datasets are immediately consumable by systems compatible with
HCatalog, the storage system represented by the `DatasetRepository`
implementation, and the format in which the data is written. As it turns out,
that's a pretty tall order and, in keeping with the CDK's purpose of simplifying
rather than presenting additional options, users are encouraged to 1. use
HCatalog, 2. allow this library to default to snappy compressed Avro data files,
and 3. use systems that also integrate with HCatalog (directly or indirectly).
In this way, this library acts as a forth integration point to working with data
in HDFS that is HCatalog-aware, in addition to Hive, Pig, and MapReduce
input/output formats.

[hcat]: http://incubator.apache.org/hcatalog/ "Apache HCatalog"

Users aren't expected to use metadata providers directly. Instead,
`DatasetRepository` implementations accept instances of `MetadataProvider`
plugins, and make whatever calls are needed as users interact with the Data
APIs. If you're paying close attention, you'll see that we didn't specify a
metadata provider when we instantiated `FileSystemDatasetRepository` earlier.
That's because `FileSystemDatasetRepository` uses an implementation of
`MetadataProvider` called `FileSystemMetadataProvider` by default. Developers
are free to explicitly pass a different implementation using the three argument
constructor `FileSystemDatasetRepository(FileSystem, Path, MetadataProvider)` if
they want to change this behavior.

The `FileSystemMetadataProvider` (also in the packge
`com.cloudera.data.filesystem`) plugin stores dataset metadata information on
a Hadoop `FileSystem` in a hidden directory. As with its sibling
`FileSystemDatasetRepository`, its constructor accepts a Hadoop `FileSystem`
object and a base directory. When metadata needs to be stored, a directory
under the supplied base directory with the dataset name is created (if it
doesn't yet exist), and the dataset descriptor information is serialized to a
set of files in a directory named `.metadata`.

_Example: Explicitly configuring `FileSystemDatasetRepository` with
`FileSystemMetadataProvider`_

    FileSystem fileSystem = FileSystem.get(new Configuration());
    Path basePath = new Path("/data");

    MetadataProvider metaProvider = new FileSystemMetadataProvider(
      fileSystem, basePath);

    DatasetRepository repo = new FileSystemDatasetRepository(
      fileSystem, basePath, metaProvider);

Note how the same base directory of `/data` is used for both the metadata
provider as well as the dataset repository. This is perfectly legal. In fact,
this is exactly what happens when you use the two argument form of the
`FileSystemDatasetRepository` constructor. Configured this way, data and
metadata will be stored together, side by side, on whatever filesystem Hadoop is
currently configured to use. Later, when we create a dataset, we'll see the
resultant file and directory structure created as a result of this
configuration.

To use HCatalog, create an instance of `HCatalogDatasetRepository` (in the package
`com.cloudera.data.hcatalog`). `HCatalogDatasetRepository` uses an internal
implementation of `MetadataProvider` called `HCatalogMetadataProvider` to communicate
with HCatalog. When using HCatalog you have two options for specifying the location of
the data files. You can let HCatalog manage the location of the data,
the so called "managed tables" option, in which case the data is stored in the warehouse
directory that is configured by the Hive/HCatalog installation (see the
`hive.metastore.warehouse.dir` setting in _hive-site.xml_). Alternatively,
you can provide an explicit Hadoop `FileSystem` and root directory for datasets,
just like `FileSystemDatasetRepository`. The latter option is referred to as
"external tables" in the context of Hive/HCatalog.

_Example: Creating a `HCatalogDatasetRepository` with managed tables_

    DatasetRepository repo = new HCatalogDatasetRepository();

## Datasets

_Summary_

* A dataset is a collection of entities.
* A dataset is represented by the interface `Dataset`.
* The Hadoop FileSystem implementation of a dataset...
    * is stored as Snappy-compressed Avro data files by default.
    * may support pluggable formats such as Parquet in the future.
    * is made up of zero or more files in a directory.

A dataset is a collection of zero or more entities (or records). All datasets
have a name and an associated _dataset descriptor_. The dataset descriptor, as
the name implies, describes all aspects of the dataset. Primarily, the
descriptor information is the dataset's required _schema_ and its optional
_partition strategy_. A descriptor must be provided at the time a dataset is
created. The schema is defined using the Avro Schema APIs. Entities must all
conform to the same schema, however, that schema can evolve based on a set of
well-defined rules. The relational database analog of a dataset is a table.

Datasets are represented by the `com.cloudera.data.Dataset` interface.
Implementations of this interface decide how to physically store the entities
within the dataset. Users do not instantiate implementations of the `Dataset`
interface directly. Instead, implementations of the `DatasetRepository` act as
a factory of the appropriate `Dataset` implementation.

_Dataset Interface_

    String getName();
    DatasetDescriptor getDescriptor();

    <E> DatasetWriter<E> getWriter();
    <E> DatasetReader<E> getReader();

    Dataset getPartition(PartitionKey, boolean);
    Iterable<Dataset> getPartitions();

The included Hadoop `FileSystemDatasetRepository` includes a `Dataset`
implementation called `FileSystemDataset`. This dataset implementation stores
data in the configured Hadoop `FileSystem` as Snappy-compressed Avro data files.
Avro data files were selected because all components of CDH support them, they
are language agnostic, support block compression, have a compact binary
representation, and are natively splittable by Hadoop MapReduce while
compressed. While it's not currently possible to change this behavior, this will
suit the needs of almost all users, and should be used whenever possible.
Support for different serialization formats is planned for future releases for
cases where users know something special about their data.

Upon creation of dataset, a name and a _dataset descriptor_ must be provided to
the `DatasetRepository#create()` method. The descriptor, represented by the
`com.cloudera.data.DatasetDescriptor` class, holds all metadata associated with
the dataset, the most important of which is the schema. Schemas are always
represented using Avro's Schema APIs, regardless of how the data is stored by
the underlying dataset implementation. This simplifies the API for users,
letting them focus on a single schema definition language for all datasets. In
an effort to support different styles of schema definition, the
`DatasetDescriptor.Builder` class supports a number of convenience methods for
defining or attaching a schema.

_DatasetDescriptor Class_

    org.apache.avro.Schema getSchema();
    PartitionStrategy getPartitionStrategy();
    boolean isPartitioned();

_DatasetDescriptor.Builder Class_

    Builder schema(Schema schema);
    Builder schema(String json);
    Builder schema(File file);
    Builder schema(InputStream inputStream);
    Builder schema(URL url);

    Builder partitionStrategy(PartitionStrategy partitionStrategy);

    DatasetDescriptor get();

_Note_

Some of the less important or specialized methods have been elided here in the
interest of simplicity.

From the methods in the `DatasetDescriptor.Builder`, you can see Avro schemas
can be defined in a few different ways. Here, for instance, is an example of
creating a dataset with a schema defined in a file on the local filesystem.

    DatasetRepository repo = ...
    Dataset users = repo.create("users",
      new DatasetDescriptor.Builder()
        .schema(new File("User.avsc"))
        .get()
    );

Just as easily, a the schema could be loaded from a Java classpath resource.
This example uses Guava's [Resources][guava-resources-cls] to simplify
resolution, but we could have (almost as) easily used Java's
`java.util.ClassLoader` directly.

[guava-resources-cls]: http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/io/Resources.html "Google Guava Resources class"

    DatasetRepository repo = ...
    Dataset users = repo.create("users",
      new DatasetDescriptor.Builder()
        .schema(Resources.getResource("Users.avsc"))
        .get()
    );

An instance of `Dataset` acts as a factory for both reader and writer streams.
Each implementation is free to produce stream implementations that make sense
for the underlying storage system. The `FileSystemDataset` implementation, for
example, produces streams that read from, or write to, Avro data files on a
Hadoop `FileSystem` implementation.

Reader and writer streams both function similarly to Java's standard IO streams,
but are specialized. As indicated in the `Dataset` interface earlier, both
interfaces are generic. The type parameter indicates the type of entity that
they produce or consume, respectively.

_DatasetReader Interface_

    void open();
    void close();
    boolean isOpen();

    boolean hasNext();
    E read();

_DatasetWriter Interface_

    void open();
    void close();
    boolean isOpen();

    void write(E);
    void flush();

Both readers and writers are single-use objects with a well-defined lifecycle.
Instances of both types (or the implementations of each, rather) must be opened
prior to invoking any of the IO-generating methods such as DatasetReader's
`hasNext()` or `read()`, or DatasetWriter's `write()` or `flush()`. Once a
stream has been closed via the `close()` method, no further IO is permitted,
nor may it be reopened.

Writing to a dataset always follows the same sequence of events. A user obtains
an instance of a `Dataset` from a `DatasetRepository` either by creating a new,
or loading an existing dataset. With a reference to a `Dataset`, you can obtain
a writer using its `getWriter()` method, open it, write any number of entities,
flush as necessary, and close it to release resources back to the system. The
use of `flush()` and `close()` can dramatically affect data durability.
Implementations of the `DatasetWriter` interface are free to define the
semantics of data durability as appropriate for their storage subsystem. See
the implementation javadoc on either the streams or the dataset for more
information.

_Example: Writing to a Hadoop FileSystem_

    DatasetRepository repo = new FileSystemDatasetRepository(
      FileSystem.get(new Configuration()), new Path("/data"));

    /*
     * Let's assume MyInteger.avsc is defined as follows:
     * {
     *   "type": "record",
     *   "name": "MyInteger",
     *   "fields": [
     *     { "type": "integer", "name": "i" }
     *   ]
     * }
     */
    Dataset integers = repo.create("integers",
      new DatasetDescriptor.Builder()
        .schema("MyInteger.avsc")
        .get()
    );

    /*
     * Getting a writer never performs IO, so it's safe to do this outside of
     * the try block. Here we're using Avro Generic records, discussed in
     * greater details later. See the Entities section.
     */
    DatasetWriter<GenericRecord> writer = integers.getWriter();

    try {
      writer.open();

      for (int i = 0; i < Integer.MAX_VALUE; i++) {
        writer.write(
          new GenericRecordBuilder(integers.getDescriptor().getSchema())
            .set("i", i)
            .build()
        );
      }
    } finally {
      // Always explicitly close writers!
      writer.close();
    }

Reading data from an existing dataset is equally straight forward. The important
part to remember is that it is illegal to call `read()` after the reader has
been exhausted (i.e. no more entities remain). Instead, users must use the
`hasNext()` method to test if the reader can produce further data.

_Example: Reading from a Hadoop FileSystem_

    DatasetRepository repo = new FileSystemDatasetRepository(
      FileSystem.get(new Configuration()), new Path("/data"));

    // Load the integers dataset.
    Dataset integers = repo.get("integers");

    DatasetReader<GenericRecord> reader = integers.getReader();

    try {
      reader.open();

      while (reader.hasNext()) {
        System.out.println("i: " + reader.read().get("i"));
      }
    } finally {
      reader.close();
    }

Dropping a dataset - an operation as equally destructive as dropping a table
in a relational database - works as expected.

_Example: Dropping an existing dataset_

    DatasetRepository repo = new FileSystemDatasetRepository(
      FileSystem.get(new Configuration()), new Path("/data"));

    if (repo.drop("integers")) {
      System.out.println("Dropped dataset integers");
    }

As discussed earlier, all operations performed on dataset repositories,
datasets, and their associated readers and writers are tightly integrated with
the dataset repository's configured metadata provider. Dropping a dataset like
this, for example, removes both the data as well as the associated metadata.
All applications that use the Data module APIs will automatically see changes
made by one another if they share the same configuration. This is an incredibly
powerful concept allowing systems to become immediately aware of data as soon
as it's committed to storage.

### Partitioned Datasets

_Summary_

* Datasets may be partitioned by attributes of the entity (i.e. fields of the
  record).
* Partitioning is transparent to readers and writers.
* Partitons also conform to the `Dataset` interface.
* A `PartitionStrategy` controls how a dataset is partitioned, and is part of
  the `DatasetDescriptor`.

Datasets may optionally be partitioned to facilitate piecemeal storage
management, as well as optimized access to data under one or more predicates. A
dataset is considered partitioned if it has an associated partition strategy
(described later). When entities are written to a partitioned dataset, they are
automatically written to the proper partition, as expected. The semantics of a
partition are defined by the implementation; no guarantees as to the performance
of reading or writing across partitions, availability of a partition in the face
of failures, or the efficiency of partition elimination under one or more
predicates (i.e. partition pruning in query engines) are made by the Data module
interfaces. It is not possible to partition an existing non-partitioned dataset,
nor can you write data into a partitioned dataset that does not land in
a partition. Should you decide to partition an existing dataset, the best course
of action is to create a new partitioned dataset with the same schema as the
existing dataset, and use MapReduce to convert the dataset in batch to the new
format. It will be possible to remove partitions from a partitioned dataset in
a future release, but today, they may only be added. A partitioned dataset can
provide a list of partitions (described later).

Upon creation of a dataset, a `PartitionStrategy` may be provided. A partition
strategy is a list of one or more partition functions that, when applied to an
attribute of an entity, produce a value used to decide in which partition an
entity should be written. Different partition function implementations exist,
each of which faciliates a different form of partitioning. The initial version
of the library includes the identity and hash functions for use in partition
strategies.

While users are free to instantiate the `PartitionStrategy` class directly, its
`Builder` greatly simpifies life.

_PartitionStrategy.Builder API_

    Builder identity(String, int);
    Builder hash(String, int);

    PartitionStrategy get();

When building a partition strategy, the attribute (i.e. field) name from which
to take the function input is specified, along with a cardinality hint (or
limit, in the case of the hash function). For example, given the Avro schema for
a `User` entity with a `segment` attribute of type `long`, a partition strategy
that uses the identity function on the `segment` attribute will effectively
"bucket" users by their segment value.

_Sample User Avro Schema (User.avsc)_

    {
      "name": "User",
      "type": "record",
      "fields": [
        { "name": "id",           "type": "long"   },
        { "name": "username",     "type": "string" },
        { "name": "emailAddress", "type": "string" },
        { "name": "segment",      "type": "long"   }
      ]
    }

_Example Creation of a dataset partitioned by an attribute_

    DatasetRepository repo = ...

    Dataset usersDataset = repo.create(
      "users",
      new DatasetDescriptor.Builder()
        .schema(new File("User.avsc"))
        .partitionStrategy(
          new PartitionStrategy.Builder().identity("segment", 1024).get()
        ).get()
    );

Given the ficticious User entities shown in _Example: Sample Users_, users A, B,
and C would be written to partition 1, while D and E end up in partition 2.

_Example: Sample Users_

    id  username  emailAddress  segment
    --  --------  ------------  -------
    100 A         A@a.com       1
    101 B         B@b.com       1
    102 C         C@c.com       1
    103 D         D@d.com       2
    104 E         E@e.com       2

Partitioning is not limited to a single attribute of an entity.

_Example: Creation of a dataset partitioned by multiple attributes_

    DatasetRepository repo = ...

    Dataset users = repo.create(
      "users",
      new DatasetDescriptor.Builder()
        .schema(new File("User.avsc"))
        .partitionStrategy(
          new PartitionStrategy.Builder()
            .identity("segment", 1024)  // Partition first by segment
            .hash("emailAddress", 3)    // and then by hash(email) % 3
            .get()
        ).get()

The order in which the partition functions are defined is important. This
controls the way the data is physically partitioned in certain implementations
of the Data APIs. Depending on the implementation, this can drastically change
the execution speed of data access by different methods.

__Warning__

It's worth pointing out that Hive and Impala only support the identity function
in partitioned datasets, at least at the time this is written. Users who do not
use partitioning for subset selection may use any partition function(s) they
choose. If, however, you wish to use the partition pruning in Hive/Impala's
query engine, only the identity function will work. This is because both systems
rely on the idea that the value in the path name equals the value found in each
record. To mimic more complex partitioning schemes, users often resort to adding
a surrogate field to each record to hold the dervived value and handle proper
setting of such a field themselves.

## Entities

_Summary_

* An entity is a record in a dataset.
* Entities can be POJOs, Avro GenericRecords, or Avro generated (specific)
  records.
* When in doubt, use GenericRecords.

An _entity_ is a is a single record. The name "entity" is used rather than
"record" because the latter caries a connotation of a simple list of primitives,
while the former evokes the notion of a [POJO][] (e.g. in [JPA][]). That said,
the terms are used interchangably. An entity can take one of three forms, at the
user's option:

1. A plain old Java object

   When a POJO is supplied, the library uses reflection to write the object out
   to storage. While not the fastest, this is the simplest way to get up and
   running. Users are encouraged to consider Avro [GenericRecord][avro-gr]s for
   production systems, or after they become familiar with the APIs.

1. An [Avro][avro] GenericRecord

   An Avro [GenericRecord][avro-gr] instance can be used to easily supply
   entities that represent a schema without using custom types for each kind of
   entity. These objects are easy to create and manipulate (see Avro's
   [GenericRecordBuilder class][avro-grb]), especially in code that has no
   knowledge of specific object types (such as libraries). Serialization of
   generic records is fast, but requires use of the Avro APIs. This is
   recommended for most users, in most cases.

1. An Avro specific type

   Advanced users may choose to use Avro's [code generation][avro-cg] support to
   create classes that implicitly know how to serialize themselves. While the
   fastest of the options, this requires specialized knowledge of Avro, code
   generation, and handling of custom types. Keep in mind that, unlike generic
   records, the applications that write datasets with specific types must also
   have the same classes available to the applications that read those datasets.

Note that entities aren't represented by any particular type in the Data APIs.
In each of the above three cases, the entities described are either simple POJOs
or are Avro objects. Remember that what has been described here is only the _in
memory_ representation of the entity; the Data module may store the data in HDFS
in a different serialization format.

[POJO]: http://en.wikipedia.org/wiki/POJO "Plain Old Java Object"
[JPA]: http://en.wikipedia.org/wiki/Java_Persistence_API "Java Persistance API"
[avro-gr]: http://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericRecord.html "Avro - GenericRecord Interface"
[avro-grb]: http://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericRecordBuilder.html "Avro - GenericRecordBuilder Class"
[avro-cg]: http://avro.apache.org/docs/current/gettingstartedjava.html#Serializing+and+deserializing+with+code+generation "Avro - Serializing and deserializing with code generation"

Entites may be complex types, representing data structures as simple as a few
string attributes, or as complex as necessary. See _Example: User entity schema
and POJO class_ for an example of a valid Avro schema, and its associated POJO.

_Example: User entity schema and POJO class_

    Avro schema (User.avsc)
    -----------------------
    {
      "name": "User",
      "type": "record",
      "fields": [
        // two required fields.
        { "name": "id",          "type": "long" },
        { "name": "username",    "type": "string" },

        // emailAddress is optional; it's value can be a string or a null.
        { "name": "emailAddress", "type": [ "string", "null" ] },

        // friendIds is an array with elements of type long.
        { "name": "friendIds",   "type": { "type": "array", "items": "long" } },
      ]
    }

    User POJO (User.java)
    ---------------------
    public class User {

      private Long id;
      private String username;
      private String emailAddress;
      private List<Long> friendIds;

      public User() {
        friendIds = new ArrayList<Long>();
      }

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getUsername() {
        return username;
      }

      public void setUsername(String username) {
        this.username = username;
      }

      public String getEmailAddress() {
        return emailAddress;
      }

      public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
      }

      public List<Long> getFriendIds() {
        return friendIds;
      }

      public void setFriendIds(List<Long> friendIds) {
        this.friendIds = friendIds;
      }

      /*
       * It's fine to have methods that the schema doesn't know about. They'll
       * just be ignored during serialization.
       */
      public void addFriend(Friend friend) {
        if (!friendIds.contains(friend.getId()) {
          friendIds.add(friend.getId());
        }
      }

    }

Instead of defining a POJO, we could also use Avro's `GenericRecordBuilder` to
create a generic entity that conforms to the User schema we defined earlier.

_Example: Using Avro's GenericRecordBuilder to create a generic entity_

    /*
     * Load the schema from User.avsc. Later, we'll an easier way to reference
     * Avro schemas when working with the CDK Data APIs.
     */
    Schema userSchema = new Schema.Parser().parse(new File("User.avsc"));

    /*
     * The GenericRecordBuilder constructs a new record and ensures that we set
     * all the necessary fields with values of an appropriate type.
     */
    GenericRecord genericUser = new GenericRecordBuilder(userSchema)
      .set("id", 1L)
      .set("username", "janedoe")
      .set("emailAddress", "jane@doe.com")
      .set("friendIds", Collections.<Long>emptyList())
      .build();

Later, we'll see how to read and write these entities to a dataset.

## Appendix

### Compatibility Statement

As a library, users must be able to reliably determine the intended
compatibility of this project. We take API stability and compatibility
seriously; any deviation from the stated guarantees is a bug. This project
follows the guidelines set forth by the [Semantic Versioning
Specification][semver] and uses the same nomenclature.

Just as with CDH (and the Semantic Versioning Specification), this project makes
the following compatibility guarantees:

1. The patch version is incremented if only backward-compatible bug fixes are
   introduced.
1. The minor version is incremented when backward-compatible features are added
   to the public API, parts of the public API are deprecated, or when changes
   are made to private code. Patch level changes may also be included.
1. The major version is incremented when backward-incompatible changes are made.
   Minor and patch level changes may also be included.
1. Prior to version 1.0.0, no backward-compatibility is guaranteed.

See the [Semantic Versioning Specification][semver] for more information.

Additionally, the following statements are made:

* The public API is defined by the Javadoc.
* Some classes may be annotated with @Beta. These classes are evolving or
  experimental, and are not subject to the stated compatibility guarantees. They
  may change incompatibly in any release.
* Deprecated elements of the public API are retained for two releases and then
  removed. Since this breaks backward compatibility, the major version must also
  be incremented.

[semver]: http://semver.org/
