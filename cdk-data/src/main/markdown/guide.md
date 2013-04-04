# CDK Data Reference Guide

## About This Guide

This reference guide is the primary source of documentation for the CDK Data
module. It covers the high level organization of the APIs,
primary classes and interfaces, intended usage, available extension points
for customization, and implementation information where helpful and
appropriate.

From here on, this guide assumes you are already familiar with the basic
design and functionality of HDFS, Hadoop MapReduce, and Java SE 1.6. Users
who are also familiar with [Avro][avro], data serialization techniques,
common compression algorithms (e.g. gzip, snappy), advanced Hadoop MapReduce
topics (e.g. input split calculation), and tranditional data management
topics (e.g. partitioning schemes, metadata management) will benefit even more.

[avro]: http://avro.apache.org "Apache Avro"

## What's New

### Version 0.1.0

Version 0.1.0 is the first release of the CDK Data module. This is considered a
*beta* release. As a sub-1.0.0 release, this version is *not* subject to the
normal API compatibility guarantees. See the *Compatibility Statement* for
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

The primary actors in the Data module are *entities*, *dataset repositories*,
*datasets*, dataset *readers* and *writers*, and *metadata providers*. Most of
these objects are interfaces, permitting multiple implementations, each with
different functionality. Today, there exists an implementation of each of these
components for the Hadoop FileSystem abstraction. While, in theory, this means
any implementation of Hadoop's `FileSystem` abstract class is supported by the
Data module, only the local and HDFS filesystem implementations are tested and
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

    create(String, DatasetDescriptor): Dataset
    get(String): Dataset
    drop(String): boolean

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

    save(String, DatasetDescriptor)
    load(String): DatasetDescriptor
    delete(String): boolean

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

_Example: Explicitly configuring `FileSystemDatasetRepository` with `FileSystemMetadataProvider`_

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

## Entities

*Summary*

* An entity is a record in a dataset.
* Entities can be POJOs, GenericRecords, or generated (specific) records.
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
or are Avro objects. Remember that what has been described here is only the *in
memory* representation of the entity; the Data module may store the data in HDFS
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
    GenericData.Record genericUser = new GenericRecordBuilder(userSchema)
      .set("id", 1L)
      .set("username", "janedoe")
      .set("emailAddress", "jane@doe.com")
      .set("friendIds", Collections.<Long>emptyList())
      .build();

Later, we'll see how to read and write these entities to a dataset.

## Datasets

*Summary*

* A dataset is a collection of entities.
* A dataset can be partitioned by attributes of the entity (i.e. fields of the
  record).
* A dataset is represented by the interface `Dataset`.
* The Hadoop FileSystem implementation of a dataset...
    * is stored as Snappy-compressed Avro data files by default.
    * may support pluggable formats such as Parquet in the future.
    * is made up of zero or more files in a directory.

A dataset is a collection of zero or more entities. All datasets have a name and
an associated _dataset descriptor_. The dataset descriptor, as the name implies,
describes all aspects of the dataset. Primarily, the descriptor information is
the dataset's required _schema_ and its optional _partition strategy_. A
descriptor must be provided at the time a dataset is created. The schema is
defined using the Avro Schema APIs. Entities must all conform to the same
schema, however, that schema can evolve based on a set of well-defined rules.
The relational analog of a dataset is a table.

_Dataset Interface_

    getName(): String
    getDescriptor(): DatasetDescriptor

    <E> getWriter(): DatasetWriter<E>
    <E> getReader(): DatasetReader<E>

    getPartition(PartitionKey, boolean): Dataset
    getPartitions(): Iterable<Dataset>

While Avro is used to define the schema, it is possible that the underlying
storage format of the data is not Avro data files. This is because other
constraints may apply, based on the subsystems or access patterns. The dataset
implementation translates the Avro schema into whatever is appropriate for the
underlying system automatically. The Hadoop FileSystem implementation does, in
fact, store all data as Avro data files in the configured filesystem.

Datasets may optionally be partitioned to facilitate piecemeal storage
management, as well as optimized access to data under one or more predicates. A
dataset is considered partitioned if it has an associated partition strategy
(described later). When entities are written to a partitioned dataset, they are
automatically written to the proper partition, as expected. The semantics of a
partition are defined by the implementation; this interface makes no guarantee
as to the performance of reading or writing across partitions, availability of a
partition in the face of failures, or the efficiency of partition elimination
under one or more predicates (i.e. partition pruning in query engines). It is
not possible to partition an existing non-partitioned dataset, nor can you write
data into a partitioned dataset that does not land in a partition. Should you
decide to partition an existing dataset, the best course of action is to create
a new partitioned dataset with the same schema as the existing dataset, and use
MapReduce to convert the dataset in batch to the new format. It is possible to
add or remove partitions from a partitioned dataset. A partitioned dataset can
provide a list of partitions (described later).

_DatasetDescriptor Class_

    getSchema(): org.apache.avro.Schema
    getPartitionStrategy(): PartitionStrategy
    isPartitioned(): boolean

Datasets are never instantiated by users, directly. Instead, they are created
using factory methods on a `DatasetRepository` (described later).

An instance of `Dataset` acts as a factory for both reader and writer streams.
Each implementation is free to produce stream implementations that make sense
for the underlying storage system. The Hadoop `FileSystem` implementation, for
example, produces streams that read from, or write to, Avro data files on a
`FileSystem` implementation.

Reader and writer streams both function similarly to Java's standard IO streams,
but are specialized. As indicated above, both interfaces are generic. The type
parameter indicates the type of entity that they produce or consume,
respectively.

_DatasetReader<E> Interface_

    open()
    close()
    isOpen(): boolean

    hasNext(): boolean
    read(): E

_DatasetWriter<E> Interface_

    open()
    close()
    isOpen(): boolean

    write(E)
    flush()

Both readers and writers are single-use objects with a well-defined lifecycle.
Instances of both types (or the implementations of each, rather) must be opened
prior to invoking any of the IO-generating methods such as DatasetReader's
`hasNext()` or `read()`, or DatasetWriter's `write()` or `flush()`. Once a
stream has been closed via the `close()` method, no further IO is permitted,
nor may it be reopened.

### Partitioned Datasets

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

    identity(String, int): Builder
    hash(String, int): Builder

    get(): PartitionStrategy

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

    DatasetRepository repo = ... // Described later.

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

The other in which the partition functions are defined is important. This
controls the way the data is physically partitioned in certain implementations
of the Data APIs. Depending on the implementation, this can drastically change
the execution speed of data access.

**Warning**

It's worth pointing out that Hive and Impala only support the identity function
in partitioned datasets, at least at the time this is written. Users who do not
use partitioning for subset selection may use any partition function(s) they
choose. If, however, you wish to use the partition pruning in Hive/Impala's
query engine, only the identity function will work. This is because both systems
rely on the idea that the value in the path name equals the value found in each
record. To mimic more complex partitioning schemes, users often resort to adding
a surrogate field to each record to hold the dervived value and handle proper
setting of such a field themselves.

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
