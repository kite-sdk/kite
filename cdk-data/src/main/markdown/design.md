# CDK Data Module

This is the design draft for the CDK Data module.

## Overview

Today, interacting with data "in Hadoop" means working with files and
directories. For users who are used to thinking in terms of tables and
databases, this is surprising low-level and complicated. Seemingly simple tasks
such as organizing and managing data hierarchies present a barrier to adoption.
Selecting from the matrix of file formats, compression codecs, tool support, and
serialization formats stumps just about every developer new to the platform. If
we could reduce this decision set by providing experience in library form, we
could reduce another barrier to adoption, and increase usability.

The CDK Data module is a Java library intended to solve this exact set of
problems. By providing a logical abstraction consisting of _dataset
repositories_, _datasets_, _entities_, and reader/writer streams, users can get
on with programmatic direct dataset access and management. The intention and
expectation is that this library will be useful, primarily, to those writing
data integration tools, either custom, or ISV-provided, but also anyone that
needs to touch data in HDFS directly (i.e. not by way of MapReduce or higher
level engines such as Impala). Accessibility and integration with the rest of
the platform are the two primary objectives. In other words, this library is one
level below Hadoop's input/output formats for MapReduce, but above the standard
HDFS+[Avro][] APIs.

The Data APIs are primarily a set of generic interfaces and classes for storing
and retrieving entities (i.e. records) to and from datasets (i.e. tables).
Datasets are, in turn, grouped into dataset repositories (i.e. databases).
Reads and writes are performed using streams that are always attached to a
dataset. The inital implementation will be focused on large, sequential
operations.

Implementations of the generic interfaces can exist for different storage
systems.  Today, the only implementation is built on top of Hadoop's FileSystem
abstraction which means anything FileSystem supports, the Data APIs should
support. The local filesystem and HDFS implementations are the primary testing
targets.

There is some logical overlap with existing systems that's worth pointing out,
however this library is, in fact, complementary to all of these systems. This is
discussed in the context of each system, in detail, in the section _Related Work
and Systems_.

[Avro]: http://avro.apache.org/
    (Apache Avro)

### Entities

Summary:

* An entity is a record in a dataset.
* Entities can be POJOs, GenericRecords, or generated (specific) records.
* When in doubt, use GenericRecords.

An _entity_ is a is a single record. The name "entity" is used rather than
"record" because the latter caries a connotation of a simple list of primitives,
while the former evokes the notion of a [POJO][] (e.g. in [JPA][]). An entity
can take one of three forms, at the user's option:

1. A plain old Java object

   When a POJO is supplied, the library uses reflection to write the object out
   to storage. While not the fastest, this is the simplest way to get up and
   running. Users are encourage to consider Avro [GenericRecord][avro-gr]s for
   production systems, or after they become familiar with the APIs.

1. An [Avro][] GenericRecord

   An Avro [GenericRecord][avro-gr] instance can be used to easily supply
   entities that represent a schema. These objects are easy to create and
   manipulate, especially in code that has no knowledge of specific object types
   (such as libraries). Serialization of generic records is fast, but requires
   use of the Avro APIs. This is recommended for most users.

1. An Avro specific type

   Advanced users may choose to use Avro's [code generation][avro-cg] support to
   create classes that implicitly know how to serialize themselves. While the
   fastest of the options, this requires specialized knowledge of Avro, code
   generation, and handling of custom types.

[POJO]: http://en.wikipedia.org/wiki/POJO
    (Plain Old Java Object)
[JPA]: http://en.wikipedia.org/wiki/Java_Persistence_API
    (Java Persistance API)
[avro-gr]: http://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericRecord.html
    (Avro - GenericRecord Interface)
[avro-cg]: http://avro.apache.org/docs/current/gettingstartedjava.html#Serializing+and+deserializing+with+code+generation
    (Avro - Serializing and deserializing with code generation)

### Datasets

Summary:

* A dataset is a collection of entities.
* A dataset may be partitioned by attributes of the entity (i.e. fields of the
  record).
* A dataset is represented by the interface `Dataset`.
* The Hadoop FileSystem implementation of a dataset...
    * is stored as Snappy-compressed Avro by default.
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

While Avro is used to define the schema, it is possible that the underlying
storage format of the data is not Avro data files. This is because other
constraints may apply, based on the subsystems or access patterns. The
implementing class is expected to translate the Avro schema into whatever is
appropriate for the underlying system. Implementations, of course, are free to
use Avro serialization if it makes sense. The current thinking is that Avro data
files will be the default for data in HDFS.

Datasets may optionally be partitioned to facilitate piecemeal storage
management, as well as optimized access to data under one or more predicates. A
dataset is considered partitioned if it has an associated partition strategy
(described later). When records are written to a partitioned dataset, they are
automatically written to the proper partition, as expected. The semantics of a
partition are defined by the implementation; this interface makes no guarantee
as to the performance of reading or writing across partitions, availability of a
partition in the face of failures, or the efficiency of partition elimination
under one or more predicates (i.e. partition pruning in query engines). It is
not possible to partition an existing non-partitioned dataset, nor can one write
data into a partitioned dataset that does not land in a partition. It is
possible to add or remove partitions from a partitioned dataset. A partitioned
dataset can provide a list of partitions (described later).

_The DatasetDescriptor API_

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

_`Dataset` stream factory methods_

    <E> getReader(): DatasetReader<E>
    <E> getWriter(): DatasetWriter<E>

Reader and writer streams both function similarly to Java's standard IO streams,
but are specialized. As indicated above, both interfaces are generic. The type
parameter indicates the type of entity that they produce or consume,
respectively.

_Stream interfaces_

    DatasetReader<E>

        open()
        close()
        isOpen(): boolean

        hasNext(): boolean
        read(): E

    DatasetWriter<E>

        open()
        close()
        isOpen(): boolean

        write(E)
        flush()

Upon creation of a dataset, a `PartitionStrategy` may be provided. A partition
strategy is a list of one or more partition functions that, when applied to an
attribute of an entity, produce a value used to decide in which partition an
entity should be written. Different partition function implementations exist,
each of which faciliates a different form of partitioning. The initial version
of the library includes the identity, hash, range, and value list functions.

_A Partitioned Dataset Example_

    /* Assume the content of userSchema is defined as follows:
     * {
     *   "type": "record",
     *   "name": "User",
     *   "fields": [
     *     { "type": "long", "name": "userId" },
     *     { "type": "string", "name": "username" }
     *   ]
     * }
     */
    Schema userSchema = ...;

    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(
      FileSystem.get(new Configuration()),
      new Path("/data"),
      new FileSystemMetadataProvider(fileSystem, new Path("/data"))
    );

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
      .schema(userSchema)
      .partitionStrategy(
        /*
         * Partition the users dataset using the hash code of the value of the
         * userId attribute modulo 53.
         */
        new PartitionStrategy.Builder().hash("userId", 53).get()
      ).get();

    Dataset users = repo.create("users", desc);
    DatasetWriter[Record] writer = users.getWriter();

    try {
      writer.open();

      /*
       * This writes to /data/users/data/userId=15/*.avro because
       * (Integer.valueOf(1234).hashCode() & Integer.MAX_VALUE) % 53 = 15
       */
      writer.write(
        new GenericRecordBuilder(userSchema)
          .set("userId", 1234)
          .set("username", "jane")
          .build()
      );

      writer.flush();
    } finally {
      writer.close();
    }

This example produces a dataset that, when written to, may have up to 53
partitions. User entities written to the dataset will be automatically written
to the correct partition. Note that the name of the attribute used in the
partition strategy builder ("userId") must appear in the schema ("user.avsc").
Multiple partition functions may be specified. The order of specification is
extremely important as it reflects the physical storage (in the case of the
Hadoop FileSystem implementation).

It's worth pointing out that Hive and Impala only support the identity function
in partitioned datasets, at least at the time this is written. Users who do not
use partitioning for subset selection may use any partition function(s) they
choose. If, however, you wish to use the partition pruning in Hive/Impala's
query engine, only the identity function will work. This is because both systems
rely on the idea that the value in the path name equals the value found in each
record. If you look closely at the earlier example, you'll see that while the
value of the userId attribute in record is 1234, its value in the path is 15. To
mimic more complex partitioning schemes, users often resort to adding a
surrogate field to each record to hold the dervived value and handle proper
setting of such a field themselves.

The equivalent workaround for the hashed field example above is to add a new
attribute to the User entity called `userIdHash`, set it to the proper value in
user code, and use the identity function on that column instead. Note that this
means partition pruning is no longer transparent; the user must know to query
the table using `... WHERE userIdHash = (hashCode(SOME_VALUE) % 53)`. The hope
is that these engines learn about more complex partitioning schemes in the
future.

### Dataset Repositories and Metadata Providers

A _dataset repository_ is a physical storage location for zero or more datasets.
In keeping with the relational database analogy, a dataset repository is the
equivalent of a database. An instance of a DatasetRepository acts as a factory
for Datasets, supplying methods for creating, loading, and dropping datasets.
Each dataset belongs to exactly one dataset repository. There's no built in
support for moving or copying datasets bewteen repositories. MapReduce and other
execution engines can easily provide copy functionality if it's desirable.

_DatasetRepository APIs_

    DatasetRepository

        get(String): Dataset
        create(String, DatasetDescriptor): Dataset
        drop(String): boolean

Along with the Hadoop FileSystem `Dataset` and stream implementations, a related
`DatasetRepository` implementation exists. This implementation requires an
instance of a Hadoop `FileSystem`, a root directory under which datasets will be
(or are) stored, and a metadata provider to be supplied upon instantiation. Once
complete, users can freely interact with datasets under the supplied root
directory. The supplied `MetadataProvider` is used to resolve dataset schemas,
partitioning information, and any other like data.

Along with the dataset repository, the _metadata provider_ is a service provider
interface used to interact with the service that provides dataset metadata
information. The MetadataProvider interface defines the contract metadata
services must provide to the library, and specifically, the `DatasetRepository`.

_MetadataProvider API_

    MetadataProvider

        save(String, DatasetDescriptor)
        load(String): DatasetDescriptor
        delete(String): boolean

The expectation is that MetadataProvider implementations will act as a bridge
between this library and centralized metadata repositories. An obvious example
of this (in the Hadoop ecosystem) is [HCatalog][hcat] and the Hive metastore. By
providing an implementation that makes the necessary API calls to HCatalog's
REST service, any and all datasets are immediately consumable by systems
compatible with HCatalog, the storage system represented by the
DatasetRepository implementation, and the format in which the data is written.
As it turns out, that's a pretty tall order and, in keeping with the CDK's
purpose of simplifying rather than presenting additional options, users are
encouraged to 1. use HCatalog, 2. allow this library to default to snappy
compressed Avro data files, and 3. use systems that also integrate with
HCatalog. In this way, this library acts as a forth integration point to working
with data in HDFS that is HCatalog-aware, in addition to Hive, Pig, and
MapReduce input/output formats.

At this time, an HCatalog implementation of the `MetadataProvider` interface
does not exist. It is, however, straight forward to implement and on the
roadmap.

[hcat]: http://incubator.apache.org/hcatalog/
    (Apache HCatalog)

## Logistics

### Project Format

The intention is for this library to be released as an open source project,
under the Apache Software License. The project will be hosted and managed on
Github. Community contributions are welcome and are encouraged. Those that wish
to contribute to the project _must_ complete a contributor license agreement
(CLA) and provide their changes to the project under the same license as the
rest of the project. This must be done prior to accepting any changes (e.g. a
Github pull request).

A more detailed "How to Contribute" document, as well as individual and
corporate CLAs, will be provided when with the initial publication of the
project.

All feature requests and bugs are tracked in the CDK JIRA project at
<https://issues.cloudera.org/browse/CDK>.

Users are encouraged to use the [cdh-user@cloudera.org mailing list][cdh-users]
to discuss CDK-related topics.

[cdh-users]: https://groups.google.com/a/cloudera.org/forum/?fromgroups#!forum/cdh-user

### Releases

Since this project ultimately produces a Java library, the natural way to
disseminate releases by way of [Cloudera's Maven repository][]. Direct downloads
containing the combined source and binary artifacts will also be provided.
Optionally, we may additionally publish artifacts to Maven Central.

Release frequency is left undefined at this time. That said, since this project
makes similar compatibility gurantees as CDH (see _Compatibility Statement_ ),
quarterly releases seem likely.

[Cloudera's Maven repository]: http://repository.cloudera.com

### Compatibility Statement

As a library, users must be able to reliably determine the intended
compatibility of this project. We take API stability and compatibility
seriously; any deviation from the stated guarantees is a bug. This project
follows the guidelines set forth by the [Semantic Versioning
Specification][semver] and uses the same nomenclature.

Just as with CDH (and semver), this project makes the following compatibility
guarantees:

1. The patch version is incremented if only backward-compatible bug fixes are
   introduced.
1. The minor version is incremented when backward-compaatible features are added
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

## Examples

The goal here is to demonstrate the possible APIs, not serve as a user
reference. In other words, the actual APIs may differ; no attempt will be made
to keep these examples up to date with the actual implementation.

**Writing to a new dataset**

    FileSystem fileSystem = FileSystem.get(new Configuration());
    Schema eventSchema = new Schema.Parser.parse(
      Resources.getResource("event.avsc").openStream()
    );

    DatasetRepository repo = new FileSystemDatasetRepository(
      fileSystem,
      new Path("/data"),
      new FileSystemMetadataProvider(fileSystem, new Path("/data"))
    );
    DatasetDescriptor eventDescriptor = new DatasetDescriptor.Builder()
      .schema(eventSchema)
      .get();
    Dataset events = repo.create("events", eventDescriptor);
    DatasetWriter<Event> writer = events.getWriter();

    try {
      writer.open();

      while (...) {
        /*
         * Event is an Avro specific (generated) type, a generic type, or a
         * POJO, in which case we use reflection. Here, we use a POJO.
         */
        Event e = ...

        writer.write(e);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

**Reading from existing an existing dataset**

    FileSystem fileSystem = FileSystem.get(new Configuration());

    DatasetRepository repo = new FileSystemDatasetRepository(
      fileSystem,
      new Path("/data")
      new FileSystemMetadataProvider(fileSystem, new Path("/data"))
    );
    Dataset events = repo.get("events");
    DatasetReader<GenericData.Record> reader = events.getReader();

    try {
      reader.open();

      while (reader.hasNext()) {
        // We can also use Avro Generic records.
        GenericData.Record record = reader.read();

        System.out.println(new StringBuilder("event - timestamp:")
          .append(record.get("timestamp"))
          .append(" eventId:", record.get("eventId"))
          .append(" message:", record.get("message"))
          .toString()
        );
      }
    } finally {
      reader.close();
    }

## Related Work and Systems

**HDFS APIs**

The HDFS APIs are used by the Data module. This seems safe as the `FileSystem`
API is relatively stable. This API, however, is much higher level than the
typical HDFS streams so users aren't worried about bytes.

**Avro, Protocol Buffers, Thrift**

The Data APIs standardize on Avro's in-memory representation of a schema, but
make no promise about the underlying storage format. That said, Avro satisfies
the set of criteria for optimally storing data in HDFS, and the platform team
has already done a bunch of work to make it work with all components. In this
way, the Data APIs are a layer above the file format.

**Kiji**

WibiData's Kiji schema is an HBase-only library that overlaps with the Data
module's schema tracking, but is much more prescriptive about how a user
interacts with its readers and writers all the way up the stack. That is, Kiji
entities are not simple Avro entities that are already supported by platform
components. Special input / output formats are required in order to be able to
use the Kiji-ified records. Further, Kiji only supports HBase for data, and
assumes HBase is available for storage of schemas. Since we have different
requirements and a different use case, we see this as a separate concern.

**HCatalog**

See the _Dataset Repositories and Metadata Providers_ section for information
about integration plans and compatibility with HCatalog.

## FAQ

* What license is this library made available under?

  This software is licensed under the Apache Software License 2.0. A file named
  LICENSE.txt should have been included with the software.

* Why use this library over direct interaction with HDFS?

  HDFS provides byte-oriented input and output streams. Most developers prefer
  to think in terms of higher level objects than files and directories, and
  frequently graft concepts of "tables" or "datasets" on to data stored in HDFS.
  This library aims to give you that, out of the box, in a pleasant format that
  works with the rest of the ecosystem, while still giving you efficient access
  to your data.

  Further, we've found that picking from the myriad of file formats and
  compression options is a weird place from which to start one's Hadoop journey.
  Rather than say "it depends," and lead developers through a decision tree, we
  decided to create a set of APIs that does what you ultimately want some high
  percentage of the time. For the rest of the time, well, feature requests are
  happily accepted (double karma if they come with patches)!

* What format is my data stored in?

  Snappy compressed, binary, Avro data files, according to Avro's [object
  container file spec][avro-cf]. Avro meets the criteria for sane storage and
  operation of data. Specifically, Avro:

    * has a binary representation that is compact.
    * is language agnostic.
    * supports compression of data.
    * is splittable by MapReduce jobs, including when compressed.
    * is self-describing.
    * is fast to serialize/deserialize.
    * is well-supported within the Hadoop ecosystem.
    * is open source under a permissive license.

* Why not protocol buffers?

  Protos do not define a standard for storing a set of protocol buffer encoded
  records in a file that supports compression and is also splittable by
  MapReduce.

* Why not thrift?

  See _Why not protocol buffers?_

* Why not Java serialization?

  See <https://github.com/eishay/jvm-serializers/wiki>. In other words, because
  it's terrible.

* Can I contribute code/docs/examples?

  Absolutely! You're encouraged to read the _How to Contribute_ docs included
  with the source code. In short, you must:

    * Be able to (legally) complete, sign, and return a contributor license
      agreement.
    * Follow the existing style and standards.

[avro-cf]: http://avro.apache.org/docs/current/spec.html#Object+Container+Files
    (Apache Avro - Object container files)

