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

* Store entities
* Made up of zero or files
* Data stored as Snappy-compressed Avro by default
* Options for pluggable formats such as Parquet (future)

          Dataset

          getDescriptor(): DatasetDescriptor
          getReader(): DatasetReader<E>
          getWriter(): DatasetWriter<E>

          getPartitions(): Set<Dataset>
          getPartitions(PartitionKey key): Set<Dataset>
          getPartition(PartitionKey key): Dataset

Write to a dataset - Each writer produces a file in its data directory.

Write to a partitioned dataset - Each writer produces a file in the leaf
partitions data directory.

Given partition strategy: hash(a, 2)

    d = partitioned dataset
    w = d.getWriter()
    w.write({ a = "foo", b = "bar" })

    w will write to ./data/a=hash("foo")/*.avro

With the same configuration, but accessing a partition directly

    d = partitioned dataset
    p1 = d.getPartition(PartitionKey("foo"))

    p1's:
        data directory is ./data/a=hash("foo")/
        partition key = PartitionKey("foo")
        partition strategy = null
        is partitioned = false

    w = p1.getWriter()
    w.write({ a = "foo", b = "bar" })

    w will write to ./data/a=hash("foo")/*.avro

Given partition strategy: identity(a), identity(b)

    d = partitioned dataset
    w = d.getWriter()
    w.write({ a = "foo", b = "bar" })

    w will write to ./data/a=foo/b=bar/*.avro

With the same configuration, but accessing a partition at level 1 directly

    d = partitioned dataset
    p1 = d.getPartition(PartitionKey("foo"))

    p1's:
        data directory is ./data/a=foo/
        partition key = PartitionKey("foo")
        partition strategy = identity(b)
        is partitioned = true

    w = p1.getWriter()
    w.write({ a = "foo", b = "bar" })

    w will write to ./data/a=foo/b=bar/*.avro

#### Partitions

* Datasets are optionally partitioned
* Data in a partitioned dataset always lives in exactly one partition
* Partitions can be hierarchical
* Partitions are represented as directories in the filesystem
* Implemented as recursive datasets

#### Partition Keys

* A list of values used to identify a partition - e.g. (1, 2)
* Basic wildcard support to select matching partitions - e.g. (1, \*) (future)

#### Partition Strategies

* A list of functions that ultimately decide how data is partitioned
* Produces a partition key from an entity or list of values

    PartitionStrategy
        partitionKey(Object entity): PartitionKey
        partitionKey(List<Object> values): PartitionKey

### Dataset Repositories

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

    DatasetRepository<? extends Dataset> repo =
      new HDFSDatasetRepository(fileSystem, new Path("/data"));
    Dataset events = repo.create("events", eventSchema);
    DatasetWriter<Event> writer = events.getWriter();

    try {
      writer.open();

      while (...) {
        /*
         * Event is an Avro specific (generated) type or a POJO, in
         * which case we use reflection.
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

    DatasetRepository<? extends Dataset> repo =
      new HDFSDatasetRepository(fileSystem, new Path("/data"));
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
      if (reader != null) {
        reader.close();
      }
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

This one is trickier. At a high level view, this library appears to duplicate
much of the functionality found in HCatalog. Both store schema information about
data, abstract the underlying format, and aim to provide simplified APIs for
accessing that data. When you zoom in and examine the details, it turns out that
they solve two distinct problems.

_Terminology Mapping_

* An HCat database is a DatasetRepository
* An HCat table is a Dataset

_Metadata and Data Model_

**TODO**

## FAQ

