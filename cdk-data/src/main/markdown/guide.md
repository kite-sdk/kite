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
APIs for its state use case. In other words, many applications will still use
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

### Entities

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
   entity. These objects are easy to create and manipulate, especially in code
   that has no knowledge of specific object types (such as libraries).
   Serialization of generic records is fast, but requires use of the Avro APIs.
   This is recommended for most users, in most cases.

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
[avro-cg]: http://avro.apache.org/docs/current/gettingstartedjava.html#Serializing+and+deserializing+with+code+generation "Avro - Serializing and deserializing with code generation"

### Datasets

### Dataset Repositories

### Dataset Readers and Writers

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
