# Primary (Generic) APIs

## Interfaces

*DatasetRepository*

A logical storage location and container for a set of related datasets. In a
relational database, this would be a database. In HDFS, this is a root
directory. It's up to the user to decide how to organize datasets into repos.

*Dataset*

A logical set of entities. An entity, in this context, is a record. All
datasets have a name and a schema. The schema is defined in Avro. Entities must
all conform to the same schema, however, that schema can evolve based on the
rules[1][2] set forth by Avro. The relational analog of a Dataset is a table.

While Avro is used to define the schema, it may not be used as the underlying
storage format of the data. This is because other constraints may apply[3] ,
based on the subsystems or access patterns. The implementing class is expected
to translate the Avro schema into whatever is appropriate. Implementations, of
course, are free to use Avro serialization if it makes sense.

Datasets may optionally be partitioned to facilitate piecemeal storage
management as well as optimized access to data under certain predicates. A
dataset is considered partitioned if it has an associated PartitionExpression
(described later). When records are written to a partitioned dataset, they are
automatically written to the proper partition, as you'd expect. The semantics of
a partition are defined by the implementation; this interface makes no guarantee
as to the performance of reading across partitions, availability of a partition
in the face of failures, or the efficiency of partition elimination under one or
more predicates (i.e. partition pruning in query engines). It is not possible to
partition an existing non-partitioned dataset, nor can one write data into a
partitioned dataset that does not land in a partition. It is possible to add or
remove partitions from a partitioned dataset.

[1]  <http://avro.apache.org/docs/current/spec.html#Schema+Resolution>

[2]  The flexibility of Avro evolution / resolution rules is also a drawback. It
 may make sense for us to restrict the set of permitted changes to something
 simpler such as what protocol buffers permits (i.e. no member removal, only new
 members, all new members must have defaults and be optional).

[3]  A good example is storing entities in HBase rows. Members of the entity
 need to be mapped to columns within the row and, as a result, do not follow the
 ordering rules of Avro. Further, it's much simpler to have tagged fields in the
 case of HBase so no per-row schema (version) information need be retained or
 evaluated at runtime. NB: Here there be dragons.

## Classes

*Partition*

All partitions have a name, and can produce a reader and writer that operate on
their local data. It is expected that partitions map to a similar construct in
the underlying storage subsystem. In HDFS, for example, the natural construct is
a directory. All partitions in a dataset have the same schema, or a compatible
schema, according to the evolution rules.

*PartitionExpression*

A partition expression is dynamic expression that is evaluated in the context
of a given record, and selects the appropriate partition for the data. The
current implementation uses the Apache Commons JEXL2[4] language, although that
may not be the final implementation. This is best described by way of a few
examples.

[4] <http://commons.apache.org/jexl/>

Given the dataset "events" with the following schema:

    {
      "name": "event",
      "type": "record",
      "fields": [
        { "name": "app_id", "type": "int" }      // app generating the event
        { "name": "timestamp", "type": "long" }  // epoch timestamp of the event
        { "name": "user_id", "type": "long" },   // associated user
        // other fields...
      ]
    }

and the partition expression:

    [identity("app_id", 100)]

data in HDFS might look like this:

    /events/app_id=1/<files>

The 100 is an estimate of the number of different apps in the system, which is
used as a hint for the number of writer threads when writing partitions.

With the expression:

    [identity("app_id", 100), hash("timestamp", 1000)]
 
data will be partitioned first by the application ID, followed by the event
timestamp modulo 1000, which will bin data into buckets numbered 0 through 999,
with a reasonably even distribution given a steady stream of events.

    /events/app_id=1/timestamp=0/<files>
    /events/app_id=1/timestamp=1/<files>
    ...
    /events/app_id=1/timestamp=999/<files>
    /events/app_id=2/timestamp=0/<files>
    ...

The *partition key* is the list of partition values, i.e. (1, 0) for the first
event, (1, 1) for the second, etc. Partition keys are abstract, and are mapped
into *partition paths* on HDFS: /app_id=1/timestamp=0, /app_id=1/timestamp=1
etc.

It's also possible to install and invoke custom functions, or invoke Java
methods on objects in JEXL. This is incredibly powerful, and allows for
natural, database-style, partitioning of datasets by value, range, or hash.

# Open Questions

*Configuration*

It's still unclear how best to configure attributes on repos and datasets. To a
lesser extent, readers and writers take their cues (i.e. inherit) from their
dataset, since the latter acts as a factory for the former instances. Currently,
we use properties on the Avro Schema instance as this is carried throughout the
lifecycle of the dataset, and automatically serialized along with it, but there
may be a nicer way to do this.

*Concurrency Model*

Instances of DatasetRepository, Dataset, and Partition should probably be thread
safe. Readers and writers, on the other hand, probably shouldn't be. Discuss.
Either way, we need to be explicit.

*Schema Storage*

The schema with which a dataset is created must always be stored somewhere by
the implementation. Should we have an SPI interface that implementations use to
store and retrieve schemas, rather than self-managing, as they're expected to do
today? This might be a nice extension point for partners or users with an
existing way of handling this kind of data.

*Schema Evolution*

We _must_ allow schema evolution of datasets. The question is what kind of
changes we permit, and how schema changes are applied.

*Generic Methods vs. Classes*

In a few places, I've used generic methods rather than classes/interfaces. This
is because the best API is still unclear to me. Further, since we're working
with strongly typed data in a strongly typed language, our use of generics is
going to be important. Here's a brief tour of the issue.

DatasetRepository takes a type param of DS extends Dataset so concrete
implementations can specify concrete instances of Dataset with implementation-
specific methods. Dataset is a factory for readers and writers, both of which
must produce or accept types that depend on the supplied Schema. Today, this
winds up looking like the following.

    HDFSDatasetRepository repo = new HDFSDatasetRepository(
      fileSystem, new Path(...)
    );

    HDFSDataset data = repo.create("events", eventSchema);

    HDFSDatasetWriter<Event> writer = data.getWriter();

    writer.write(new Event(...));

Note hat HDFSDataset doesn't take a type param. Instead, getWriter() is defined
as a template method (<E> DatasetWriter<E> getWriter()). In this specific
example, it's obvious that the user knows the proper type parameter and could
easily supply it in the definition. However, in the case of an existing dataset,
this is less clear.

    // If HDFSDataset has a type, it could only be <?>.
    HDFSDataset data = repo.get("events");

Once we have a handle to HDFSDataset, we could provide a method that tells us
what type should be expected. Of course, this doesn't help later definitions.

    // I don't know what type to use and reflection can't help due to erasure.
    HDFSDatasetWriter<?> writer = data.getWriter();

It seems like the only thing to do is be JDBC-ish in our treatment. That is,
if the developer knows the table (dataset), they must have some idea how to
access and manipulate it. Of course, that's not 100% true since JDBC supports
introspective operations. The user does have the Schema, thought, so they could
do something similar. Maybe we should do it for them?

# Partition Refactoring

Turns out that both datasets and partitions:

* have a name
* have a schema
* produce readers and writers
* may be partitioned

It's only the implementations that potentially treat a top level partition (i.e.
a dataset) differently. That said, most developers segfault on overly generic
recursive structures. Datasets having datasets is confusing. Equally, a
partition being a top level construct is weird. Really, what we want is a top
level object called a dataset.

Repo
  create(String, Schema): Dataset
  load(String): Dataset

Dataset
  getName(): String
  getSchema(): Schema

  getReader(): DatasetReader<E>
  getWriter(): DatasetWriter<E>

  isPartitioned(): boolean
  getPartitionStrategy(): PartitionStrategy
  getPartitions(): List<Dataset>

PartitionStrategy
  getExpression(): PartitionExpression
  getAllowAddition(): boolean // isFixed?

PartitionExpression
  apply(String): Object

Non-Partitioned:

    /txs
      /schema.avsc
      /data
        /*.avro

Date:

    /txs
      /schema.avsc
      /data
        /dt=20130101
          /*.avro
        /dt=20130102
          /*.avro
        /dt=20130103
          /*.avro

Date -> summary.hashCode % 53

    /txs
      /data
        /dt=20130101
          /hash.summary=0
            /*.avro
          /hash.summary=1
            /*.avro
        /dt=20130102
          /hash.summary=0
            /*.avro
          /hash.summary=1
            /*.avro

HDFS Implementation Details

HDFSDataset implements Dataset
  name: String
  schema: Schema
  partitionStrategy: PartitionStrategy

  directory: Path
  dataDirectory: Path
  metadataProvier: MetadataProvider

  Note: directory == dataDirectory for partitions

  def dropPartition(String name): boolean = {
    val p = getPartition(name)
    fileSystem.delete(p.getDataDirectory(), true)
  }

  def addPartition(String name): Dataset = {
    if (pStrat.getAllowAddition()) {
      val p = new HDFSDataset(fileSystem, schema)
      p.directory = new Path(directory, name);
      p.dataDirectory = p.directory;
      fileSystem.mkdir(p.dataDirectory)
      p
    } else {
      throw new UnsupportedOperationException()
    }
  }

A fully qualified name is:

    fq-name := repo dataset partition* data-fragment*

    data-fragment := id
    partition := id '=' value
    dataset := name-component
    repo := name-component

    name-component := ( name-component '.' ) | id

    id := [a-zA-Z0-9]+
    value := .*
