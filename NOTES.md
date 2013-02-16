# Primary (Generic) APIs

*Interfaces*

DatasetRepository
  A logical storage location and container for a set of related datasets. In a
  relational database, this would be a database. In HDFS, this is a root
  directory. It's up to the user to decide how to organize datasets into repos.

Dataset
  A logical set of entities. An entity, in this context, is a record. All
  datasets have a name and a schema. The schema is defined in Avro. Entities
  must all conform to the same schema, however, that schema can evolve based on
  the rules[1][2] set forth by Avro. The relational analog of a Dataset is a
  table.

  While Avro is used to define the schema, it may not be used as the underlying
  storage format of the data. This is because other constraints may apply[3],
  based on the subsystems or access patterns. The implementing class is expected
  to translate the Avro schema into whatever is appropriate. Implementations, of
  course, are free to use Avro serialization if it makes sense.

  Datasets may optionally be partitioned to facilitate piecemeal storage
  management as well as optimized access to data under certain predicates. A
  dataset is considered partitioned if it has an associated PartitionExpression
  (described later). When records are written to a partitioned dataset, they are
  automatically written to the proper partition, as you'd expect. The semantics
  of a partition are defined by the implementation; this interface makes no
  guarantee as to the performance of reading across partitions, availability of
  a partition in the face of failures, or the efficiency of partition
  elimination under one or more predicates (i.e. partition pruning in query
  engines). It is not possible to partition an existing non-partitioned dataset,
  nor can one write data into a partitioned dataset that does not land in a
  partition. It is possible to add or remove partitions from a partitioned
  dataset.

[1] http://avro.apache.org/docs/current/spec.html#Schema+Resolution
[2] The flexibility of Avro evolution / resolution rules is also a drawback. It
    may make sense for us to restrict the set of permitted changes to something
    simpler such as what protocol buffers permits (i.e. no member removal, only
    new members, all new members must have defaults and be optional).
[3] A good example is storing entities in HBase rows. Members of the entity need
    to be mapped to columns within the row and, as a result, do not follow the
    ordering rules of Avro. Further, it's much simpler to have tagged fields in
    the case of HBase so no per-row schema (version) information need be
    retained or evaluated at runtime. NB: Here there be dragons.

*Classes*

Partition
  All partitions have a name, and can produce a reader and writer that operate
  on their local data. It is expected that partitions map to a similar construct
  in the underlying storage subsystem. In HDFS, for example, the natural
  construct is a directory. All partitions in a dataset have the same schema, or
  a compatible schema, according to the evolution rules.

PartitionExpression
  A partition expression is dynamic expression that is evaluated in the context
  of a given record, and selects the appropriate partition for the data. The
  current implementation uses the Apache Commons JEXL2[4] language. This is best
  described by way of a few examples.

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
  [record.app_id]

  data in HDFS might look like this:
  /app_id=1/<files>

  With the expression:
  [record.app_id, record.timestamp % 1000]
 
  ...data will be partitioned first by the application ID, followed by the event
  timestamp modulo 1000, which will bin data into buckets numbered 0 through
  999, with a reasonably even distribution given a steady stream of events.

  It's also possible to install and invoke custom functions, or invoke Java
  methods on objects in JEXL. This is incredibly powerful, and allows for
  natural, database-style, partitioning of datasets by value, range, or hash.
