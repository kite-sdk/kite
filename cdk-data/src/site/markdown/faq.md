# Frequently Asked Questions

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

* Why not store data as protocol buffers?

  Protos do not define a standard for storing a set of protocol buffer encoded
  records in a file that supports compression and is also splittable by
  MapReduce.

* Why not store data as thrift?

  See _Why not protocol buffers?_

* Why not store data as Java serialization?

  See <https://github.com/eishay/jvm-serializers/wiki>. In other words, because
  it's terrible.

* Can I contribute code/docs/examples?

  Absolutely! You're encouraged to read the _How to Contribute_ docs included
  with the source code. In short, you must:

  * Be able to (legally) complete, sign, and return a contributor license
    agreement.
  * Follow the existing style and standards.

[avro-cf]: http://avro.apache.org/docs/current/spec.html#Object+Container+Files "Apache Avro - Object container files"
