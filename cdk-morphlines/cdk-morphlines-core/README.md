# CDK - Morphlines Core

Core library that higher level modules such as cdk-morphlines-avro and cdk-morphlines-tike depend on.

Morphlines is an open source framework that reduces the time and skills
necessary to build or change Search indexing applications. A morphline is a rich
configuration file that makes it easy to define an ETL transformation chain that
consumes any kind of data from any kind of data source, processes the data and
loads the results into Cloudera Search. Executing in a small embeddable Java
runtime system, morphlines can be used for Near Real Time applications as well
as Batch processing applications. Morphlines are easy to use, configurable and
extensible, efficient and powerful. They can see been as an evolution of Unix
pipelines, generalised to work with streams of generic records and to be
embedded into Hadoop components such as Search, Flume, MapReduce, Pig, Hive,
Sqoop. The system ships with a set of frequently used high level transformation
and I/O commands that can be combined in application specific ways. The plugin
system allows to add new transformations and I/O commands and integrate existing
functionality and third party systems in a straightforward manner. This enables
rapid prototyping of Hadoop ETL applications, complex stream and event
processing in real time, flexible Log File Analysis, integration of multiple
heterogeneous input schemas and file formats, as well as reuse of ETL logic
building blocks across Search applications. Cloudera ships a high performance
runtime that processes all commands of a given morphline in the same thread, and
adds no artificial overheads. For high scalability, a large number of morphline
instances can be deployed on a cluster in a large number of Flume agents and
MapReduce tasks. Cloudera Search includes a set of frequently used high level
transformation and I/O commands that reduce the time and skills necessary to
build or change Search ETL applications.