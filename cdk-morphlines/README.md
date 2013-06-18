# CDK - Morphlines Parent

Cloudera Morphlines is an open source framework that reduces the time and skills necessary to build or
change Hadoop ETL applications such as Search indexing. A morphline is a rich configuration file that makes it easy
to define an ETL transformation chain that consumes any kind of data from any kind of data source,
processes the data and loads the results into a Hadoop component such as Cloudera Search. Executing in a small embeddable
Java runtime system, morphlines can be used for Near Real Time applications as well as Batch
processing applications.

Morphlines are easy to use, configurable and extensible, efficient and powerful. They can see been
as an evolution of Unix pipelines, generalised to work with streams of generic records and to be
embedded into Hadoop components such as Search, Flume, MapReduce, Pig, Hive, Sqoop, Storm.

The system ships with a set of frequently used high level transformation and I/O commands that can
be combined in application specific ways. The plugin system allows to add new transformations and
I/O commands and integrate existing functionality and third party systems in a straightforward
manner.

This enables rapid prototyping of Hadoop ETL applications, complex stream and event
processing in real time, flexible Log File Analysis, integration of multiple heterogeneous input
schemas and file formats, as well as reuse of ETL logic building blocks across applications.

Cloudera ships a high performance runtime that compiles a morphline on the fly and processes all
commands of a given morphline in the same thread, and adds no artificial overheads.
For high scalability, a large number of morphline instances can be deployed on a cluster in a
large number of Flume agents and MapReduce tasks.

Currently there are three components that execute morphlines:

* MapReduceIndexerTool
* Flume Morphline Solr Sink
* Flume MorphlineInterceptor

Morphlines manipulate continuous or arbitrarily large streams of records. The data model can be
described as follows: A record is a set of named fields where each field has an ordered list of one
or more values. A value can be any Java Object. That is, a record is essentially a hash table where
each hash table entry contains a String key and a list of Java Objects as values. (The
implementation uses Guava’s ArrayListMultimap, which is a ListMultimap). Note that a field can have
multiple values and any two records need not use common field names. This flexible data model
corresponds exactly to the characteristics of the Solr/Lucene data model, meaning a record can be
seen as a SolrInputDocument. A field with zero values is removed from the record - fields with zero
values effectively do not exist.

Not only structured data, but also arbitrary binary data can be passed into and processed by a
morphline. By convention, a record can contain an optional field named _attachment_body, which can
be a Java java.io.InputStream or Java byte[]. Optionally, such binary input data can be
characterized in more detail by setting the fields named _attachment_mimetype (e.g. application/pdf)
and _attachment_charset (e.g. UTF-8) and _attachment_name (e.g. cars.pdf), which assists in
detecting and parsing the data type.

This generic data model is useful to support a wide range of applications.

A command transforms a record into zero or more records. Commands can access all record fields. For
example, commands can parse fields, set fields, remove fields, rename fields, find and replace
values, split a field into multiple fields, split a field into multiple values, or drop records.
Often, regular expression based pattern matching is used as part of the process of acting on fields.
The output records of a command are passed to the next command in the chain. A command has a Boolean
return code, indicating success or failure.

For example, consider the case of a multi-line input record: A command could take this multi-line
input record and divide the single record into multiple output records, one for each line. This
output could then later be further divided using regular expression commands, splitting each single
line record out into multiple fields in application specific ways.

A command can extract, clean, transform, join, integrate, enrich and decorate records in many other
ways. For example, a command can join records with external data sources such as relational
databases, key-value stores, local files or IP Geo lookup tables. It can also perform DNS
resolution, expand shortened URLs, fetch linked metadata from social networks, perform sentiment
analysis and annotate the record accordingly, continuously maintain statistics for analytics over
sliding windows, compute exact or approximate distinct values and quantiles, etc.

A command can also consume records and pass them to external systems. For example, a command can
load records into Solr or write them to a MapReduce Reducer or pass them into an online
dashboard.

A command can contain nested commands. Thus, a morphline is a tree of commands, akin to a push-based
data flow engine or operator tree in DBMS query execution engines.

A morphline has no notion of persistence or durability or distributed computing or node failover. It
is basically just a chain of in-memory transformations in the current thread. There is no need for a
morphline to manage multiple processes or nodes or threads because this is already covered by host
systems such as MapReduce, Flume, Storm, etc. However, a morphline does support passing
notifications on the control plane to command subtrees. Such notifications include
BEGIN_TRANSACTION, COMMIT_TRANSACTION, ROLLBACK_TRANSACTION, SHUTDOWN.

The morphline configuration file is implemented using the HOCON format (Human-Optimized Config
Object Notation). HOCON is basically JSON slightly adjusted for the configuration file use case.
HOCON syntax is defined at [HOCON github page](http://github.com/typesafehub/config/blob/master/HOCON.md)
and also used by [Akka](http://www.akka.io) and [Play](http://www.playframework.org/).

The CDK includes several maven modules that contain morphline commands for integration with
Apache Solr including SolrCloud, flexible log file analysis, single-line records, multi-line
records, CSV files, regular expression based pattern matching and extraction, operations on record
fields for assignment and comparison, operations on record fields with list and set semantics,
if-then-else conditionals, string and timestamp conversions, scripting support for dynamic java
code, a small rules engine, logging, metrics and counters, integration with Avro, integration with
Apache SolrCell and all Apache Tika parsers, integration with Apache Hadoop Sequence Files,
auto-detection of MIME types from binary data using Apache Tika, and decompression and unpacking of
arbitrarily nested container file formats, among others. These are introduced below.

## Maven Modules

The following maven modules currently exist:

### cdk-morphlines-core

This module contains the Cloudera morphline compiler, runtime and standard library of commands that higher
level modules such as `cdk-morphlines-avro` and `cdk-morphlines-tika` depend on.

This includes commands for flexible log file analysis, single-line records, multi-line records,
CSV files, regular expression based pattern matching and extraction, operations on fields for
assignment and comparison, operations on fields with list and set semantics, if-then-else
conditionals, string and timestamp conversions, scripting support for dynamic java code,
a small rules engine, logging, metrics & counters, etc.

### cdk-morphlines-avro

This module contains Cloudera morphline commands for reading, extracting and transforming Avro files and Avro objects.

### cdk-morphlines-tika-core

This module contains Cloudera morphline commands for auto-detecting MIME types from binary data. Depends on Apache Tika Core.

### cdk-morphlines-tika-decompress

This module contains Cloudera morphline commands for decompressing and unpacking files. Depends on Apache Tika Core and Commons Compress.

### cdk-morphlines-solr-core

This module contains morphline commands for Solr that higher level modules such as `cdk-morphlines-solr-cell` and `search-mr` and `search-flume` depend on for indexing.

### cdk-morphlines-solr-cell

This module contains morphline commands for using SolrCell with Tika parsers. This includes support for HTML,
XML, PDF, Word, Excel, Images, Audio, Video, etc.

## Documentation

* [Search Landing Page](http://tiny.cloudera.com/search-v1-docs-landing)

## Building

This step builds the software from source.

<pre>
git clone git@github.com:cloudera/cdk.git
cd cdk
#git checkout master
mvn clean package
</pre>

## Integrating with Eclipse

* This section describes how to integrate the codeline with Eclipse.
* Build the software as described above. Then create Eclipse projects like this:
<pre>
cd cdk
mvn test -DskipTests eclipse:eclipse
</pre>
* `mvn test -DskipTests eclipse:eclipse` creates several Eclipse projects, one for each maven submodule.
It will also download and attach the jars of all transitive dependencies and their source code to the eclipse
projects, so you can readily browse around the source of the entire call stack.
* Then in eclipse do Menu `File/Import/Maven/Existing Maven Project/` on the root parent
directory `~/cdk` and select all submodules, then "Next" and "Finish".
* You will see some maven project errors that keep eclipse from building the workspace because
the eclipse maven plugin has some weird quirks and limitations. To work around this, next, disable
the maven "Nature" by clicking on the project in the browser, right clicking on Menu
`Maven/Disable Maven Nature`. This way you get all the niceties of the maven dependency management
without the hassle of the (current) maven eclipse plugin, everything compiles fine from within
Eclipse, and junit works and passes from within Eclipse as well.
* When a pom changes simply rerun `mvn test -DskipTests eclipse:eclipse` and
then run Menu `Eclipse/Refresh Project`. No need to disable the Maven "Nature" again and again.
* To run junit tests from within eclipse click on the project (e.g. `cdk-morphlines-core`)
in the eclipse project explorer, right click, `Run As/JUnit Test`.