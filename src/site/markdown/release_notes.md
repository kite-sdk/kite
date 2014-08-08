# Release Notes

All past Kite releases are documented on this page. Upcoming release dates can be found in
[JIRA](https://issues.cloudera.org/browse/CDK#selectedTab=com.atlassian.jira.plugin.system.project%3Aversions-panel).

## Version 0.16.0

Release date: TBD

Version 0.16.0 contains the following notable changes:

* Writing to a non-empty dataset or view from MapReduce or Crunch will now fail unless 
the write mode is explicitly set to append or overwrite. This is a change from 
the previous behavior which was to append. See
[CDK-572](https://issues.cloudera.org/browse/CDK-572) and
[CDK-347](https://issues.cloudera.org/browse/CDK-347) for details.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10609)
is available from JIRA.

## Version 0.15.0

Release date: 15 July 2014

Version 0.15.0 contains the following notable changes:

* Kite artifacts are built against Apache Hadoop 2 and related projects, 
and are now available in Maven Central.
* Added new [introduction and concepts documentation](http://kitesdk.org/docs/current/guide).
* Added a new [Datasets](http://kitesdk.org/docs/current/apidocs/org/kitesdk/data/Datasets.html) 
convenience class for opening and working with Datasets, superseding DatasetRepositories.
* Deprecated partition related methods in [Dataset](http://kitesdk.org/docs/current/apidocs/org/kitesdk/data/Dataset.html) in favor of the views API.
* Added a CLI [copy task](https://issues.cloudera.org/browse/CDK-374) for copying 
datasets and also for dataset format conversion and data compaction.
* Added an application parent POM that makes it easy to use Kite in a Maven project. 
The [examples](https://github.com/kite-sdk/kite-examples) now use this parent POM.
* Updated to Crunch 0.10.0
* Morphlines Library
    * Added morphline command that parses an InputStream that contains protobuf data: [readProtobuf](kite-morphlines/morphlinesReferenceGuide.html#readProtobuf) (Rober Fiser via whoschek)
    * Added morphline command that extracts specific values from a protobuf object, akin to a simple form of XPath: [extractProtobufPaths](kite-morphlines/morphlinesReferenceGuide.html#extractProtobufPaths) (Rober Fiser via whoschek)
    * Added morphline command that removes all record fields for which the field name matches a blacklist but not a whitelist: [removeFields](kite-morphlines/morphlinesReferenceGuide.html#removeFields)
    * Added optional parameters `maxCharactersPerRecord` and `onMaxCharactersPerRecord` to morphline command [readCSV](kite-morphlines/morphlinesReferenceGuide.html#readCSV)
    * Upgraded kite-morphlines-maxmind module from maxmind-db-0.3.1 to bug fix release maxmind-db-0.3.3
    * Upgraded kite-morphlines-core module from metrics-0.3.1 to bug fix release metrics-0.3.2

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10603)
is available from JIRA.

## Version 0.14.1

Release date: 23 May 2014

Version 0.14.1 is a bug-fix release with the following notable changes:

* Usability improvements for the CLI, [CDK-424](https://issues.cloudera.org/browse/CDK-424)
* Fixed Dataset examples: [dataset-hbase](https://github.com/kite-sdk/kite-examples/tree/master/dataset-hbase),
[dataset-staging](https://github.com/kite-sdk/kite-examples/tree/master/dataset-staging),
[dataset-compatibility](https://github.com/kite-sdk/kite-examples/tree/master/dataset-compatibility)
* Fixed a bug in the Kite Maven Plugin, [CDK-406](https://issues.cloudera.org/browse/CDK-406)

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10518)
is available from JIRA.

## Version 0.14.0

Release date: 13 May 2014

Version 0.14.0 has the following notable additions:

* Added View support to Kite [MapReduce](http://kitesdk.org/docs/current/apidocs/org/kitesdk/data/mapreduce/package-summary.html) and [Crunch](http://kitesdk.org/docs/current/apidocs/org/kitesdk/data/crunch/CrunchDatasets.html)
* Added more [documentation on kitesdk.org](http://kitesdk.org/docs/current/overview.html)
    * [Kite command-line interface tutorial](http://kitesdk.org/docs/current/usingkiteclicreatedataset.html) and [reference](http://kitesdk.org/docs/current/kitedatasetcli.html)
* Added HBase storage option to the CLI, `--use-hbase` (experimental)
* Added a new JSON configuration format for partition strategies
    * Supports hash, identity, year, month, day, hour, minute parititoners
    * [Partition strategy documentation](http://kitesdk.org/docs/current/overview.html#Defining_a_Partitioning_Strategy)
* Added partition strategy support to the CLI
    * Create and validate partition strategies using [partition-config](kitedatasetcli.html#partition-config)
    * Create partitioned datasets with [create](kitedatasetcli.html#create)
* Added a builder and JSON configuration format for HBase column mappings
    * Supports column, counter, keyAsColumn, key, and version mappings
* Updated to parquet 1.4.1

And the following bug fixes:

* Updated CLI environment setup for CDH5.0 QuickStart VM
* Fixed compatibility with CDH5 Hive, [CDK-416](https://issues.cloudera.org/browse/CDK-416)
* Fixed schema update validation bug, [CDK-410](https://issues.cloudera.org/browse/CDK-410)
* Added reconnect support when Hive connections drop, [CDK-415](https://issues.cloudera.org/browse/CDK-415)

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10515)
is available from JIRA.

## Version 0.13.0

Release date: April 23, 2014

Version 0.13.0 has the following notable changes:

* Added datasets command-line interface
    * Build avro schemas from CSV data samples and java classes
    * Create, view, and delete Kite datasets
    * Import CSV data into a dataset
* Morphlines Library
    * Added morphline command that opens an HDFS file for read and returns a corresponding Java InputStream: [openHdfsFile](kite-morphlines/morphlinesReferenceGuide.html#openHdfsFile)
    * Added morphline command that converts an InputStream to a byte array in main memory: [readBlob](kite-morphlines/morphlinesReferenceGuide.html#readBlob)
    * Upgraded kite-morphlines-saxon module from Saxon-HE-9.5.1-4 to Saxon-HE-9.5.1-5

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10510)
is available from JIRA.

## Version 0.12.1

Release date: March 18, 2014

Version 0.12.1 is a bug-fix release with the following notable changes:

* Fixed slow job setup for crunch when using large Datasets (thanks Gabriel Reid!)
* Fixed CDK-328, Hive metastore concurrent access bug (thanks Karel Vervaeke!)
* Clarified documentation for deleting datasets
* Added more better checking to catch errors earlier
    * Catch partition strategies that rely on missing data fields
    * Catch Hive-incompatible table, column, and partition names
* Added warnings when creating FS or HBase datasets that are incompatible with Hive

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10517)
is available from JIRA.

## Version 0.12.0

Release date: March 10, 2014

Version 0.12.0 has the following notable changes:

* MapReduce support for Datasets. New input and output formats (DatasetKeyInputFormat
  and DatasetKeyOutputFormat) make it possible to use Datasets with MapReduce.
* Views API. There is an incompatible change in this release: RefineableView in the
  org.kitesdk.data package has been renamed to RefinableView (no 'e'). Clients should
  update and recompile.
* Morphlines Library
    * Added a sampling command that forwards each input record with a given probability to its child command: [sample](kite-morphlines/morphlinesReferenceGuide.html#sample)
    * Added a command that ignores all input records beyond the N-th record, akin to the Unix head command: [head](kite-morphlines/morphlinesReferenceGuide.html#head)
    * Improved morphline import performance if all commands are specified via fully qualified class names.
    * Added several performance enhancements.
    * Added an [example module](https://github.com/kite-sdk/kite-examples/tree/master/kite-examples-morphlines) that describes how to unit test Morphline config files and custom Morphline commands.
    * Improved documentation.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10509)
is available from JIRA.

## Version 0.11.0

Release date: February 6, 2014

Version 0.11.0 has the following notable changes:

* Views API. A new API for expressing a subset of a dataset using logical constraints
  such as field matching or ranges. See the documentation for [RefineableView](http://kitesdk.org/docs/0.11.0/apidocs/org/kitesdk/data/RefineableView.html)
  for details. The [HBase example](https://github.com/kite-sdk/kite-examples/tree/master/dataset-hbase)
  has been extended to use a view for doing a partial scan of the table.
* Dataset API. Removed APIs that were deprecated in 0.9.0. See the
  [API Diffs](http://kitesdk.org/docs/0.11.0/jdiff/changes.html) for all the changes.
* Upgrade to Crunch 0.9.0.
* Morphlines Library
    * Added morphline command to read from Hadoop Avro Parquet Files: [readAvroParquetFile](kite-morphlines/morphlinesReferenceGuide.html#readAvroParquetFile)
    * Added support for multi-character separators as well as a regex separators to [splitKeyValue](kite-morphlines/morphlinesReferenceGuide.html#splitKeyValue) command.
    * Added `addEmptyStrings` parameter to [readCSV](kite-morphlines/morphlinesReferenceGuide.html#readCSV) command to indicate whether or not to add zero length strings to the output field.
    * Upgraded kite-morphlines-solr-* modules from solr-4.6.0 to solr-4.6.1.
    * Upgraded kite-morphlines-json module from jackson-databind-2.2.1 to jackson-databind-2.3.1.
    * Upgraded kite-morphlines-metrics-servlets module from jetty-8.1.13.v20130916 to jetty-8.1.14.v20131031.
    * Upgraded kite-morphlines-saxon module from Saxon-HE-9.5.1-3 to Saxon-HE-9.5.1-4.
    * Fixed CDK-282 `readRCFile` command is broken (Prasanna Rajaperumal via whoschek).

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10401)
is available from JIRA.

## Version 0.10.1

Release date: January 13, 2014

Version 0.10.1 includes the following bug fixes:

* CDK-249: Correctly add new partitions to the Hive MetaStore
* CDK-260: Fixed the date-format partition function in expressions
* CDK-266: Fixed file name uniqueness
* CDK-273: Fixed spurious batch size warning in log4j integration
* Fixed NoClassDefFoundError for crunch in kite-tools module
* Added more debug logging to Morphlines
* Solr should fail fast if ZK has no solr configuration

This patch release is fully-compatible with 0.9.1, which uses the deprecated CDK packages.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10507)
is available from JIRA.

## Version 0.10.0

Release date: December 9, 2013

Version 0.10.0 has the following notable changes:

* Renamed the project from CDK to Kite.
  The main goal of Kite is to increase the accessibility of Apache Hadoop as a platform. 
  This isn't specific to Cloudera, so we updated the name to correctly represent the project as an open, community-driven set of tools.
  To make migration easier, there are no **feature changes** and [migration instructions](migrating.html) have been added for existing projects.
    * Renamed java packages `com.cloudera.cdk.*` to `org.kitesdk.*`. This change is trivial and mechanical but it does break backwards compatibility. This is a one-time event - going forward no such backwards incompatible renames are planned. This mass rename is the only change going from the `cdk-0.9.0` release to the `kite-0.10.0` release.
    * Renamed maven module names and jar files from `cdk-*` to `kite-*`.
    * Moved github repo from http://github.com/cloudera/cdk to http://github.com/kite-sdk/kite.
    * Moved documentation from http://cloudera.github.io/cdk/docs/current to http://kitesdk.org/docs/current.
    * Moved morphline reference guide from http://cloudera.github.io/cdk/docs/current/cdk-morphlines/morphlinesReferenceGuide.html to http://kitesdk.org/docs/current/kite-morphlines/morphlinesReferenceGuide.html.

## Version 0.9.0

Release date: December 5, 2013

Version 0.9.0 has the following notable changes:

* HBase support. There is a new experimental API working with random access datasets
  stored in HBase. The API exposes get/put operations, but there is no support for
  scans from an arbitrary row in this release. (The latter will be added in 0.11.0 as a
  part of the forthcoming [views API](https://issues.cloudera.org/browse/CDK-177).) For
  usage information, consult the new
  [HBase example](https://github.com/cloudera/cdk-examples/tree/master/dataset-hbase).
* Parquet. Datasets in Parquet format can now be written (and read) using Crunch.
* CSV. Datasets in CSV format can now be read using the dataset APIs. See the
  [compatibility example](https://github.com/cloudera/cdk-examples/tree/master/dataset-compatibility).
* Dataset API. Removed APIs that were deprecated in 0.8.0. See the
  [API Diffs](http://cloudera.github.io/cdk/docs/0.9.0/jdiff/changes.html) for all the
  changes.
* Morphlines Library
    * Added morphline command to read from RCFile: [readRCFile](kite-morphlines/morphlinesReferenceGuide.html#readRCFile) (Prasanna Rajaperumal via whoschek)
    * Added morphline command to convert a morphline record to an Avro record: [toAvro](kite-morphlines/morphlinesReferenceGuide.html#toAvro)
    * Added morphline command that serializes Avro records into a byte array: [writeAvroToByteArray](kite-morphlines/morphlinesReferenceGuide.html#writeAvroToByteArray)
    * Added morphline command that returns Geolocation information for a given IP address, using an efficient in-memory Maxmind database lookup:
      [geoIP](kite-morphlines/morphlinesReferenceGuide.html#geoIP)
    * Added morphline command that parses a user agent string and returns structured higher level data like user agent family, operating system, version, and device type:
      [userAgent](kite-morphlines/morphlinesReferenceGuide.html#userAgent)
    * Added option to fail the following commands if an URI is syntactically invalid:
      [extractURIComponents](kite-morphlines/morphlinesReferenceGuide.html#extractURIComponents),
      [extractURIComponent](kite-morphlines/morphlinesReferenceGuide.html#extractURIComponent),
      [extractURIQueryParameters](kite-morphlines/morphlinesReferenceGuide.html#extractURIQueryParameters)
    * Upgraded cdk-morphlines-solr-core module from solr-4.4 to solr-4.6.
    * Upgraded cdk-morphlines-saxon module from saxon-HE-9.5.1-2 to saxon-HE-9.5.1-3.
    * Fixed race condition on parallel initialization of multiple Solr morphlines within the same JVM.
    * For enhanced safety readSequenceFile command nomore reuses the identity of Hadoop Writeable objects.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10277)
is available from JIRA.

## Version 0.8.1

Release date: October 23, 2013

Version 0.8.1 has the following notable changes:

* Morphlines Library
    * Made xquery and xslt commands also compatible with woodstox-3.2.7 (not just woodstox-4.x).

## Version 0.8.0

Release date: October 7, 2013

Version 0.8.0 has the following notable changes:

* Dataset Repository URIs. Repositories can be referred to (and opened) by a URI. For
  example, repo:hdfs://namenode:8020/data specifies a Dataset Repository stored in HDFS.
  Dataset descriptors carry the repository URI.
* Dataset API. Removed APIs that were deprecated in 0.7.0. Deprecated some constructors
  in favor of builders. See [API Diffs](http://cloudera.github.io/cdk/docs/0.8.0/jdiff/changes.html)
  for all the changes.
* Upgrade to Parquet 1.2.0.
* Morphlines Library
    * Added option for commands to register health checks (not just metrics) with the MorphlineContext.
    * Added [registerJVMMetrics](kite-morphlines/morphlinesReferenceGuide.html#registerJVMMetrics) command that registers metrics that are related to the Java Virtual Machine
      with the MorphlineContext. For example, this includes metrics for garbage collection events,
      buffer pools, threads and thread deadlocks.
    * Added morphline commands to publish the metrics of all morphline commands to JMX, SLF4J and CSV files.
      The new commands are:
      [startReportingMetricsToJMX](kite-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToJMX),
      [startReportingMetricsToSLF4](kite-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToSLF4J) and
      [startReportingMetricsToCSV](kite-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToCSV).
    * Added EXPERIMENTAL `cdk-morphlines-metrics-servlets` maven module with new
      [startReportingMetricsToHTTP](kite-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToHTTP) command that
      exposes liveness status, health check status, metrics state and thread dumps via a set of HTTP URIs served by Jetty,
      using the AdminServlet.
    * Added `cdk-morphlines-hadoop-core` maven module with new
      [downloadHdfsFile](kite-morphlines/morphlinesReferenceGuide.html#downloadHdfsFile)
      command for transferring HDFS files, e.g. to help with centralized configuration file management.
    * Added option to specify boost values to
      [loadSolr](kite-morphlines/morphlinesReferenceGuide.html#loadSolr) command.
    * Added several performance enhancements.
    * Upgraded `cdk-morphlines-solr-cell` maven module from tika-1.3 to tika-1.4 to pick up some bug fixes.
    * Upgraded `cdk-morphlines-core` maven module from com.google.code.regexp-0.1.9 to 0.2.3 to pick up some bug fixes (Internally shaded version).
    * The constructor of AbstractCommand now has an additional parameter that refers to the CommandBuilder.
      The old constructor has been deprecated and will be removed in the next release.
    * The ISO8601_TIMEZONE grok pattern now allows the omission of minutes in a timezone offset.
    * Ensured morphline commands can refer to record field names containing arbitrary characters.
      Previously some commands could not refer to record field names containing the '.' dot character.
      This limitation has been removed.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10268)
is available from JIRA.

## Version 0.7.0

Release date: September 5, 2013

Version 0.7.0 has the following notable changes:

* Dataset API. Changes to make the API more consistent and better integrated with
  standard Java APIs like Iterator, Iterable, Flushable, and Closeable.
* Java 7. CDK now also works with Java 7.
* Upgrade to Avro 1.7.5.
* Morphlines Library
    * Added commands `splitKeyValue`, `extractURIComponent` and `toByteArray`
    * Added `outputFields` parameter to the `split` command to support a list of column names similar to the `readCSV` command
    * Added tika-xmp maven module as a dependency to cdk-morphline-solr-cell module
    * Added several performance enhancements
    * Upgraded cdk-morphlines-saxon module from saxon-HE-9.5.1-1 to saxon-HE-9.5.1-2

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10264)
is available from JIRA.

## Version 0.6.0

Release date: August 16, 2013

Version 0.6.0 has the following notable changes:

* Dependency management. Solr and Lucene dependencies have been upgrade to 4.4.
* Build system. The version of the Maven Javadoc plugin has been upgraded.

## Version 0.5.0

Release date: August 1, 2013

Version 0.5.0 has the following notable changes:

* Examples. All examples can be run from the user's host machine,
  as an alternative to running from within the QuickStart VM guest.
* CDK Maven Plugin. A new
  [plugin](http://cloudera.github.io/cdk/docs/0.5.0/cdk-maven-plugin/usage.html) with
  goals for manipulating datasets, and packaging, deploying,
  and running distributed applications.
* Dependency management. Hadoop components are now marked as provided to give users
  more control. See the
  [dependencies page](http://cloudera.github.io/cdk/docs/0.5.0/dependencies.html).
* Upgrade to Parquet 1.0.0 and Crunch 0.7.0.
* Morphlines Library
    * Added commands `xquery`, `xslt`, `convertHTML` for reading, extracting and transforming XML and HTML with XPath, XQuery and XSLT
    * Added `tokenizeText` command that uses the embedded Solr/Lucene Analyzer library to generate tokens from a text string, without sending data to a Solr server
    * Added `translate` command that examines each value in a given field and replaces it with the replacement value defined in a given dictionary aka lookup hash table
    * By default, disable quoting and multi-line fields feature and comment line feature for the `readCSV` morphline command.
    * Added several performance enhancements

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10256)
is available from JIRA.

## Version 0.4.1

Release date: July 11, 2013

Version 0.4.1 has the following notable changes:

* Morphlines Library
    * Expanded documentation and examples
    * Made `SolrLocator` and `ZooKeeperDownloader` collection alias aware
    * Added commands `readJson` and `extractJsonPaths` for reading, extracting, and transforming JSON files and JSON objects, in the same style as Avro support
    * Added commands `split`, `findReplace`, `extractURIComponents`, `extractURIQueryParameters`, `decodeBase64`
    * Fixed `extractAvroPaths` exception with flatten = true if path represents a non-leaf node of type Record
    * Added several performance enhancements

## Version 0.4.0

Release date: June 22, 2013

Version 0.4.0 has the following notable changes:

* Morphlines Library. A morphline is a rich configuration file that makes it easy to
define an ETL transformation chain embedded in Hadoop components such as Search, Flume,
MapReduce, Pig, Hive, Sqoop.
* An Oozie example. A new example of using Oozie to run a transformation job
periodically.
* QuickStart VM update. The examples now use version 4.3.0 of the
[Cloudera QuickStart VM](https://ccp.cloudera.com/display/SUPPORT/Cloudera+QuickStart+VM).
* Java package changes. The `com.cloudera.data` package and subpackages
have been renamed to `com.cloudera.cdk.data`, and `com.cloudera.cdk.flume` has become
`com.cloudera.cdk.data.flume`.
* Finer-grained Maven modules. The module organization and naming has changed,
including making all group IDs `com.cloudera.cdk`. Please see the new [dependencies
page](http://cloudera.github.io/cdk/docs/0.4.0/dependencies.html) for details.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10253)
is available from JIRA.

## Version 0.3.0

Release date: June 6, 2013

Version 0.3.0 has the following notable changes:

* Logging to a dataset. Using log4j as the logging API and Flume as the log transport,
it is now possible to log application events to datasets.
* Crunch support. Datasets can be exposed as Crunch sources and targets.
* Date partitioning. New partitioning functions for partitioning datasets by
year/month/day/hour/minute.
* New examples. The new [examples repository](https://github.com/cloudera/cdk-examples)
has examples for all these new features. The examples use the
[Cloudera QuickStart VM](https://ccp.cloudera.com/display/SUPPORT/Cloudera+QuickStart+VM),
version 4.2.0, to make running the examples as simple as possible.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10244)
is available from JIRA.

## Version 0.2.0

Release date: May 2, 2013

Version 0.2.0 has two major additions:

* Experimental support for reading and writing datasets in
  [Parquet format](https://github.com/Parquet/parquet-format).
* Support for storing dataset metadata in a Hive/HCatalog metastore.

The examples module has example code for both of these usages.

The full [change log](https://issues.cloudera.org/secure/ReleaseNote.jspa?projectId=10143&amp;version=10239)
is available from JIRA.

## Version 0.1.0

Release date: April 5, 2013

Version 0.1.0 is the first release of the CDK Data module. This is considered a
_beta_ release. As a sub-1.0.0 release, this version is _not_ subject to the
normal API compatibility guarantees. See the _Compatibility Statement_ for
information about API compatibility guarantees.
