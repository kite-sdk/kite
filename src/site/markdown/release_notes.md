# Release Notes

All past CDK releases are documented on this page. Upcoming release dates can be found in
[JIRA](https://issues.cloudera.org/browse/CDK#selectedTab=com.atlassian.jira.plugin.system.project%3Aversions-panel).

## Version TBD

Release date: TBD

Version TBD has the following notable changes:

* Morphlines Library
    * Made xquery and xslt commands also compatible with woodstox-3.2.7, not just woodstox-4.x.

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
    * Added [registerJVMMetrics](cdk-morphlines/morphlinesReferenceGuide.html#registerJVMMetrics) command that registers metrics that are related to the Java Virtual Machine 
      with the MorphlineContext. For example, this includes metrics for garbage collection events, 
      buffer pools, threads and thread deadlocks.
    * Added morphline commands to publish the metrics of all morphline commands to JMX, SLF4J and CSV files. 
      The new commands are: 
      [startReportingMetricsToJMX](cdk-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToJMX), 
      [startReportingMetricsToSLF4](cdk-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToSLF4J) and 
      [startReportingMetricsToCSV](cdk-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToCSV).
    * Added EXPERIMENTAL `cdk-morphlines-metrics-servlets` maven module with new 
      [startReportingMetricsToHTTP](cdk-morphlines/morphlinesReferenceGuide.html#startReportingMetricsToHTTP) command that 
      exposes liveness status, health check status, metrics state and thread dumps via a set of HTTP URIs served by Jetty, 
      using the AdminServlet.
    * Added `cdk-morphlines-hadoop-core` maven module with new 
      [downloadHdfsFile](cdk-morphlines/morphlinesReferenceGuide.html#downloadHdfsFile) 
      command for transferring HDFS files, e.g. to help with centralized configuration file management.
    * Added option to specify boost values to 
      [loadSolr](cdk-morphlines/morphlinesReferenceGuide.html#loadSolr) command.
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