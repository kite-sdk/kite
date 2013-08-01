# Release Notes

All past CDK releases are documented on this page. Upcoming release dates can be found in
[JIRA](https://issues.cloudera.org/browse/CDK#selectedTab=com.atlassian.jira.plugin.system.project%3Aversions-panel).

## Version 0.5.0

Release date: Aug 1, 2013

Version 0.5.0 has the following notable changes:

* Examples. All examples can be run from the user's host machine,
  as an alternative to running from within the QuickStart VM guest.
* CDK Maven Plugin. A new
  [plugin](http://cloudera.github.io/cdk/docs/0.5.0/cdk-maven-plugin/index.html) with
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