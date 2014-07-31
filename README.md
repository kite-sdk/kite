# Kite [![Build Status](https://travis-ci.org/kite-sdk/kite.png?branch=master)](http://travis-ci.org/kite-sdk/kite)

Kite is a set of libraries, tools, examples,
and documentation focused on making it easier to build systems on top of the
Hadoop ecosystem.

The goals of Kite are:

* Codify expert patterns and practices for building data-oriented systems and
applications.
* Let developers focus on business logic, not plumbing or infrastructure.
* Provide smart defaults for platform choices.
* Support piecemeal adoption via loosely-coupled modules.

Eric Sammer recorded a [webinar](http://www.cloudera.com/content/cloudera/en/resources/library/recordedwebinar/cloudera-development-kit-cdk-hadoop-application-development-made-easier.html)
in which he talks about the goals of the project, which was then called CDK (the Cloudera Development Kit).

This project is organized into modules. Modules may be independent or have
dependencies on other modules within Kite. When possible, dependencies on
external projects are minimized.

## Modules

The following modules currently exist.

### Kite Data

The data module provides logical abstractions on top of storage subsystems (e.g.
HDFS) that let users think and operate in terms of records, datasets, and
dataset repositories. If you're looking to read or write records directly
to/from a storage system, the data module is for you.

### Kite Maven Plugin

The Kite Maven Plugin provides Maven goals for packaging, deploying, and running
distributed applications.

### Kite Morphlines

The Morphlines module reduces the time and skills necessary to build and change Hadoop
ETL stream processing applications that extract, transform and load data into Apache
Solr, Enterprise Data Warehouses, HDFS, HBase or Analytic Online Dashboards.

### Kite Tools

The tools module provides command-line tools and APIs for performing common tasks with
the Kite.

## Examples

Example code demonstrating how to use Kite can be found in the separate GitHub
repository at [https://github.com/kite-sdk/kite-examples](https://github.com/kite-sdk/kite-examples)

## License

Kite is provided under the Apache Software License 2.0. See the file
`LICENSE.txt` for more information.

## Building

To build using the default CDH dependencies use

```
mvn install
```

For Hadoop 2:

```
mvn install -Dhadoop.profile=2
```

For Hadoop 1:

```
mvn install -Dhadoop.profile=1
```

By default Java 7 is used. If you want to use Java 6, then add `-DjavaVersion=1.6`, e.g.

```
mvn install -DjavaVersion=1.6
```
