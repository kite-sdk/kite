# Cloudera Development Kit

<p align="center">
  <img src="images/cdk-big.jpg" alt="Cloudera Development Kit" />
</p>

---

## Hadoop App Development Made Easier

The Cloudera Development Kit (Apache License, Version 2.0), or CDK for short,
is a set of libraries, tools, examples, and documentation focused on making it easier
to build systems on top of the Hadoop ecosystem.

* __Codifies expert patterns and practices__ for building data-oriented systems and
applications
* __Lets developers focus on business logic__, not plumbing or infrastructure
* __Provides smart defaults__ for platform choices
* __Supports gradual adoption__ via loosely-coupled modules

## Modules

* [__CDK Data__](cdk-data/index.html) The data module provides logical abstractions on
top of storage subsystems (e.g. HDFS) that let users think and operate in terms of
records, datasets, and dataset repositories. If you're looking to read or write records
directly to/from a storage system, the data module is for you.
* [__CDK Maven Plugin__](cdk-maven-plugin/index.html) The CDK Maven Plugin provides Maven
goals for packaging, deploying, and running distributed applications.
* [__CDK Morphlines__](cdk-morphlines/index.html) The Morphlines module reduces the time
and skills necessary to build and change Hadoop
ETL stream processing applications that extract, transform and load data into Apache
Solr, Enterprise Data Warehouses, HDFS, HBase or Analytic Online Dashboards.
* [__CDK Tools__](cdk-tools/index.html) The tools module provides command-line tools and
APIs for performing common tasks with the CDK.

There is also __example code__ demonstrating how to use the CDK in a separate GitHub
repository at [https://github.com/cloudera/cdk-examples](https://github.com/cloudera/cdk-examples)