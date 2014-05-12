# Kite Software Development Kit

---

## Hadoop App Development Made Easier

The Kite Software Development Kit (Apache License, Version 2.0), or Kite for short,
is a set of libraries, tools, examples, and documentation focused on making it easier
to build systems on top of the Hadoop ecosystem.

* __Codifies expert patterns and practices__ for building data-oriented systems and
applications
* __Lets developers focus on business logic__, not plumbing or infrastructure
* __Provides smart defaults__ for platform choices
* __Supports gradual adoption__ via loosely-coupled modules

[Kite Documentation](kite/overview.html)

## Modules

* [__Kite Data__](kite-data/index.html) The data module provides logical abstractions on
top of storage subsystems (e.g. HDFS) that let users think and operate in terms of
records, datasets, and dataset repositories. If you're looking to read or write records
directly to/from a storage system, the data module is for you.
* [__Kite Maven Plugin__](kite-maven-plugin/index.html) The Kite Maven Plugin provides Maven
goals for packaging, deploying, and running distributed applications.
* [__Kite Morphlines__](kite-morphlines/index.html) The Morphlines module reduces the time
and skills necessary to build and change Hadoop
ETL stream processing applications that extract, transform and load data into Apache
Solr, Enterprise Data Warehouses, HDFS, HBase or Analytic Online Dashboards.
* [__Kite Tools__](kite-tools/index.html) The tools module provides command-line tools and
APIs for performing common tasks with Kite.

There is also __example code__ demonstrating how to use the Kite SDK in a separate GitHub
repository at [https://github.com/kite-sdk/kite-examples](https://github.com/kite-sdk/kite-examples)
