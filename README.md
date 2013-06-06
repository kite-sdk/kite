# Cloudera Development Kit

The Cloudera Development Kit, or CDK for short, is a set of libraries, tools, examples,
and documentation focused on making it easier to build systems on top of the
Hadoop ecosystem.

The goals of the CDK are:

* Codify expert patterns and practices for building data-oriented systems and
applications.
* Let developers focus on business logic, not plumbing or infrastructure.
* Provide smart defaults for platform choices.
* Support piecemeal adoption via loosely-coupled modules.

Eric Sammer recorded a [webinar](http://www.cloudera.com/content/cloudera/en/resources/library/recordedwebinar/cloudera-development-kit-cdk-hadoop-application-development-made-easier.html)
in which he talks about the goals of the CDK.

This project is organized into modules. Modules may be independent or have
dependencies on other modules within the CDK. When possible, dependencies on
external projects are minimized.

## Modules

The following modules currently exist.

### CDK Data

The data module provides logical abstractions on top of storage subsystems (e.g.
HDFS) that let users think and operate in terms of records, datasets, and
dataset repositories. If you're looking to read or write records directly
to/from a storage system, the data module is for you.

### CDK Tools

The tools module provides command-line tools and APIs for performing common tasks with
the CDK.

### CDK Flume Modules

The `cdk-flume-avro-event-serializer` module is a Flume extension that allows the HDFS
sink to write Avro records.

The `cdk-flume-log4jappender` module is a Log4j appender for writing Avro records to
Flume.

The [CDK logging example](https://github.com/cloudera/cdk-examples/tree/master/logging)
has a usage example for these modules.

## Examples

Example code demonstrating how to use the CDK can be found in the separate GitHub
repository at [https://github.com/cloudera/cdk-examples](https://github.com/cloudera/cdk-examples)

## License

The CDK is provided under the Apache Software License 2.0. See the file
`LICENSE.txt` for more information.

