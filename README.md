# Cloudera Development Kit

The Cloudera Development Kit, or CDK for short, is a set of libraries, tools,
and documentation focused on making it easier to build systems on top of the
Hadoop ecosystem.

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

## Examples

Example code demonstrating how to use the CDK can be found in the separate GitHub
repository at [https://github.com/cloudera/cdk-examples](https://github.com/cloudera/cdk-examples)

## License

The CDK is provided under the Apache Software License 2.0. See the file
`LICENSE.txt` for more information.

