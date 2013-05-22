# CDK - Morphlines Core

This module contains the morphline compiler, runtime and standard library of commands that higher
level modules such as `cdk-morphlines-avro` and `cdk-morphlines-tika` depend on.

Includes commands for flexible log file analysis, single-line records, multi-line records, 
CSV files, regular expression based pattern matching and extraction, operations on fields for 
assignment and comparison, operations on fields with list and set semantics, if-then-else 
conditionals, string and timestamp conversions, scripting support for dynamic java code, 
a small rules engine, logging, metrics & counters, etc.
