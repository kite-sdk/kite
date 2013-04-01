# Cloudera Development Kit - Tools Module

The Tools Module is a collection of command-line tools and APIs for performing common
tasks with CDK.

## Example - Convert Combined Log Format files to a CDK dataset

From the tools module, build with

```bash
mvn install
```

Then run with

```bash
cp src/test/resources/access_log.txt /tmp/input
mvn exec:java -Dexec.mainClass="com.cloudera.cdk.tools.CombinedLogFormatConverter" \
-Dexec.args="file:///tmp/input file:///tmp/output logs"
```

Look at the output (Combined Log format converted to Avro files):

```bash
java -jar /path/to/avro-tools-*.jar tojson /tmp/output/logs/*.avro | head
```

***

[CDK Data Module](../cdk-data/README.md)