# Migrating to Kite

This guide details the changes necessary to migrate existing projects to Kite.

## From CDK-0.9.0

As of version 0.10.0, CDK has been renamed to Kite. The main goal of Kite is to
increase the accessibility of Apache Hadoop as a platform. This isn't specific
to Cloudera, so we updated the name to correctly represent the project as an
open, community-driven set of tools.

Version 0.10.0 is purely a rename. There are **no feature changes**, so after
migration from 0.9.0 to 0.10.0, datasets and morphlines will produce exactly
the same results. Migration to is done primarily by updating project
dependencies and imports, with some additional configuration changes.

### Dependencies

Kite artifacts are now prefixed with `kite-` instead of `cdk-`, and the group
id is now `org.kitesdk` instead of `com.cloudera.cdk`. All references must be
updated.

For projects using maven, running these commands in a project's root folder
will find and update POM entries:

```
# Update group ids from com.cloudera.cdk to org.kitesdk
find . -name pom.xml -exec sed -i '' -e 's/com\.cloudera\.cdk/org.kitesdk/g' '{}' \;
# Update artifact names from cdk-* to kite-*
find . -name pom.xml -exec sed -i '' 's/cdk-/kite-/g' '{}' \;
```

In addition, the version in POM files must be updated to `0.10.0`. This command
will find the potential references:

```
find . -name pom.xml -exec grep 0.9.0 '{}' \;
```

Other build configurations should be updated similarly. The **required
changes** are:

* `com.cloudera.cdk` group id changes to `org.kitesdk`
* `cdk-...` artifact names change to `kite-...`
* `0.9.0` artifact versions change to `0.10.0`

### Imports

Next, references to CDK / Kite packages must be updated. Only the package names
have changed, so only import statements and similar package references must be
changed. This command finds and updates import statements, avro schema
namespaces, and log4j configurations:

```
find . \( -name *.avsc -o -name *.avdl -o -name *.java -o -name log4j.properties \) \
      -exec sed -i '' -e 's/com\.cloudera\.cdk/org.kitesdk/g' '{}' \;
```

Other references to CDK should be updated also. For example, using a different
slf4j binding would require updating the logging configuration for Kite / CDK
classes in that logger's configuration instead of `log4j.properties`.

**Required changes**:

* `com.cloudera.cdk...` package names change to `org.kitesdk...`

### Dataset configuration changes

Some internal dataset properties have been changed from using the prefix `cdk.`
to `kite.`. Most updates are backwards-compatible, except for partitioned
datasets that are written by flume. Flume agent configurations must be updated
when migrating to the `kite-data-flume` `Log4jAppender`.

This command updates `flume.properties` files. Flume agent configurations must
also be updated, and the agents restarted.

```
# Update Flume configuration files - note that Flume agents will need to be updated too
find . -name flume.properties -exec sed -i '' 's/cdk\.partition/kite.partition/g' '{}' \;
```

This command renames the partition properties that are set by the
`Log4jAppender`. The two components, clients logging events with the appender
and flume agents, must use the same property naming scheme and should be
updated and restarted at the same time.

### Morphlines configuration changes

Morphlines configuration files must be updated to import morphlines commands
from `org.kitesdk`. The following command finds and updates `.conf` files:

```
# Update Morphlines config file imports from com.cloudera.cdk to org.kitesdk
find . -name *.conf -exec sed -i '' 's/com\.cloudera\.cdk/org.kitesdk/g' '{}' \;
find . -name *.conf -exec sed -i '' 's/com\.cloudera\.\*\*/org.kitesdk.**/g' '{}' \;
```

The above change fixes errors that look like like this:

```
org.kitesdk.morphline.api.MorphlineCompilationException: No command builder registered for name: readLine 
```
