# Dependency Information

To use the Kite modules in a Java project add the Cloudera repository to your Maven POM:

```xml
<repository>
  <id>cdh.repo</id>
  <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
  <name>Cloudera Repositories</name>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>
</repository>
```
Then add a dependency for each module you want to use by referring to the
information listed on the Dependency Information pages listed below.
You can also view the transitive dependencies for each module.

## Hadoop Component Dependencies

As a general rule, Kite modules mark Hadoop component dependencies as having `provided`
[scope](http://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Transitive_Dependencies),
since in many cases the dependencies are provided by the container that the code is
running in.

For example,

* Kite Data has a `provided` dependency on the core Hadoop libraries
* Kite Crunch has a `provided` dependency on Crunch and the core Hadoop libraries
* Kite HCatalog has a `provided` dependency on HCatalog

The following containers provide the dependencies listed:

* The Kite Maven Plugin goal `kite:run-tool` provides the Hadoop and HCatalog dependencies.
* The Kite Maven Plugin goal `kite:run-job` provides the Hadoop dependencies. HCatalog
should be added as a `runtime` dependency ([example](https://github.com/cloudera/kite-examples/tree/master/demo/demo-oozie)).
* The `hadoop jar` command provides the Hadoop dependencies.

However, there are some cases where you may have to provide the relevant Hadoop component
dependencies yourself:

* Crunch programs (even those running in the containers listed above) ([example](https://github.com/cloudera/kite-examples/tree/master/demo/demo-crunch))
* Standalone Java programs, not run using `kite:run-tool` or `hadoop jar` ([example](https://github.com/cloudera/kite-examples/tree/master/dataset))
* Web apps ([example](https://github.com/cloudera/kite-examples/tree/master/logging-webapp))

## Kite Data Modules

* Kite Data Core
 [Dependency Information](kite-data/kite-data-core/dependency-info.html),
 [Dependencies](kite-data/kite-data-core/dependencies.html)
* Kite Data Crunch
 [Dependency Information](kite-data/kite-data-crunch/dependency-info.html),
 [Dependencies](kite-data/kite-data-crunch/dependencies.html)
* Kite Data Flume
 [Dependency Information](kite-data/kite-data-flume/dependency-info.html),
 [Dependencies](kite-data/kite-data-flume/dependencies.html)
* Kite Data HCatalog
 [Dependency Information](kite-data/kite-data-hcatalog/dependency-info.html),
 [Dependencies](kite-data/kite-data-hcatalog/dependencies.html)

## Kite Morphlines Modules

* Kite Morphlines Core
 [Dependency Information](kite-morphlines/kite-morphlines-core/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-core/dependencies.html)
* Kite Morphlines Avro
 [Dependency Information](kite-morphlines/kite-morphlines-avro/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-avro/dependencies.html)
* Kite Morphlines JSON
 [Dependency Information](kite-morphlines/kite-morphlines-json/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-json/dependencies.html)
* Kite Morphlines Hadoop Core
 [Dependency Information](kite-morphlines/kite-morphlines-hadoop-core/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-hadoop-core/dependencies.html)
* Kite Morphlines Hadoop Parquet Avro
 [Dependency Information](kite-morphlines/kite-morphlines-hadoop-parquet-avro/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-hadoop-parquet-avro/dependencies.html)
* Kite Morphlines Hadoop RC File
 [Dependency Information](kite-morphlines/kite-morphlines-hadoop-rcfile/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-hadoop-rcfile/dependencies.html)
* Kite Morphlines Hadoop Sequence File
 [Dependency Information](kite-morphlines/kite-morphlines-hadoop-sequencefile/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-hadoop-sequencefile/dependencies.html)
* Kite Morphlines Maxmind
 [Dependency Information](kite-morphlines/kite-morphlines-maxmind/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-maxmind/dependencies.html)
* Kite Morphlines Metrics Servlets
 [Dependency Information](kite-morphlines/kite-morphlines-metrics-servlets/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-metrics-servlets/dependencies.html)
* Kite Morphlines Saxon
 [Dependency Information](kite-morphlines/kite-morphlines-saxon/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-saxon/dependencies.html)
* Kite Morphlines Solr Core
 [Dependency Information](kite-morphlines/kite-morphlines-solr-core/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-solr-core/dependencies.html)
* Kite Morphlines Solr Cell
 [Dependency Information](kite-morphlines/kite-morphlines-solr-cell/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-solr-cell/dependencies.html)
* Kite Morphlines Tika Core
 [Dependency Information](kite-morphlines/kite-morphlines-tika-core/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-tika-core/dependencies.html)
* Kite Morphlines Tika Decompress
 [Dependency Information](kite-morphlines/kite-morphlines-tika-decompress/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-tika-decompress/dependencies.html)
* Kite Morphlines Twitter
 [Dependency Information](kite-morphlines/kite-morphlines-twitter/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-twitter/dependencies.html)
* Kite Morphlines UserAgent
 [Dependency Information](kite-morphlines/kite-morphlines-useragent/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-useragent/dependencies.html)
* Kite Morphlines All
 [Dependency Information](kite-morphlines/kite-morphlines-all/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-all/dependencies.html)
* Kite Morphlines All Except Solr
 [Dependency Information](kite-morphlines/kite-morphlines-all-except-solr/dependency-info.html),
 [Dependencies](kite-morphlines/kite-morphlines-all-except-solr/dependencies.html)

## Kite Tools Modules

* Kite Tools
 [Dependency Information](kite-tools/dependency-info.html),
 [Dependencies](kite-tools/dependencies.html)
