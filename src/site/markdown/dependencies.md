# Dependency Information

To use the CDK modules in a Java project add the Cloudera repository to your Maven POM:

    <repository>
      <id>cdh.repo</id>
      <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
      <name>Cloudera Repositories</name>
      <snapshots>
        <enabled>false</enabled>
      </snapshots>
    </repository>

Then add a dependency for each module you want to use by referring to the
information listed on the Dependency Information pages listed below.
You can also view the transitive dependencies for each module.

## Hadoop Component Dependencies

As a general rule, CDK modules mark Hadoop component dependencies as having `provided`
[scope](http://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Transitive_Dependencies),
since in many cases the dependencies are provided by the container that the code is
running in.

For example,

* CDK Data has a `provided` dependency on the core Hadoop libraries
* CDK Crunch has a `provided` dependency on Crunch and the core Hadoop libraries
* CDK HCatalog has a `provided` dependency on HCatalog

The following containers provide the dependencies listed:

* The CDK Maven Plugin goal `cdk:run-tool` provides the Hadoop and HCatalog dependencies.
* The CDK Maven Plugin goal `cdk:run-job` provides the Hadoop dependencies. HCatalog
should be added as a `runtime` dependency ([example](https://github.com/cloudera/cdk-examples/tree/master/demo/demo-oozie)).
* The `hadoop jar` command provides the Hadoop dependencies.

However, there are some cases where you may have to provide the relevant Hadoop component
dependencies yourself:

* Crunch programs (even those running in the containers listed above) ([example](https://github.com/cloudera/cdk-examples/tree/master/demo/demo-crunch))
* Standalone Java programs, not run using `cdk:run-tool` or `hadoop jar` ([example](https://github.com/cloudera/cdk-examples/tree/master/dataset))
* Web apps ([example](https://github.com/cloudera/cdk-examples/tree/master/logging-webapp))

## CDK Data Modules

* CDK Data Core
 [Dependency Information](cdk-data/cdk-data-core/dependency-info.html),
 [Dependencies](cdk-data/cdk-data-core/dependencies.html)
* CDK Data Crunch
 [Dependency Information](cdk-data/cdk-data-crunch/dependency-info.html),
 [Dependencies](cdk-data/cdk-data-crunch/dependencies.html)
* CDK Data Flume
 [Dependency Information](cdk-data/cdk-data-flume/dependency-info.html),
 [Dependencies](cdk-data/cdk-data-flume/dependencies.html)
* CDK Data HCatalog
 [Dependency Information](cdk-data/cdk-data-hcatalog/dependency-info.html),
 [Dependencies](cdk-data/cdk-data-hcatalog/dependencies.html)

## CDK Morphlines Modules

* CDK Morphlines Core
 [Dependency Information](cdk-morphlines/cdk-morphlines-core/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-core/dependencies.html)
* CDK Morphlines Avro
 [Dependency Information](cdk-morphlines/cdk-morphlines-avro/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-avro/dependencies.html)
* CDK Morphlines Hadoop Sequence File
 [Dependency Information](cdk-morphlines/cdk-morphlines-hadoop-sequencefile/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-hadoop-sequencefile/dependencies.html)
* CDK Morphlines Solr Core
 [Dependency Information](cdk-morphlines/cdk-morphlines-solr-core/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-solr-core/dependencies.html)
* CDK Morphlines Solr Cell
 [Dependency Information](cdk-morphlines/cdk-morphlines-solr-cell/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-solr-cell/dependencies.html)
* CDK Morphlines Tika Core
 [Dependency Information](cdk-morphlines/cdk-morphlines-tika-core/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-tika-core/dependencies.html)
* CDK Morphlines Tika Decompress
 [Dependency Information](cdk-morphlines/cdk-morphlines-tika-decompress/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-tika-decompress/dependencies.html)
* CDK Morphlines Twitter
 [Dependency Information](cdk-morphlines/cdk-morphlines-twitter/dependency-info.html),
 [Dependencies](cdk-morphlines/cdk-morphlines-twitter/dependencies.html)

## CDK Tools Modules

* CDK Tools
 [Dependency Information](cdk-tools/dependency-info.html),
 [Dependencies](cdk-tools/dependencies.html)
