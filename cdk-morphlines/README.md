# CDK - Morphlines Parent

Cloudera Morphlines is an open source framework that reduces the time and skills necessary to build and
change Hadoop ETL stream processing applications that extract, transform and load data into Apache Solr, Enterprise Data Warehouses, HDFS, HBase or Analytic Online Dashboards.

## Documentation

See [Online Documentation](http://cloudera.github.io/cdk/docs/current/cdk-morphlines/index.html).

## Usage in your Maven Project 

* To use Cloudera Morphlines in your Maven project, add the following repository to the `<repositories>` section of your pom.xml:

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

* Also, to pull in the minimum set of dependencies add the following dependency to the `<dependencies>` section of your pom.xml:

```xml
<dependency>
  <groupId>com.cloudera.cdk</groupId>
  <artifactId>cdk-morphlines-core</artifactId>
  <version>0.8.0</version> <!-- or whatever the latest version is -->
</dependency>
```

* Alternatively, to pull in the maximum set of dependencies (i.e. all available commands) add the following dependency to the `<dependencies>` section of your pom.xml:

```xml
<dependency>
  <groupId>com.cloudera.cdk</groupId>
  <artifactId>cdk-morphlines-all</artifactId>
  <version>0.8.0</version> <!-- or whatever the latest version is -->
  <type>pom</type>
</dependency>
```

## Building

This step builds the software from source.

```bash
git clone git@github.com:cloudera/cdk.git
cd cdk
#git checkout master
mvn clean package
find cdk-morphlines/cdk-morphlines-core/target -name '*.jar'
find cdk-morphlines/cdk-morphlines-all/target -name '*.jar'
```

## Integrating with Eclipse

* This section describes how to integrate the codeline with Eclipse.
* Build the software as described above. Then create Eclipse projects like this:

```bash
cd cdk
mvn eclipse:eclipse
```

* `mvn eclipse:eclipse` creates several Eclipse projects, one for each maven submodule.
It will also download and attach the jars of all transitive dependencies and their source code to the eclipse
projects, so you can readily browse around the source of the entire call stack.
* Then in eclipse do Menu `File/Import/Maven/Existing Maven Project/` on the root parent
directory `~/cdk` and select all submodules, then "Next" and "Finish".
* You will see some maven project errors that keep eclipse from building the workspace because
the eclipse maven plugin has some weird quirks and limitations. To work around this, next, disable
the maven "Nature" by clicking on the project in the browser, right clicking on Menu
`Maven/Disable Maven Nature`. Repeat this for each project. This way you get all the niceties of the maven dependency management
without the hassle of the (current) Maven Eclipse plugin, everything compiles fine from within
Eclipse, and junit works and passes from within Eclipse as well.
* When a pom changes simply rerun `mvn eclipse:eclipse` and
then run Menu `Eclipse/Refresh Project`. No need to disable the Maven "Nature" again and again.
* To run junit tests from within eclipse click on the project (e.g. `cdk-morphlines-core`)
in the eclipse project explorer, right click, `Run As/JUnit Test`.
