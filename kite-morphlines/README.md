# Kite - Morphlines Parent

Morphlines is an open source framework that reduces the time and skills necessary to build and
change Hadoop ETL stream processing applications that extract, transform and load data into Apache Solr, Enterprise Data Warehouses, HDFS, HBase or Analytic Online Dashboards.

## Documentation

See [Online Documentation](http://kitesdk.org/docs/current/kite-morphlines/index.html).

## Usage in your Maven Project 

* To pull in the minimum set of dependencies add the following dependency to the `<dependencies>` section of your pom.xml:

```xml
<dependency>
  <groupId>org.kitesdk</groupId>
  <artifactId>kite-morphlines-core</artifactId>
  <version>1.0.0</version> <!-- or whatever the latest version is -->
</dependency>
```

* Alternatively, to pull in the maximum set of dependencies (i.e. all available commands) add the following dependency to the `<dependencies>` section of your pom.xml:

```xml
<dependency>
  <groupId>org.kitesdk</groupId>
  <artifactId>kite-morphlines-all</artifactId>
  <version>1.0.0</version> <!-- or whatever the latest version is -->
  <type>pom</type>
</dependency>
```

* If you want to reuse the little unit test framework (class AbstractMorphlineTest), add the following dependency to the `<dependencies>` section of your pom.xml:

```xml
<dependency>
  <groupId>org.kitesdk</groupId>
  <artifactId>kite-morphlines-core</artifactId>
  <type>test-jar</type>
  <scope>test</scope>
  <version>1.0.0</version> <!-- or whatever the latest version is -->
</dependency>
```

## Building

This step builds the software from source. It also runs the unit tests.

```bash
git clone https://github.com/kite-sdk/kite.git
cd kite
#git tag # list available releases
#git checkout master
#git checkout release-1.0.0 # or whatever the latest version is
mvn clean install -DskipTests -DjavaVersion=1.7
cd kite-morphlines
mvn clean package
find kite-morphlines-core/target -name '*.jar'
find kite-morphlines-all/target -name '*.jar'
```

## Using the Maven CLI to run test data through a morphline

* This section describes how to use the mvn CLI to run test data through a morphline config file. 
* Here we use the simple [MorphlineDemo](https://github.com/kite-sdk/kite/blob/master/kite-morphlines/kite-morphlines-core/src/test/java/org/kitesdk/morphline/api/MorphlineDemo.java) class.

```bash
cd kite/kite-morphlines/kite-morphlines-core
mvn test -DskipTests exec:java -Dexec.mainClass="org.kitesdk.morphline.api.MorphlineDemo" -Dexec.args="src/test/resources/test-morphlines/addValues.conf src/test/resources/test-documents/email.txt" -Dexec.classpathScope=test
```

* The first parameter in `exec.args` above is the morphline config file and the remaining parameters specify one or more data files to run over. At least one data file is required.
* To print diagnostic information such as the content of records as they pass through the morphline commands, consider enabling TRACE log level, for example by adding the following line to your 
`src/test/resources/log4j.properties` file:

```
log4j.logger.org.kitesdk.morphline=TRACE
```

## Integrating with Eclipse

* This section describes how to integrate the codeline with Eclipse.
* Build the software as described above. Then create Eclipse projects like this:

```bash
cd kite
mvn eclipse:eclipse -DjavaVersion=1.7
```

* `mvn eclipse:eclipse` creates several Eclipse projects, one for each maven submodule.
It will also download and attach the jars of all transitive dependencies and their source code to the eclipse
projects, so you can readily browse around the source of the entire call stack.
* Then in eclipse do Menu `File/Import/Maven/Existing Maven Project/` on the root parent
directory `~/kite` and select all submodules, then "Next" and "Finish".
* You will see some maven project errors that keep eclipse from building the workspace because
the eclipse maven plugin has some weird quirks and limitations. To work around this, next, disable
the maven "Nature" by clicking on the project in the browser, right clicking on Menu
`Maven/Disable Maven Nature`. Repeat this for each project. This way you get all the niceties of the maven dependency management
without the hassle of the (current) Maven Eclipse plugin, everything compiles fine from within
Eclipse, and junit works and passes from within Eclipse as well.
* When a pom changes simply rerun `mvn eclipse:eclipse` and
then run Menu `Eclipse/Refresh Project`. No need to disable the Maven "Nature" again and again.
* To run junit tests from within eclipse click on the project (e.g. `kite-morphlines-core`)
in the eclipse project explorer, right click, `Run As/JUnit Test`.
