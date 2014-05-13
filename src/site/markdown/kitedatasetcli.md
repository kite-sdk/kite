<a name="top" />

# Dataset Command-line Interface

The Kite Dataset command line interface (CLI) provides utility commands that let you quickly create a schema and dataset, import data from a CSV file, then view the results.

Each command is described below. See [Using the Kite CLI to Create a Dataset](usingkiteclicreatedataset.html) for a practical example of the CLI in use.

* [csv-schema](#csvSchema) (create a schema from a CSV data file)
* [obj-schema](#objSchema) (create a schema from a Java object)
* [create](#create) (create a dataset based on an existing schema)
* [schema](#schema) (view the schema for an existing dataset)
* [csv-import](#csvImport) (import a CSV data file)
* [show](#show) (show the first _n_ records of a dataset)
* [delete](#delete) (delete a dataset)
* [partition-config](#partition-config) (create a partition strategy for a schema)
* [help](#help) (get help for the dataset command in general or a specific command)

<a name="csvSchema" />
##csv-schema

Use `csv-schema` to generate an Avro schema from a comma separated value (CSV) file.

###Usage

`dataset [general options] csv-schema <sample csv path> [command options]`

###Options
`--skip-lines`

The number of lines to skip before the start of the CSV data. Default is 0.

`--quote`

Quote character in the CSV data file. Default is the double-quote (&quot;).

`--delimiter`

Delimiter character in the CSV data file. Default is the comma (,).

`--escape`

Escape character in the CSV data file. Default is the backslash (\\).

`--class, --record-name`

A class name or record name for the schema result. This value is **required**.

`-o, --output`

Save schema avsc to path.

`--no-header`

Use this option when the CSV data file does not have header information in the first line. Fields are given the default names *field_0*, *field_1*, and so on.

`--minimize`

Minimize schema file size by eliminating white space.

###Examples

Print the schema to standard out: `dataset csv-schema sample.csv --class Sample`

Write the schema to sample-schema.avsc: `dataset csv-schema sample.csv -o sample-schema.avsc`

---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)


<a name="objSchema" />
##obj-schema
Build a schema from a Java class.
###Usage

`dataset [general options] obj-schema <class name> [command options]`

###Options

`-o, --output`

Save schema in Avro format to a given path.

`--jar`

Add a jar to the classpath used when loading the Java class.

`--lib-dir`

Add a directory to the classpath used when loading the Java class.

`--minimize`

Minimize schema file size by eliminating white space.

###Examples

Create a schema for an example User class:
`dataset obj-schema org.kitesdk.cli.example.User`

Create a schema for a class in a jar:
`dataset obj-schema com.example.MyRecord --jar my-application.jar`

Save the schema for the example User class to user.avsc:
`dataset obj-schema org.kitesdk.cli.example.User -o user.avsc`

---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="create" />
##create
After you have generated an Avro schema, you can use `create` to make an empty dataset.

###Usage

`dataset [general options] create <dataset name> [command options]`

###Options

`-d, --directory`

The root directory of the dataset repository. Optional if using Hive for metadata storage.

`--use-hbase`

Store data in HBase tables.

`--use-hdfs`

Store data in HDFS files.

`--use-hive`

Store data in Hive managed tables (default).

`--zookeeper`

ZooKeeper host list as host or host:port.

`-s, --schema`

The file containing the Avro schema. This value is **required**.

`-f, --format`

By default, the dataset is created in Avro format. Use this switch to set the format to Parquet (`-f parquet`).

`-p, --partition-by`

The file containing a JSON-formatted partition strategy.


###Examples:

Create dataset &quot;users&quot; in Hive:
`dataset create users --schema user.avsc`

Create dataset &quot;users&quot; using Parquet:
`dataset create users --schema user.avsc --format parquet`

Create dataset &quot;users&quot; partitioned by JSON configuration:
`dataset create users --schema user.avsc --partition-by user_part.json`



---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="schema" />
##schema

Show the schema for a dataset.

###Usage

`dataset [general options] schema <dataset name> [command options]`

###Options

`-d, --directory`

The root directory of the dataset repository. Optional if you are using Hive for metadata storage.

`--use-hbase`

Store data in HBase tables.

`--use-hdfs`

Store data in HDFS files.

`--use-hive`

Store data in Hive managed tables (default).

`--zookeeper`

ZooKeeper host list as host or host:port.

`--minimize`

Minimize schema file size by eliminating white space.

`-o, --output`

Save schema in Avro format to a given path.

###Examples:

Print the schema for dataset &quot;users&quot; to standard out: `dataset schema users`

Save the schema for dataset &quot;users&quot; to user.avsc: `dataset schema users -o user.avsc`


---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="csvImport" />
##csv-import

Copy CSV records into a dataset.

###Usage

`dataset [general options] csv-import <csv path> <dataset name> [command options]`

###Options

`-d, --directory`

The root directory of the dataset repository. Optional if using Hive for metadata storage.

`--use-hbase`

Store data in HBase tables.

`--use-hdfs`

Store data in HDFS files.

`--use-hive`

Store data in Hive managed tables (default).

`--zookeeper`

ZooKeeper host list as host or host:port.

`--escape`

Escape character. Default is backslash (\\).

`--delimiter`

Delimiter character. Default is comma (,).

`--quote`

Quote character. Default is double quote (&quot;).

`--skip-lines`


Lines to skip before CSV start (default: 0)

`--no-header`

Use this option when the CSV data file does not have header information in the first line. Fields are given the default names *field_0*, *field_1*, and so on.

###Examples

Copy the records from `sample.csv` to a dataset named &quot;sample&quot;: `dataset csv-import csv-import path/to/sample.csv sample`


---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="show" />
##show

Print the first *n* records in a dataset.

###Usage

`dataset [general options] show <dataset name> [command options]`

###Options

`-d, --directory`

The root directory of the dataset repository. Optional if using Hive for metadata storage.

`--use-hbase`

Store data in HBase tables.

`--use-hdfs`

Store data in HDFS files.

`--use-hive`

Store data in Hive managed tables (default).

`--zookeeper`

ZooKeeper host list as host or host:port.

`-n, --num-records`

The number of records to print. The default number is 10.

###Examples

Show the first 10 records in dataset &quot;users&quot;: `dataset show users`

Show the first 50 records in dataset &quot;users&quot;: `dataset show users -n 50`


---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="delete" />
##delete

Delete one or more datasets and related metadata.

###Usage

`dataset [general options] delete <dataset names> [command options]`

###Options

`-d, --directory`

The root directory of the dataset repository. Optional if using Hive for metadata storage.

`--use-hbase`

Store data in HBase tables.

`--use-hdfs`

Store data in HDFS files.

`--use-hive`

Store data in Hive managed tables (default).

`--zookeeper`

ZooKeeper host list as host or host:port.

###Examples

Delete all data and metadata for the dataset &quot;users&quot;: `dataset delete users`


---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="partition-config" />
##partition-config

Builds a partition strategy for a schema.

###Usage

dataset [general options] partition-config <field:type pairs> [command options]

###Options:

`-s, --schema`

The file containing the Avro schema. **This value is required**.

`-o, --output`

Save partition JSON file to path

`--minimize`

Minimize output size by eliminating white space

###Examples

Partition by email address, balanced across 16 hash partitions and save as a JSON file.
`dataset partition-config email:hash[16] email:copy -s user.avsc -o part.json`

Partition by created_at time&apos;s year, month, and day
`dataset partition-config created_at:year created_at:month created_at:day -s event.avsc`


---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)

<a name="help" />
##help

Retrieves details on the functions of one or more dataset commands.

###Usage

`dataset [general options] help <commands> [command options]`

###Examples

Retrieve details for the create, show, and delete commands. `dataset help create show delete`

---

[Top](#top) | [csv-schema](#csvSchema) | [obj-schema](#objSchema) | [create](#create) | [schema](#schema) | [csv-import](#csvImport) | [show](#show) | [delete](#delete) | [partition-config](#partition-config) | [help](#help)
