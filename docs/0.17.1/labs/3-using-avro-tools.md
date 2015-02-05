---
title: 'Lab 3: Using avro-tools'
sidebar: hidden
layout: page
---

In this lab, you will use `avro-tools` utility to inspect Avro files. You will learn to:

1. Print the key-value metadata for an Avro file
2. Find an Avro file's schema
3. Dump the content of an Avro file as JSON

If you haven't already, make sure you've completed [Lab 2: Create a movies dataset][lab-2].

## Copy an Avro file from HDFS

Start by listing the contents of the dataset you created in lab 2:

```
hadoop fs -ls hdfs:/user/cloudera/example/movies
```

You should see two entries: an Avro data file with the movies you loaded with Kite, and a metadata folder that has metadata for the overall dataset.

```
Found 2 items
drwxr-xr-x   - cloudera cloudera          0 2015-02-03 14:19 hdfs:///user/cloudera/example/movies/.metadata
-rw-r--r--   1 cloudera cloudera      73090 2015-02-03 14:20 hdfs:///user/cloudera/example/movies/a0f892e6-74e5-4098-bfe3-68e2b119046f.avro
```

In this lab, you will use `avro-tools` to inspect the Avro data file from your dataset, which will have a different UUID name. Use the `hadoop` command to copy the `.avro` file to the local file system.

```
hadoop fs -copyToLocal /user/cloudera/example/movies/a0f892e6-74e5-4098-bfe3-68e2b119046f.avro movies.avro
```

## Inspect an Avro data file

For the rest of this lab, use `avro-tools` to inspect the `movies.avro` file.

You might want to refer to the built-in help:

```
avro-tools help
```

Running a command without arguments will print out help for using that command:

```
avro-tools getmeta
```

Using `avro-tools`:

### 1. Find the file schema

### 2. Find the compression algorithm that was used

### 3. Print the content of the data file

_Hint_: Print the data as JSON

## Next

* [View the solution][lab-3-solution]
* Move on to the next lab: [Using `parquet-tools`][lab-4]

[lab-2]: 2-create-a-movies-dataset.html
[lab-3-solution]: 3-using-avro-tools-solution.html
[lab-4]: 4-using-parquet-tools.html
