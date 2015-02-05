---
title: 'Lab 4: Using parquet-tools'
sidebar: hidden
layout: page
---

In this lab, you will use `parquet-tools` utility to inspect Parquet files. You will learn to:

1. Print the metadata and schema for a Parquet file
2. View column-level compression ratios
3. Dump the content of Parquet file
4. Explore the structure of a Parquet file from its metadata

If you haven't already, make sure you've completed [Lab 2: Create a movies dataset][lab-2].

## Create a Parquet dataset of movies

This lab requires a Parquet data file, you will create by copying an Avro dataset to a Parquet dataset.

First, create a Parquet dataset in Hive. Use `--format` to configure the dataset to store data as Parquet files.

```
kite-dataset create dataset:hive:movies --schema movie.avsc --format parquet
```

Next, import the movies data using `csv-import`:

```
kite-dataset csv-import u.item --no-header --delimiter '|' dataset:hive:movies
```

You can verify the data import using either the `show` command.

## Copy a Parquet file from HDFS

Start by listing the contents of the dataset you created above, which is in the Hive warehouse directory:

```
hadoop fs -ls hdfs:/user/hive/warehouse/movies
```

```
Found 1 items
-rw-r--r--   1 cloudera hive      77314 2015-02-03 17:26 /user/hive/warehouse/movies/72bc5d73-000f-4f16-ae03-fa14eeb74c38.parquet
```

Use the `hadoop` command to copy the `.parquet` file to the local file system.

```
hadoop fs -copyToLocal /user/hive/warehouse/movies/72bc5d73-000f-4f16-ae03-fa14eeb74c38.parquet movies.parquet
```

## Inspect a Parquet data file

For the rest of this lab, use `parquet-tools` to inspect the `movies.parquet` file.

You might need to refer to the built-in help:

```
parquet-tools --help
```

Running a command with `-h` will print out help for using that command:

```
parquet-tools meta -h
```

Using `parquet-tools`:

### 1. Find the file schema

### 2. The file's Avro schema

### 3. Print the content of the data file

### 4. Find the column with the best compression ratio

## Next

* [View the solution][lab-4-solution]
* Move on to the next lab: [Create a partitioned dataset][lab-5]

[lab-2]: 2-create-a-movies-dataset.html
[lab-4-solution]: 4-using-parquet-tools-solution.html
[lab-5]: 5-create-a-partitioned-dataset.html
