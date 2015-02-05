---
title: 'Lab 2: Create a movies dataset'
sidebar: hidden
layout: page
---

In this lab, you will create a simple dataset of stored in HDFS. You will learn to:

1. Infer a Schema from a CSV data sample
2. Create a dataset using command-line tools
3. Import data from a CSV file

If you haven't already, make sure you've completed [Lab 1: Setting up the Quickstart VM][lab-1].

## Movies data from GroupLens

This lab uses the [MovieLens][movielens] data, collected and made available by [GroupLens][grouplens]. The MovieLens data is a large set of real ratings for a group of movies. For this series of labs, you need the small version with 100,000 ratings.

Start by downloading the data:

```
curl http://files.grouplens.org/datasets/movielens/ml-100k.zip -o ml-100k.zip
unzip ml-100k.zip
```

In the `ml-100k` directory, the three files you need are `README`, `u.item`, and `u.data`.

* `README` has the column names for 
* `u.item` is a headerless pipe-separated CSV file of movies
* `u.data` is a headerless tab-separated CSV file of movie ratings

The `u.user` file has anonymous data about raters, but isn't needed for this series of labs.

[movielens]: http://grouplens.org/datasets/movielens/
[grouplens]: http://grouplens.org/

## Steps

### 1. Use the `kite-dataset` command to create a schema for the movies data, named `movie.avsc`.

The columns in the data, from the `README`, are: _id_, _title_, _release\_date_, _video\_release\_date_, and _imdb\_url_.

You might need to refer to the [`kite-dataset` online reference][cli-csv-schema] or the built-in help:

```
kite-dataset help csv-schema
```

You should edit the schema to replace the generic field names with the column names above.

_Hint_: you only need the first few data columns and can remove the genre columns from the schema (field\_5 to the end). The next steps will ignore data columns that aren't in the schema.

[cli-csv-schema]: http://kitesdk.org/docs/0.17.1/cli-reference.html#csv-schema

### 2. Create a dataset in HDFS

Create a dataset stored in HDFS, using the `kite-dataset` command and the `movie.avsc` schema.

Kite identifies a dataset by a URI, which should be `dataset:hdfs:/user/cloudera/example/movies` to work with the other lab modules.

You might need to refer to the [online reference][cli-create] or built-in help:

```
kite-dataset help create
```

[cli-create]: http://kitesdk.org/docs/0.17.1/cli-reference.html#create

### 3. Import the movies into the new dataset, `dataset:hdfs:/user/cloudera/examples`

You might need to refer to the [online reference][cli-csv-import] or built-in help:

```
kite-dataset help csv-import
```

[cli-csv-import]: http://kitesdk.org/docs/0.17.1/cli-reference.html#csv-import

## Next

* [View the solution][lab-2-solution]
* Move on to the next lab: [Using `avro-tools`][lab-3]

[lab-1]: 1-setting-up-the-quickstart-vm.html
[lab-2-solution]: 2-create-a-movies-dataset-solution.html
[lab-3]: 3-using-avro-tools.html
