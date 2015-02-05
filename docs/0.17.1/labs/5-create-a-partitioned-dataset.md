---
title: 'Lab 5: Create a partitioned dataset'
sidebar: hidden
layout: page
---

In this lab, you will create a partitioned dataset to store movie ratings. You will learn to:

1. Build a partition strategy JSON configuration
2. Create a partitioned dataset and import data
3. Explore the dataset using Impala

If you haven't already, make sure you've completed [Lab 2: Create a movies dataset][lab-2].

## Data preparation

In this lab, you will use the MovieLens ratings data from lab 2.

The ratings in `u.data` need a slight transformation before they are immediately usable. Run the following commands to produce `ratings.csv`.

```
echo "timestamp,user_id,movie_id,rating" > ratings.csv
cat u.data | awk '{ print $4 "000," $1 "," $2 "," $3 }' | sort >> ratings.csv
```

This transform work _could_ be done during import using MapReduce, but would require additional topics that are outside the scope of this series of labs.

Your `ratings.csv` file should look like this, including a header line and the timestamp value in milliseconds:

```
timestamp,user_id,movie_id,rating
874724710000,259,255,4
874724727000,259,286,4
```

## Steps

### 1. Infer a schema from the `ratings.csv` file, named `rating.avsc`

The ratings will be partitioned by time, so the `timestamp` field must be _required_ by the schema.

### 2. Build a partition strategy JSON file, named `year-month.json`

Use the `kite-dataset` command and the `rating.avsc` schema, build a partition strategy that will store data by year and month.

You might need to refer to the [online reference][cli-partition-config], which has more detail than the built-in help.

[cli-partition-config]: http://kitesdk.org/docs/0.17.1/cli-reference.html#partition-config

### 3. Create a dataset called `ratings`

With the configuration you created, `rating.avsc` and `year-month.json`, create a Hive dataset called `ratings`.

Hive is the default storage for datasets; you can use either the full URI, `dataset:hive:ratings`, or simply `ratings` instead.

### 4. Import the ratings data

You might want to refer to the [online reference][cli-csv-import], or the built-in help:

```
kite-dataset help csv-import
```

[cli-csv-import]: http://kitesdk.org/docs/0.17.1/cli-reference.html#csv-import

### 5. Run a SQL query in Impala

You can run queries in the Impala shell with `impala-shell -q`.

Before running queries over the ratings data, run `invalidate metadata` to force an immediate re-sync with Hive:

```
impala-shell -q 'invalidate metadata'
```

## Next

* [View the solution][lab-5-solution]
* Move on to the next lab: [Create a Flume pipeline][lab-6]

[lab-2]: 2-create-a-movies-dataset.html
[lab-5-solution]: 5-create-a-partitioned-dataset-solution.html
[lab-6]: 6-create-a-flume-pipeline.html
