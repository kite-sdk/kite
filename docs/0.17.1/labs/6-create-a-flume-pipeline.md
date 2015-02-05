---
title: 'Lab 6: Create a Flume pipeline'
sidebar: hidden
layout: page
---

In this lab, you will create a Flume pipeline to send movie ratings from a web application into HDFS. You will learn to:

1. Configure Flume to write records to a dataset
2. Build and run a movie rating web application

If you haven't already, make sure you've completed [Lab 2: Create a movies dataset][lab-2] and [Lab 5: Create a partitioned dataset][lab-5].

## Background

In this lab, you will build a Flume pipeline that adds movie ratings to the ratings dataset you created in lab 5.

[Flume][flume] collects individual ratings sent via RPC and constantly appends them to a dataset. Ratings are produced by a simple web application, located in `/home/cloudera/ratings-app`.

This architecture mimics a real-world deployment:

* The web application is decoupled from Hadoop. It does a minimal amount of work to send records, then moves on with its purpose: user interaction.
* Flume accepts records from the application as quickly as possible to avoid interrupting the user experience.
* Flume handles durability and eventual storage in HDFS.
* The Hadoop cluster doesn't need to be exposed to the web application tier, unless the application tier chooses to access Hadoop data directly. (This application caches the movies dataset.)

[flume]: https://flume.apache.org/FlumeUserGuide.html

## Steps

### 1. Create a Flume configuration, named `flume.conf`

You might want to refer to the [online reference][cli-flume-config], or the built-in help:

```
kite-dataset help flume-config
```

_Hints_:

* You should set the agent name to _agent_ when generating your Flume configuration
* The web application is configured to use the default RPC port, 41415
* For this example, it is easier to use a memory channel

[cli-flume-config]: http://kitesdk.org/docs/0.17.1/cli-reference.html#flume-config

### 2. Restart Flume using your `flume.conf`

Replace the `/etc/flume-ng/conf/flume.conf` file, where Flume will look for its configuration.

```
sudo cp flume.conf /etc/flume-ng/conf/flume.conf
sudo /etc/init.d/flume-ng-agent restart
```

You can verify that there were no start-up errors by looking at the Flume log, `/var/log/flume-ng/flume.log`.

### 3. Build and run the ratings application

Build the ratings web application with Maven:

```
cd /home/cloudera/ratings-app
mvn package
```

Then run the runtime jar with the `hadoop` command:

```
hadoop jar target/ratings-app-0.17.1-runtime.jar
```

### 4. Rate some movies

Once the application has started, [rate a few movies with the app][ratings-app].

In the terminal window where the application is running, you should see ratings shown as they are submitted:

```
>> Listening on 0.0.0.0:4567
...
Sending rating: {"timestamp": 1423073425248, "user_id": 34, "movie_id": 176, "rating": 5}
Sending rating: {"timestamp": 1423073571712, "user_id": 34, "movie_id": 367, "rating": 4}
```

[ratings-app]: http://localhost:4567/

### 5. Verify the ratings are written correctly

The Flume configuration will release data files every 30 seconds. Use Impala or the Kite `show` command to see the new records that have been added by Flume.

Remember, you can run individual Impala queries using `-q` and you will need to re-sync Impala's metadata to pick up new partitions:

```
impala-shell -q 'invalidate metadata'
```

## Next

* [View the solution][lab-6-solution]
* Move on to the next lab: [Create a Flume pipeline][lab-7]

[lab-2]: 2-create-a-movies-dataset.html
[lab-5]: 5-create-a-partitioned-dataset.html
[lab-6-solution]: 6-create-a-flume-pipeline-solution.html
[lab-7]: 7-analyze-ratings-with-crunch.html
