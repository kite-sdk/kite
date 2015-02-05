---
title: 'Lab 6 Solution: Create a Flume pipeline'
sidebar: hidden
layout: page
---

This is the solution page for [Lab 6: Create a Flume pipeline][lab-6].

## Steps

### 1. Create a Flume configuration

The `flume-config` command will produce a working configuration with the following options:

* `ratings` -- the target dataset
* `--agent agent` -- configure the agent named _agent_
* `--channel-type memory` -- hold ratings in memory while waiting for them to be written

```
kite-dataset flume-config ratings --agent agent --channel-type memory --output flume.conf
```

### 2. Update Flume's configuration and restart

Replace the `/etc/flume-ng/conf/flume.conf` file, where Flume will look for its configuration.

```
sudo cp flume.conf /etc/flume-ng/conf/flume.conf
sudo /etc/init.d/flume-ng-agent restart
```

Flume's log should show successful start-up messages for the source and sink:

```
...
INFO  [thread-1] (o.a.f.s.k.DatasetSink.start:191)  - Started DatasetSink dataset:hive://127.0.0.1:9083/default/default/ratings
...
INFO  [thread-3] (o.a.f.s.AvroSource.start:253)  - Avro source avro-event-source started.
...
```

### 3. Build and run the ratings application

```
cd /home/cloudera/ratings-app
mvn package
hadoop jar target/ratings-app-0.17.1-runtime.jar
```

### 4. Rate some movies

Use the running app to [rate a few movies][ratings-app].

You will see the ratings show in the terminal window as they are submitted:

```
>> Listening on 0.0.0.0:4567
...
Sending rating: {"timestamp": 1423073425248, "user_id": 34, "movie_id": 176, "rating": 5}
Sending rating: {"timestamp": 1423073571712, "user_id": 34, "movie_id": 367, "rating": 4}
```

[ratings-app]: http://localhost:4567/

### 5. Verify the ratings are written correctly

To use Impala, re-sync the metadata and select ratings from this year:

```
impala-shell -q 'invalidate metadata'
impala-shell -q 'select * from ratings where year=2015'
```
```
Query: select * from ratings where year=2015
+---------------+---------+----------+--------+------+-------+
| timestamp     | user_id | movie_id | rating | year | month |
+---------------+---------+----------+--------+------+-------+
| 1423073571712 | 34      | 367      | 4      | 2015 | 2     |
| 1423073425248 | 34      | 176      | 5      | 2015 | 2     |
+---------------+---------+----------+--------+------+-------+
Fetched 2 row(s) in 0.61s
```

To use Kite, you can pass a [view URI][view-uris] to the show command:

```
kite-dataset show view:hive:ratings?year=2015
```
```
{"timestamp": 1423073571712, "user_id": 34, "movie_id": 367, "rating": 4}
{"timestamp": 1423073425248, "user_id": 34, "movie_id": 176, "rating": 5}
```

[view-uris]: http://kitesdk.org/docs/0.17.1/URIs.html#view-uris

## Next

* [Back to the lab][lab-6]
* Move on to the next lab: [Analyze ratings with Crunch][lab-7]

[lab-6]: 6-create-a-flume-pipeline.html
[lab-7]: 7-analyze-ratings-with-crunch.html
