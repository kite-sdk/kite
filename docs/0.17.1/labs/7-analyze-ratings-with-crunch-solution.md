---
title: 'Lab 7 Solution: Analyze ratings with Crunch'
sidebar: hidden
layout: page
---

This is the solution page for [Lab 7: Analyze ratings with Crunch][lab-7].

## Steps

### 1. Build and run the Crunch job

```
cd /home/cloudera/ratings-crunch
git checkout bimodal
mvn clean package
mvn kite:run-tool
```

### 2. Look at the code

Read through the source at [`src/main/java/org/kitesdk/examples/movies/AnalyzeRatings.java`][job-code].

Good questions earn prizes.

[job-code]: https://github.com/rdblue/ratings-crunch/blob/bimodal/src/main/java/org/kitesdk/examples/movies/AnalyzeRatings.java

### 3. Find an interesting movie

You can use a SQL join query to view the title and the ratings histogram at once, using Hive.

Using `beeline` will produce pretty output. Start `beeline` in embedded mode:

```
beeline -u jdbc:hive2://
```
```
select m.title, h.histogram from movies as m, ratings_histograms as h where m.id = h.movie_id;
```
```
+----------------------------------------------------------------------------------+--------------------+--+
|                                     m.title                                      |    h.histogram     |
+----------------------------------------------------------------------------------+--------------------+--+
| ...                                                                              |                    |
| Bio-Dome (1996)                                                                  | [16,5,8,1,1]       |
| ...                                                                              |                    |
+----------------------------------------------------------------------------------+--------------------+--+
50 rows selected (42.249 seconds)
```

That can't be right. Bio-Dome is a classic!

## Next

* [Back to the lab][lab-7]

[lab-7]: 7-analyze-ratings-with-crunch.html
