---
title: 'Lab 7: Analyze ratings with Crunch'
sidebar: hidden
layout: page
---

In this lab, you will run a Crunch job to analyze movie ratings. You will learn to:

1. Build a crunch job
2. Build and run a movie rating web application

If you haven't already, make sure you've completed [Lab 5: Create a partitioned dataset][lab-5] and, optionally, [Lab 6: Create a Flume pipeline][lab-6].

## Background

In this lab, you will build and run a Crunch job that analyzes the movie ratings dataset you imported in lab 5.

You want to identify movies that have two distinct rating peaks. This would show two groups of people with very different opinions of a particular movie.

A simple way to detect this is done in two steps:

1. Count the ratings for each movie to produce a histogram of each rating and the number of people that chose it.
2. Look for two peaks in the histogram, where the count goes up, then down, then back up.

The Crunch job you will run in this lab uses a MapReduce round to produce the rating-to-count histogram for each movie, then filters out the movies that don't meet the criteria in the second step. Filtering is done at the end of the reduce phase.

## Steps

### 1. Build and run the Crunch job

Build the Crunch job with Maven:

```
cd /home/cloudera/ratings-crunch
checkout bimodal
mvn clean package
```

Next, submit the job:

```
mvn kite:run-tool
```

This uses the Kite maven plugin to submit the job jar with all of its dependencies. The configuration, which sets the main class, is in the project's POM file.

### 2. Look at the code

While the Crunch job runs, look at the source located at [`src/main/java/org/kitesdk/examples/movies/AnalyzeRatings.java`][job-code].

The Crunch job is made of a series of small functions that do a single task.

The map phase extracts two pieces of information from a Rating object:

* `GetMovieID` returns the ID that is used to group ratings together.
* `GetRating` returns the rating value.

Next, the id and rating pairs are grouped by id. All of the ratings for a movie are processed as a single group in the reduce phase.

The reduce phase is made of three tasks:

* `AccumulateRatings` produces the rating-to-count histogram by counting the each rating for a movie.
* `BuildRatingsHistogram` converts the movie id and accumulated ratings into an Avro object.
* `IdentifyBimodalMovies` filters the histograms, selecting those with a peak, then a drop, then another peak.

The final output is stored in a Hive dataset called `ratings_histograms`.

[job-code]: https://github.com/rdblue/ratings-crunch/blob/bimodal/src/main/java/org/kitesdk/examples/movies/AnalyzeRatings.java

### 3. Look at the results

Find an interesting movie id in the `ratings_histograms` table and find its title.

_Hint_: Impala doesn't support nested types yet. To join the results using SQL, run your query in Hive.

## Next

* [View the solution][lab-7-solution]

[lab-5]: 5-create-a-partitioned-dataset.html
[lab-6]: 6-create-a-flume-pipeline.html
[lab-7-solution]: 7-analyze-ratings-with-crunch-solution.html
