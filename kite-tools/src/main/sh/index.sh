#!/bin/sh
#
# Create a SOLR-Collection to store a dataset indexed in Cloudera-Search
#
##########################################################################
clear
echo "> Import a CSV-file and create the SOLR-index ... "

export COLLECTION=$1
export SCHEMA=$2
export CSVFILE=$3

echo " Collection name : $COLLECTION"
echo " SCHEMA name     : $SCHEMA"
echo " CSV-File name   : $CSVFILE"

echo "> Will drop the dataset if it already exists.     "
read

# Import the dataset into Kite ...
#
./dataset delete dataset:hdfs:/user/training/$COLLECTION
echo Deleted ...

./dataset create dataset:hdfs:/user/training/$COLLECTION -s $SCHEMA.avsc
echo Created ...

./dataset schema dataset:hdfs:/user/training/$COLLECTION
./dataset csv-import $CSVFILE.csv dataset:hdfs:/user/training/$COLLECTION
echo Data imported ...

./dataset show dataset:hdfs:/user/training/$COLLECTION

# Autocreate the schema file ...
#
./dataset solr-schema dataset:hdfs:/user/training/$COLLECTION

# Create a local index-directory, starting point for customization ...
#
rm -r index
mkdir ./index/

cd index
solrctl --zk dev.loudacre.com:2181/solr instancedir --generate $COLLECTION
cd ..

cp schema.xml ./index/$COLLECTION/conf/schema.xml

# Deploy the index to SOLR
#
echo Deploy SOLR config ...
solrctl --zk dev.loudacre.com:2181/solr instancedir --create $COLLECTION ./index/$COLLECTION

##solrctl --zk dev.loudacre.com:2181/solr instancedir --update $COLLECTION ./index/$COLLECTION
solrctl --zk dev.loudacre.com:2181/solr collection --create $COLLECTION

# Run the mapreduce import procedure ...
#
echo # IMPORT via MapReduce / Spark
hadoop jar /usr/lib/solr/contrib/mr/search-mr-1.1.0-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
--morphline-file $COLLECTION-csv-morphlines.conf \
--output-dir hdfs://dev.loudacre.com/user/training/indexes/$COLLECTION \
--zk-host dev.loudacre.com:2181/solr \
--collection $COLLECTION hdfs://dev.loudacre.com/user/training/$COLLECTION \
--mappers 1 \
--reducers 1 \
--go-live 

echo Look now into your SOLR WebUI ....
echo Done.

hadoop fs -chmod 774 hdfs://dev.loudacre.com/user/training/$COLLECTION/.metadata
hadoop fs -mkdir hdfs://dev.loudacre.com/user/training/$COLLECTION/.metadata/SOLR
hadoop fs -mkdir hdfs://dev.loudacre.com/user/training/$COLLECTION/.metadata/FLUME

hadoop fs -put ./index/$COLLECTION hdfs://dev.loudacre.com/user/training/$COLLECTION/.metadata/SOLR
hadoop fs -put ./$COLLECTION-csv-morphlines.conf hdfs://dev.loudacre.com/user/training/$COLLECTION/.metadata/FLUME





