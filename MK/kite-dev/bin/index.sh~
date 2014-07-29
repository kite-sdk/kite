#!/bin/sh
#
# Create an index to strore a datasets index in Cloudera-Search
# 
# Default collection:  myusers
#
########################################################################
clear
echo Create the index now ... 

export COLLECTION=myusers

if [ -n $1 ] 
then	
	echo Default collection:  myusers
else
	COLLECTION=$1
	echo Collection name: $COLLECTION
fi

# Import the dataset into Kite ...
#
./dataset delete dataset:hdfs:/user/training/myusers 
./dataset create dataset:hdfs:/user/training/myusers -s user.avsc
./dataset schema dataset:hdfs:/user/training/myusers
./dataset csv-import users.csv dataset:hdfs:/user/training/myusers
./dataset show dataset:hdfs:/user/training/myusers


# Autocreate the schema file ...
#
./dataset solr-schema dataset:hdfs:/user/training/myusers
#read 

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
#solrctl --zk dev.loudacre.com:2181/solr instancedir --create $COLLECTION ./index/$COLLECTION

##solrctl --zk dev.loudacre.com:2181/solr instancedir --update $COLLECTION ./index/$COLLECTION
##solrctl --zk dev.loudacre.com:2181/solr collection --create $COLLECTION
#read

# Run the mapreduce import procedure ...
#
echo # IMPORT via MapReduce / Spark
hadoop jar /usr/lib/solr/contrib/mr/search-mr-1.1.0-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
--morphline-file default-morphlines.conf \
--output-dir hdfs://dev.loudacre.com/user/training/indexes/$COLLECTION \
--zk-host dev.loudacre.com:2181/solr \
--collection $COLLECTION hdfs://dev.loudacre.com/user/training/$COLLECTION \
--mappers 1 \
--reducers 1 \
--go-live 

echo Look now into your SOLR WebUI ....
echo Done.




