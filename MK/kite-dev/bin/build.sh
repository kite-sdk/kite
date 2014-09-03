#!/bin/bash
#
# This script create an executable including the jar file for the
# kite-tools.
#
##########################################################################

#
# go to the parrent folder of the kite project and then into kite-tools
#
cd ./../../../kite-tools

mvn clean compile package -DskipTests

read 

cd ./../MK/kite-dev/bin

#ls ./../../../kite-tools/target/

sudo rm dataset
cat dataset.sh ./../../../kite-tools/target//kite-tools-0.15.1-SNAPSHOT.jar >> dataset
chmod 777 dataset
./dataset

