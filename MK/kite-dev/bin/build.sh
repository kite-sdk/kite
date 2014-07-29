#!/bin/bash

cd ./../../../kite/kite-tools
mvn clean compile package -DskipTests
cd ../../workspace/kite-dev/bin

#ls ./../../../kite/kite-tools/target/

sudo rm dataset
cat dataset.sh ./../../../kite/kite-tools/target//kite-tools-0.15.1-SNAPSHOT.jar >> dataset
chmod 777 dataset
./dataset
