#!/bin/bash

cd /opt/spark/sbin
./start-slave.sh -m 5000M -c 2 spark://172.25.0.9:7077
#./start-slave.sh -m 1500M -c 1 spark://172.25.0.9:7077

tail -f /dev/null