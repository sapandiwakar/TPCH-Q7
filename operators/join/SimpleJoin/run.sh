#!/bin/bash

hadoop fs -rmr /user/cloudera/join/output

hadoop jar join.jar Join /user/cloudera/join/input/R.txt 1 /user/cloudera/join/input/S.txt 0 /user/cloudera/join/output

hadoop fs -cat /user/cloudera/join/output/part-00000