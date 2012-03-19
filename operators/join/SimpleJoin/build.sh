#!/bin/bash

javac -nowarn -classpath /usr/lib/hadoop-0.20/hadoop-0.20.2-cdh3u0-core.jar -d join_classes Join.java TextPair.java

jar cvf join.jar -C join_classes/ . 

rm -R output