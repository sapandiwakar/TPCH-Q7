#!/bin/bash

hadoop fs -rm /user/cloudera/join/input/*

hadoop fs -put R.txt S.txt /user/cloudera/join/input