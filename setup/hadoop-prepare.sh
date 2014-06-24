#!/bin/sh
# Script to prepare the hadoop cluster
#
# Run this on a SUDO account on the node that mapreduce-wsi should SSH
# to (i.e. as configured in WEB-INF/mapreduce-wsi-config.xml)
#
# Before running, make sure the hadoop cluster is all set
# up and yarn, hadoop and sqoop are locally available.

useradd mapreduce_wsi
passwd

sudo su hdfs
useradd mapreduce_wsi
hadoop fs -mkdir /user/mapreduce_wsi
hadoop fs -chown mapreduce_wsi /user/mapreduce_wsi
