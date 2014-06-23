mapreduce-wsi
=============

MapReduceWSI is a web service end point that exposes subsets of the MR functionality of a Hadoop 2.x (YARN) clusters to clients.

 Clients can

 - Import structured data from RDBMS into HDFS
 - Export structured data from HDFS to RDBM
 - Deploy custom MapReduce code onto the cluster (as JAR) and run it (If
  additional resources are required for the MR, they should be embedded into
   the JAR.)

Clients are isolated against each other only on a filesystem (HDFS) level, there is no mitigation or scheduling of compute loads.

Getting started
-------

TODO: link javadoc
