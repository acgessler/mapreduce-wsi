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

Documentation
-------

TODO: link javadoc


Deployment using Tomcat 7
-------

- Build, export to WAR. Copy WAR to the `$TOMCAT/webapps` folder.
- Get JAX-WS RI dependencies from http://jax-ws.java.net/ and copy all jars from `lib` to `$TOMCAT/lib`
- (Re)start Tomcat
- Navigate to `http://localhost:8080/mapreduce-wsi/mapreduce` to verify deployment.

`http://localhost:8080/mapreduce-wsi/mapreduce?wsdl` to retrieve the service WSDL.
