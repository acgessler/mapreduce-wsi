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


Cluster setup
-------

The _remote host_ is any node in your hadoop cluster that has network connectivity to the MapReduceWSI service.

- On the remote host, make sure the oozie, yarn and hadoop binaries are available
- On the remote host, execute `setup/hadoop_prepare.sh` under an account that can do password-less sudo
- Make sure the remote host allows password-only authentication for SSH.

Deployment using Tomcat 7
-------

- Update `WEB-INF/mapreduce-wsi-config.xml` with your remote host info
- Build, export to WAR. Copy WAR to the `$TOMCAT/webapps` folder.
- Get JAX-WS RI dependencies from http://jax-ws.java.net/ and copy all jars from `lib` to `$TOMCAT/lib`
- (Re)start Tomcat
- Navigate to `http://localhost:8080/mapreduce-wsi/mapreduce` to verify deployment.

`http://localhost:8080/mapreduce-wsi/mapreduce?wsdl` to retrieve the service WSDL.
