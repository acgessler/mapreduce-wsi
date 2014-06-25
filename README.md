mapreduce-wsi
=============

MapReduceWSI is a JAX-WX web service that exposes subsets of the MR functionality of a Hadoop 2.x (YARN)
clusters to clients.

The service itself is intended to run on a server outside the cluster with SSH access
to a cluster node. However it is also possible to serve the web service directly from a cluster node.

 Clients of MapReduceWSI can

 - Import structured data from a RDBMS into HDFS
 - Export structured data from HDFS to a a RDBMS
 - Deploy a JAR containing custom MapReduce code onto the cluster and run it (If
   additional resources are required for the MR, they can be embedded into
   the JAR.)

Clients are weakly isolated against each other only on a filesystem (HDFS) level,
there is no mitigation or scheduling of compute loads.

### Documentation

JavaDoc is available [here](http://acgessler.github.io/mapreduce-wsi/doc/index.html).

A full example (actually, an integration test) can be found [here](https://github.com/acgessler/mapreduce-wsi/blob/master/test/src/de/uni_stuttgart/ipvs_as/test/EndToEndTest.java).

### Remote (Cluster) setup

The _remote host_ is any node in your hadoop cluster that has network connectivity to the MapReduceWSI service.
If the service is served from a node that is part of the cluster and satifies the requirements below,
configure the remote host to be `localhost`.

- On the remote host, make sure the `sqoop`, `yarn` and `hadoop` binaries are available
- On the remote host, execute `setup/hadoop_prepare.sh` under an account that can do password-less sudo
- Make sure the remote host allows password-only authentication for SSH.

### Deployment using Tomcat 7

- Update `WEB-INF/mapreduce-wsi-config.xml` with your remote host info
- Build, export to WAR. Copy WAR to the `$TOMCAT/webapps` folder.
- Get JAX-WS RI dependencies from http://jax-ws.java.net/ and copy all jars from `lib` to `$TOMCAT/lib`
- (Re)start Tomcat
- Navigate to `http://localhost:8080/mapreduce-wsi/mapreduce` to verify deployment.

### WSDL

Use `http://localhost:8080/mapreduce-wsi/mapreduce?wsdl` to retrieve the service WSDL.
