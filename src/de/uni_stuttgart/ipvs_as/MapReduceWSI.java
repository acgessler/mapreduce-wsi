package de.uni_stuttgart.ipvs_as;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.jws.soap.SOAPBinding.Style;

/**
 * MapReduceWSI is a web service end point that exposes subsets of the MR
 * functionality of a Hadoop 2.x (YARN) clusters to clients.
 * 
 * Clients can
 * <ul>
 * <li>Import structured data from RDBMS into HDFS (backed by Apache sqoop)
 * <li>Export structured data from HDFS to RDBM (backed by Apache sqoop)
 * <li>Deploy custom MapReduce code onto the cluster (as JAR) and run it (If
 * additional resources are required for the MR, they should be embedded into
 * the JAR.)
 * </ul>
 * 
 * The service provides partial isolation of clients against each other. Each
 * client obtains an application scope ID by calling {@link #createScope}. This
 * ID is then used with other APIs to identify the scope within to run the
 * operation.
 * 
 * This WSI is technically stateless as to avoid that clients are forced to
 * implement HTTP cookies or otherwise fancy technology (such as WS-Addressing).
 * Naturally, there is state associated with the scope ID though.
 * 
 * While different scopes enjoy file system isolation, the computational
 * resources in the cluster are limited and simultaneously running jobs do
 * affect each other's runtime.
 * 
 * @author - acg
 * */
@WebService
@SOAPBinding(style = Style.RPC)
public interface MapReduceWSI {

	/**
	 * Creates an execution scope that is isolated from other scopes. Clients
	 * must call this method first and use the scope ID they obtain for any
	 * further API calls.
	 * 
	 * Furthermore, calls to {@link #createScope} should be matched with calls to
	 * {@link #deleteScope}
	 * */
	@WebMethod
	long createScope();

	/**
	 * Delete a scope previously created using {@link #createScope}
	 * 
	 * This permanently deletes all file system and other cluster resources
	 * utilized by that scope. The scope ID may not be used with any further API
	 * calls (unless re-issued by {@link #createScope}).
	 * */
	@WebMethod
	void deleteScope(long scopeId);

	/**
	 * Run a MR job from a given JAR archive.
	 * 
	 * This deploys the given JAR onto the cluster and executes the main()
	 * routine contained therein. From within the main, the client can use the
	 * YARN API to run a MR job.
	 * 
	 * The first argument passed to main is the absolute prefix of all HDFS
	 * imports made via {@link #importIntoHDFS}
	 * 
	 * @param arguments
	 *            Arguments to forward to the JARs main() (after the HDFS
	 *            prefix)
	 * @param srcJarName
	 *            Path to the (JDK 1.7-compatible) JAR on the source machine.
	 */
	@WebMethod
	void runMapReduce(long scopeId, String srcJarName, String[] arguments);

	/**
	 * Import tuples from a RDBMS into HDFS
	 * 
	 * @param jdbcURI
	 *            JDBC URI under which the source database is accessible
	 * @param query
	 *            Arbitrary SQL SELECT query to filter the input database
	 * @param destinationName
	 *            Destination name under which to store the data in HDFS. Note
	 *            that the actual physical HDFS path has a prefix that depends
	 *            on the scope. 
	 * @see runMapReduce
	 * */
	@WebMethod
	void importIntoHDFS(long scopeId, String jdbcURI, String dbName,
			String dbUser, String dbCredentials, String query,
			String destinationName);

	// TODO
	void exportToHDFS(long scopeId, String jdbcURI, String dbName,
			String dbUser, String dbCredentials, String query,
			String destinationName);
}
