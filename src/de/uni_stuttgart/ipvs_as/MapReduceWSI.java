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
 * <li>Import structured data from RDBMS into HDFS (backed by Apache Sqoop)
 * <li>Export structured data from HDFS to RDBM (backed by Apache Sqoop)
 * <li>Deploy a JAR containing custom MapReduce code onto the cluster and run it
 * (If additional resources are required for the MR, they can be embedded into
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
 * @author acgessler
 * */
@WebService
@SOAPBinding(style = Style.RPC)
public interface MapReduceWSI {

	/**
	 * Creates an execution scope that is isolated from other scopes. Clients
	 * must call this method first and use the scope ID they obtain for any
	 * further API calls.
	 * 
	 * Furthermore, calls to {@link #createScope} should be matched with calls
	 * to {@link #deleteScope}
	 * */
	@WebMethod
	long createScope() throws MapReduceWSIException;

	/**
	 * Delete a scope previously created using {@link #createScope}
	 * 
	 * This permanently deletes all file system and other cluster resources
	 * utilized by that scope. The scope ID may not be used with any further API
	 * calls (unless re-issued by {@link #createScope}).
	 * */
	@WebMethod
	void deleteScope(long scopeId) throws MapReduceWSIException;

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
	void runMapReduce(long scopeId, String srcJarName, String[] arguments)
			throws MapReduceWSIException;

	/**
	 * Run a MR job from a given Streaming Mode Mapper and Reducer script.
	 * 
	 * This deploys both scripts (given as a string containing their full source
	 * code) onto the cluster and runs them as Streaming Mode MapReduce jobs.
	 * 
	 * @param mapperScript
	 *            Source code of the mapper script. See Hadoop reference for
	 *            examples. Must include a valid UNIX shebang.
	 * @param reducerScript
	 *            Source code of the reducer script. See Hadoop reference for
	 *            examples. Must include a valid UNIX shebang.
	 * @param input
	 *            HDFS input to run the MapReduce from. This can be a directory
	 *            or a single file name or a wildcard pattern.
	 * @param output
	 *            Output HDFS file name or directory.
	 * 
	 * @see <a href="http://hadoop.apache.org/docs/r1.2.1/streaming.html">Hadoop
	 *      Streaming Mode (external)</a>
	 */
	@WebMethod
	void runStreamingMapReduce(long scopeId, String mapperScript,
			String reducerScript, String input, String output)
			throws MapReduceWSIException;

	/**
	 * Import rows from a JDBC-compatible RDBMS into text files on HDFS.
	 * 
	 * @param jdbcURI
	 *            JDBC URI under which the source database is accessible. Note
	 *            that the source database is accessed from *within* the hadoop
	 *            cluster.
	 * @param query
	 *            SQL SELECT query fragment to filter the input database. Only
	 *            simple projections and filters are allowed. NO trailing
	 *            semicolons. WHERE clauses must put parentheses around any OR
	 *            clauses, i.e.
	 * 
	 *            <pre>
	 * SELECT ... FROM ... WHERE (a OR b)
	 * </pre>
	 * 
	 *            as to allow adding further AND clauses.
	 * @param partitionColumn
	 *            This column is used to partition the input data for parallel
	 *            import from multiple mappers. For best results, it should have
	 *            many distinct values that are uniformly distributed (i.e. a
	 *            plain AUTO INCREMENT / SERIAL column is perfect).
	 * 
	 *            The column name must be prefixed by the table name, i.e.
	 * 
	 *            <pre>
	 * a.id
	 * </pre>
	 * @param destinationName
	 *            Destination name under which to store the data in HDFS. Note
	 *            that the actual physical HDFS path has a prefix that depends
	 *            on the scope.
	 * @see runMapReduce
	 * */
	@WebMethod
	void importIntoHDFS(long scopeId, String jdbcURI, String dbUser,
			String dbCredentials, String query, String partitionColumn,
			String destinationName) throws MapReduceWSIException;

	/**
	 * Export rows from text files on HDFS to a JDBC-compatible RDBMS.
	 * 
	 * @param jdbcURI
	 *            JDBC URI under which the destination database is accessible.
	 *            Note that the destination database is accessed from *within*
	 *            the Hadoop cluster.
	 * 
	 * @param tableName
	 *            Name of a pre-existing table to insert into. The table must
	 *            allow insertion of all values from HDFS without violating any
	 *            primary key or other constraints.
	 * @param sourceName
	 *            Source name from which to get the data from HDFS, typically
	 *            the output of a previous MR invocation. Note that the actual
	 *            physical HDFS path has a prefix that depends on the scope.
	 * @throws MapReduceWSIException
	 */
	void exportToRDBMS(long scopeId, String jdbcURI, String dbUser,
			String dbCredentials, String tableName, String sourceName)
			throws MapReduceWSIException;
}
