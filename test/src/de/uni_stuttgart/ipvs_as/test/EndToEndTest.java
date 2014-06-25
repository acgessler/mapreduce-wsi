package de.uni_stuttgart.ipvs_as.test;

import java.io.File;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import de.uni_stuttgart.ipvs_as.MapReduceWSIException;
import de.uni_stuttgart.ipvs_as.MapReduceWSI;

/**
 * End-to-end (integration) test for MapReduceWSI: populates a local PostgreSQL
 * database with tuples, then exports them to HDFS via MapReduceWSI. It then
 * runs a JAR that contains a simple MR app which performs a computation on the
 * values. Results are written back to HDFS from where they are imported into
 * PostgreSQL again to verify the result.
 * 
 * As a prerequisite, ensure PostgreSQL is running and the credentials as
 * configured in constants below work. Also, PostgreSQL must be accessible from
 * within the hadoop cluster.
 * 
 * Before running you should also review the constants in the class, in
 * particular {@link DB_URI}.
 * 
 * The JAR containing the executable MR code is currently checked into source
 * control as test/integration_test_mapreduce_bundle.jar. It is built from the
 * {@link de.uni_stuttgart.ipvs_as.test.mapreduce} package.
 * <hr>
 * <b>TEST OUTLINE: </b>
 * 
 * The initial DB import adds 14k tuples of the following form:
 * 
 * <pre>
 *    id num0 ... num6
 * </pre>
 * 
 * where the "num"-columns are initialized such that the mean value of each
 * column is 3. The mapper then emits
 * 
 * <pre>
 * (0, num0), (1, num1), (2, num2) ..
 * </pre>
 * 
 * and the reducer calculates the mean of each of the 7 data columns.
 * 
 * These 7 tuples are exported back into the RDBMS and verified to be 3. Since
 * all arithmetic happens in integers, the result must be an exact match.
 * 
 * @author acg
 */
public class EndToEndTest {

	public static final String WSDL_PATH = "http://localhost:8080/mapreduce-wsi/mapreduce?wsdl";
	public static final String SERVICE_SCOPE = "http://ipvs_as.uni_stuttgart.de/";
	public static final String SERVICE_NAME = "MapReduceWSIImplService";

	// Change as needed
	// localhost does not work here: the DB is also accessed from within the
	// hadoop cluster.
	public static String DB_URI = "jdbc:postgresql://10.0.0.8:5432/";

	public static final String DB_USER = "postgres";
	public static final String DB_PW = "postgres";
	public static final String DB_NAME = "postgres";

	public static final String DB_INPUT_TABLE_NAME = "mapreduce_wsi_end2end_test_input";
	public static final String DB_OUTPUT_TABLE_NAME = "mapreduce_wsi_end2end_test_output";

	public static final String HDFS_INPUT_NAME = "mr_input";
	public static final String HDFS_OUTPUT_NAME = "mr_output";

	public static final String PREBUILT_MAPREDUCE_JAR = "test" + File.separator
			+ "integration_test_mapreduce_bundle.jar";

	public static final int COUNT_INPUT_TUPLES = 14000; /* Must be multiple of 7 */

	public void run() throws Exception {
		// Populate the DB with all table schemata and synthetic inputs
		initDBContents();

		// Dynamically connect to MapReduce-WSI (to avoid wsimport)
		URL url = new URL(WSDL_PATH);
		QName qname = new QName(SERVICE_SCOPE, SERVICE_NAME);
		Service service = Service.create(url, qname);
		MapReduceWSI port = service.getPort(MapReduceWSI.class);

		long scope;
		try {
			// Create a new MapReduce-WSI scope
			scope = port.createScope();

			// Import data into HDFS, discard the primary key
			// (This verifies correct filtering)
			final String importQuery = String
					.format("SELECT num0, num1, num2, num3, num4, num5, num6 FROM %s",
							DB_INPUT_TABLE_NAME);
			port.importIntoHDFS(scope, DB_URI, DB_USER, DB_PW, importQuery,
					DB_INPUT_TABLE_NAME + ".id", HDFS_INPUT_NAME);

			// Run MR with the pre-compiled JAR
			final String absolutePathToSourceJar = (new File(
					PREBUILT_MAPREDUCE_JAR)).getAbsolutePath();
			port.runMapReduce(scope, absolutePathToSourceJar, new String[] {
					HDFS_INPUT_NAME, HDFS_OUTPUT_NAME });

			// port.exportToHDFS(scopeId, jdbcURI, dbName, dbUser,
			// dbCredentials, query, destinationName)

			// port.deleteScope(scope);
		} catch (MapReduceWSIException e) {

			e.printStackTrace();
		}
	}

	public void initDBContents() throws Exception {
		final Connection conn = this.openDBConnection();
		final Statement stat = conn.createStatement();

		// Clear input and output table
		try {
			stat.execute(String.format("DROP TABLE %s;", DB_INPUT_TABLE_NAME));
			stat.execute(String.format("DROP TABLE %s;", DB_OUTPUT_TABLE_NAME));
		} catch (SQLException e) {
			// Fine for the first time.
			e.printStackTrace();
			System.out.println("This can be ignored if the End2EndTest is run"
					+ " for the first time or the DB has been cleared.");
		}

		// (Re)create and set schema for input and output table
		stat.execute(String.format("CREATE TABLE %s ("
				+ "id SERIAL PRIMARY KEY," + "num0 INT," + "num1 INT,"
				+ "num2 INT," + "num3 INT," + "num4 INT," + "num5 INT,"
				+ "num6 INT" + ");", DB_INPUT_TABLE_NAME));
		stat.execute(String.format("CREATE TABLE %s (" + "id INT, mean INT"
				+ ");", DB_OUTPUT_TABLE_NAME));

		// Populate the input table in larger batches
		assert COUNT_INPUT_TUPLES % 7 == 0;
		final int BATCH_SIZE = 100;
		for (int i = 0; i < COUNT_INPUT_TUPLES / BATCH_SIZE; ++i) {
			final int thisBatchSize = Math.min(BATCH_SIZE, COUNT_INPUT_TUPLES
					- i * BATCH_SIZE);
			final StringBuilder query = new StringBuilder();
			query.append("INSERT INTO ");
			query.append(DB_INPUT_TABLE_NAME);
			query.append(" VALUES");
			for (int j = 0; j < thisBatchSize; ++j) {
				if (j != 0) {
					query.append(",");
				}

				final int base = i * BATCH_SIZE + j;
				query.append(String.format(
						"(DEFAULT, %s, %s, %s, %s, %s, %s, %s)",
						(base + 0) % 7, (base + 1) % 7, (base + 2) % 7,
						(base + 3) % 7, (base + 4) % 7, (base + 5) % 7,
						(base + 6) % 7));
			}
			query.append(";");
			stat.execute(query.toString());
		}

		stat.close();
		conn.close();
	}

	public Connection openDBConnection() throws Exception {
		return DriverManager.getConnection(DB_URI, DB_USER, DB_PW);
	}

	public static void main(String[] arguments) throws Exception {
		(new EndToEndTest()).run();
	}
}
