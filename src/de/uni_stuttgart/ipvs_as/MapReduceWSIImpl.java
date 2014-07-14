package de.uni_stuttgart.ipvs_as;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.jws.WebService;
import javax.servlet.ServletContext;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.IOptionName;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.impl.ExecCommand;

/**
 * Implementation of {@link MapReduceWSI} that can be run on a node that is not
 * part of a hadoop cluster but has (password-based) SSH access to a *nix
 * cluster node with the following clients installed:
 * <hr>
 * <ul>
 * <li>sqoop
 * <li>hadoop
 * <li>yarn
 * </ul>
 * 
 * This implementation currently has massive drawbacks, rendering it unusable
 * for productions environments.
 * <ul>
 * <li>There is no real ACL-based isolation in HDFS. If MR jobs wish they can
 * delete each others data. All jobs execute as the same user.
 * <li>Authentication is generally password based and passwords are transmitted
 * in plain text or leaked into configuration and command lines.
 * <li>Lack of logging and proper error forwarding from inside the cluster to
 * users of the MapReduceWSI service.
 * </ul>
 * 
 * @author acgessler
 * */
@WebService(endpointInterface = "de.uni_stuttgart.ipvs_as.MapReduceWSI")
public class MapReduceWSIImpl implements MapReduceWSI {

	// TODO(acgessler) It would be nice to automatically locate the file
	// using the $HADOOP_HOME environment variable. In my (Ambari) setup
	// $HADOOP_HOME is not globally set though.
	private static final String HADOOP_STREAMING_JAR = "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar";

	@Resource
	private WebServiceContext context;

	@Override
	public long createScope() throws MapReduceWSIException {
		// Simply generate a random scope id - the likelihood
		// of collisions is sufficiently low.
		//
		// Otherwise, to really guarantee an unique scope we
		// would need an atomic counter on the cluster.
		final long scopeId = Math.abs((new Random()).nextLong());

		// Create both HDFS and local folders
		try {
			execRemote("hadoop fs -mkdir " + getHDFSDir(scopeId));
			execRemote("mkdir -p " + getRemoteLocalDir(scopeId));
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException("Failed to create scope", e);
		}
		return scopeId;
	}

	@Override
	public void deleteScope(long scopeId) throws MapReduceWSIException {
		// Delete both local and HDFS folders (+ contents)
		try {
			execRemote("hadoop fs -rmr " + getHDFSDir(scopeId));
			execRemote("rm -rf " + getRemoteLocalDir(scopeId));
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException("Failed to run clean up scope", e);
		}
	}

	@Override
	public void runMapReduce(long scopeId, String srcJarName, String[] arguments)
			throws MapReduceWSIException {

		final String destName = getRemoteLocalDir(scopeId)
				+ "/mapreduce_wsi_upload.jar";

		final StringBuilder sb = new StringBuilder();
		sb.append("yarn jar ");
		sb.append(destName);
		sb.append(' ');
		sb.append(getHDFSDir(scopeId));
		sb.append(' ');
		for (String arg : arguments) {
			sb.append(arg);
			sb.append(' ');
		}

		// Deploy the JAR to the remote, then let yarn do the rest
		try {
			copyToRemote(srcJarName, destName);
			execRemote(sb.toString());
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException(
					"Failed to run MR remotely on the cluster", e);
		}
	}

	// Write |contents| to a temporary file and returns the absolute path name
	// of the file. The file obtained must be deleted by the caller.
	private File writeToTemporaryFile(String contents) throws IOException {
		final File file = File.createTempFile("mapreduce_wsi_tmp", null);
		final FileWriter fout = new FileWriter(file);
		try {
			fout.write(contents);
		} catch (IOException ex) {
			throw ex;
		} finally {
			fout.close();
		}
		return file;
	}

	@Override
	public void runStreamingMapReduce(long scopeId, String mapperScript,
			String reducerScript, String input, String output)
			throws MapReduceWSIException {
		final String mapFileDestName = getRemoteLocalDir(scopeId)
				+ "/streaming_mapper";
		final String reduceFileDestName = getRemoteLocalDir(scopeId)
				+ "/streaming_reducer";

		// Deploy mapper and reducer script.
		// TODO(acgessler) Unfortunately, SSHXCUTE only has a utility
		// for uploading files from local disk so we have to dump them to a
		// temporary, local file first.
		try {
			final File mapFile = writeToTemporaryFile(mapperScript);
			final File reduceFile = writeToTemporaryFile(reducerScript);

			try {
				copyToRemote(mapFile.getAbsolutePath(), mapFileDestName);
				copyToRemote(reduceFile.getAbsolutePath(), reduceFileDestName);
			} catch (MapReduceWSIException e) {
				throw new MapReduceWSIException(
						"Failed to deploy Streaming Mode Mapper and Reducer script",
						e);
			} finally {
				// deleteOnExit() is not sufficient for a potentially
				// long-running web service.
				mapFile.delete();
				reduceFile.delete();
			}
		} catch (IOException e) {
			throw new MapReduceWSIException(
					"Failed to write Streaming Mode Mapper and Reducer script to (local) temporary files",
					e);
		}

		// Build the command line for running the Streaming MapReduce
		// http://hadoop.apache.org/docs/r1.2.1/streaming.html
		final String hdfsPrefix = getHDFSDir(scopeId) + "/";
		final StringBuilder sb = new StringBuilder();
		sb.append("hadoop jar ");
		sb.append(HADOOP_STREAMING_JAR);
		sb.append(' ');

		sb.append("-input ");
		sb.append(hdfsPrefix);
		sb.append(input);
		sb.append(' ');

		sb.append("-output ");
		sb.append(hdfsPrefix);
		sb.append(output);
		sb.append(' ');

		sb.append("-mapper ");
		sb.append(mapFileDestName);
		sb.append(' ');

		sb.append("-reducer ");
		sb.append(reduceFileDestName);
		sb.append(' ');

		// The -file causes the scripts to be deployed the cluster machines as a
		// part of job submission.
		sb.append("-file ");
		sb.append(mapFileDestName);
		sb.append(' ');

		sb.append("-file ");
		sb.append(reduceFileDestName);
		// sb.append(' ');

		// Run Streaming MapReduce
		try {
			execRemote(sb.toString());
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException(
					"Failed to run Streaming MR remotely on the cluster", e);
		}
	}

	// Pattern to decompose a simple SQL statement into its primary
	// constituents. The pattern is designed to ensure that the very last WHERE
	// is matched (if any).
	private static final Pattern selectPattern = Pattern.compile(
			"SELECT(.*?)FROM(.*?)(WHERE(?!.*WHERE).*|)$",
			Pattern.CASE_INSENSITIVE);

	@Override
	public void importIntoHDFS(long scopeId, String jdbcURI, String dbUser,
			String dbCredentials, String query, String partitionColumn,
			String destinationName) throws MapReduceWSIException {

		if (partitionColumn.indexOf('.') == -1) {
			throw new IllegalArgumentException(
					"|partitionColumn| must be prefixed by table");
		}

		// TODO(acgessler) Replace this with a proper rewrite engine
		final Matcher queryMatch = selectPattern.matcher(query);
		if (!queryMatch.find()) {
			throw new IllegalArgumentException("Unrecognized |query|");
		}

		// Sqoop requires WHERE/AND $CONDITIONS to be appended to the
		// query so it can insert the partition condition.
		final String fullQuery = String.format("%s %s $CONDITIONS", query,
				(queryMatch.group(3).toUpperCase().startsWith("WHERE") ? "AND"
						: "WHERE"));

		// If no --boundary-query is given, Sqoop does a
		// SELECT MIN(t1.<partitionColumn>), MAX(t1.<partitionColumn>)
		// on the result of |escapedQuery|, causing it to fail if
		// the selected columns do not include the partition column.
		//
		// Here we try to detect absence of the partition column in
		// the SELECT clause and build an appropriate --boundary-query.
		final String partitionColumnAfterPeriod = partitionColumn
				.substring(partitionColumn.indexOf(".") + 1);

		String boundaryQuery = query;
		if (!Pattern.matches("(\\b|\\.)" + partitionColumnAfterPeriod
				+ "\\b(?!\\s*AS)", queryMatch.group(1))) {
			boundaryQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s %s",
					partitionColumnAfterPeriod, partitionColumnAfterPeriod,
					queryMatch.group(2), queryMatch.group(3));
		}

		final String absoluteDestinationName = escapeShellArgument(String
				.format("%s/%s", getHDFSDir(scopeId), destinationName));

		final String escapedUserName = escapeShellArgument(dbUser);
		final String escapedPassword = escapeShellArgument(dbCredentials);
		final String escapedURI = escapeShellArgument(jdbcURI);
		final String escapedFullQuery = escapeShellArgument(fullQuery);
		final String escapedBoundaryQuery = escapeShellArgument(boundaryQuery);
		try {
			execRemote(String.format(
					"sqoop import --connect %s --username %s --password %s "
							+ "--query %s --target-dir %s --split-by %s "
							+ "--boundary-query %s", escapedURI,
					escapedUserName, escapedPassword, escapedFullQuery,
					absoluteDestinationName, partitionColumn,
					escapedBoundaryQuery));
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException(
					"Failed to run import into HDFS remotely using sqoop", e);
		}
	}

	@Override
	public void exportToRDBMS(long scopeId, String jdbcURI, String dbUser,
			String dbCredentials, String tableName, String sourceName)
			throws MapReduceWSIException {

		final String absoluteSourceName = escapeShellArgument(String.format(
				"%s/%s", getHDFSDir(scopeId), sourceName));

		final String escapedUserName = escapeShellArgument(dbUser);
		final String escapedPassword = escapeShellArgument(dbCredentials);
		final String escapedURI = escapeShellArgument(jdbcURI);
		final String escapedTableName = escapeShellArgument(tableName);

		try {
			execRemote(String
					.format("sqoop export --connect %s --username %s --password %s "
							+ "--table %s --export-dir %s --fields-terminated-by '\\t'",
							escapedURI, escapedUserName, escapedPassword,
							escapedTableName, absoluteSourceName));
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException(
					"Failed to run export to SQL remotely using sqoop", e);
		}
	}

	private String escapeShellArgument(String arg) {
		// TODO(acgessler): verify that this is sufficient to escape shell args
		return String.format("'%s'", arg.replace("'", "\\'"));
	}

	private String getRemoteLocalDir(long scopeId) {
		return String.format("%s/%s",
				getConfig().getProperty("remoteBaseLocalFolder"), scopeId);
	}

	private String getHDFSDir(long scopeId) {
		return String.format("%s/%s",
				getConfig().getProperty("remoteBaseHDFSFolder"), scopeId);
	}

	/**
	 * Execute a given command on the remote host. No further checking is
	 * performed on the command string.
	 * 
	 * @param command
	 * @throws MapReduceWSIException
	 */
	private void execRemote(String command) throws MapReduceWSIException {
		SSHExec sshConnection = initSSHConnection();
		try {
			// SSHXCUTE is not thread-safe
			synchronized (sshConnection) {
				sshConnection.exec(new ExecCommand(command));
			}
		} catch (TaskExecFailException e) {
			e.printStackTrace();
			throw new MapReduceWSIException("Failed to execute remote command",
					e);
		} finally {
			sshConnection.disconnect();
		}
	}

	/**
	 * Copy a file to the remote host. Given paths are unchanged and not
	 * checked.
	 * 
	 * @param srcJarName
	 * @param destName
	 * @throws MapReduceWSIException
	 */
	private void copyToRemote(String srcJarName, String destName)
			throws MapReduceWSIException {
		SSHExec sshConnection = initSSHConnection();
		try {
			// SSHXCUTE is not thread-safe
			synchronized (sshConnection) {
				sshConnection.uploadSingleDataToServer(srcJarName, destName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new MapReduceWSIException(String.format(
					"Failed to copy source file %s to destination %s",
					srcJarName, destName), e);
		} finally {
			sshConnection.disconnect();
		}
	}

	/**
	 * Obtain a SSH connection to the remote host that hosts hadoop.
	 * 
	 * @return SSH connection handle. The caller must close it using
	 *         {@link SSHExec.disconnect} after finishing to use it.
	 */
	private SSHExec initSSHConnection() throws MapReduceWSIException {
		Properties properties = getConfig();
		ConnBean cb = new ConnBean(properties.getProperty("remoteHost"),
				properties.getProperty("remoteUser"),
				properties.getProperty("remotePassword"));
		SSHExec sshConnection = SSHExec.getInstance(cb);
		if (!sshConnection.connect()) {
			throw new MapReduceWSIException("Failed to connect to remote host "
					+ properties.getProperty("remoteHost"));
		}
		SSHExec.setOption(IOptionName.INTEVAL_TIME_BETWEEN_TASKS, 0L);
		return sshConnection;
	}

	/** Get global mapreduce-wsi configuration */
	private Properties getConfig() {
		ServletContext servletContext = (ServletContext) context
				.getMessageContext().get(MessageContext.SERVLET_CONTEXT);
		Properties prop = (Properties) servletContext.getAttribute("config");

		assert prop != null;
		return prop;
	}
}
