package de.uni_stuttgart.ipvs_as;

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

		// Deploy the jar to the remote, then let yarn do the rest
		try {
			copyToRemote(srcJarName, destName);
			execRemote(sb.toString());
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException(
					"Failed to run MR remotely on yarn", e);
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

		// TODO(acg) Replace all of the following with a proper rewrite engine
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

		final String absoluteDestinationName = String.format("%s/%s",
				getHDFSDir(scopeId), destinationName);

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
	public void exportToHDFS(long scopeId, String jdbcURI, String dbUser,
			String dbCredentials, String tableName, String updateColumn,
			boolean allowInserts, String sourceName)
			throws MapReduceWSIException {
		// TODO
	}

	private String escapeShellArgument(String arg) {
		// TODO(acg): verify that this is sufficient to escape shell args
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
