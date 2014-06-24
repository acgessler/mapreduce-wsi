package de.uni_stuttgart.ipvs_as;

import java.util.Properties;
import java.util.Random;

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
 * part of a hadoop cluster but has password-less (user name hdfsuser) SSH
 * access to a *nix cluster node with the following clients installed:
 * 
 * <ul>
 * <li>sqoop
 * <li>yarn
 * </ul>
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

	@Override
	public void importIntoHDFS(long scopeId, String jdbcURI, String dbName,
			String dbUser, String dbCredentials, String query,
			String destinationName) throws MapReduceWSIException {
		// TODO
	}

	@Override
	public void exportToHDFS(long scopeId, String jdbcURI, String dbName,
			String dbUser, String dbCredentials, String query,
			String destinationName) throws MapReduceWSIException {
		// TODO
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
