package de.uni_stuttgart.ipvs_as;

import java.util.Random;

import javax.jws.WebService;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
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

	private static volatile SSHExec sharedSSH;
	private static final Object sharedSSHMutex = new Object();

	@Override
	public long createScope() throws MapReduceWSIException {
		// Simply generate a random scope id - the likelihood
		// of collisions is sufficiently low.
		//
		// Otherwise, to really guarantee an unique scope we
		// would need an atomic counter on the cluster.
		final long scopeId = (new Random()).nextLong();

		// Create both HDFS and local folders
		try {
			execRemote("hadoop fs -mkdir " + getHDFSDir(scopeId));
			execRemote("mkdir " + getRemoteLocalDir(scopeId));
		} catch (MapReduceWSIException e) {
			throw new MapReduceWSIException("Failed to create scope", e);
		}
		return scopeId;
	}

	@Override
	public void deleteScope(long scopeId) throws MapReduceWSIException {

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

		try {
			copyToRemote(srcJarName, destName);
			execRemote("yarn jar " + destName);
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

	private static void execRemote(String command) throws MapReduceWSIException {
		initSSH();
		try {
			sharedSSH.exec(new ExecCommand(command));
		} catch (TaskExecFailException e) {
			e.printStackTrace();
			throw new MapReduceWSIException("Failed to execute remote command",
					e);
		}
	}

	private static void copyToRemote(String srcJarName, String destName)
			throws MapReduceWSIException {
		initSSH();
		try {
			sharedSSH.uploadSingleDataToServer(srcJarName, destName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new MapReduceWSIException(String.format(
					"Failed to copy source file %s to destination %s",
					srcJarName, destName), e);
		}
	}

	private static void initSSH() {
		if (sharedSSH == null) {
			synchronized (sharedSSHMutex) {
				// DCL is fine with JRE1.5+
				// http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html
				if (sharedSSH == null) {
					ConnBean cb = new ConnBean("ip ", "username", "password");
					sharedSSH = SSHExec.getInstance(cb);
					sharedSSH.connect();
				}
			}
		}

	}

	private static String getRemoteLocalDir(long scopeId) {
		return "~/mapreduce-wsi-local/" + scopeId;
	}

	private static String getHDFSDir(long scopeId) {
		return "/mapreduce-wsi/" + scopeId;
	}
}
