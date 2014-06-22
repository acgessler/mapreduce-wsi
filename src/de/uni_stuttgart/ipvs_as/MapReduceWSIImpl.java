package de.uni_stuttgart.ipvs_as;

import javax.jws.WebService;

@WebService(endpointInterface = "de.uni_stuttgart.ipvs_as.MapReduceWSI")
public class MapReduceWSIImpl implements MapReduceWSI {

	@Override
	public long createScope() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void deleteScope(long scopeId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void runMapReduce(long scopeId, String srcJarName, String[] arguments) {
		// TODO Auto-generated method stub

	}

	@Override
	public void importIntoHDFS(long scopeId, String jdbcURI, String dbName,
			String dbUser, String dbCredentials, String query,
			String destinationName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void exportToHDFS(long scopeId, String jdbcURI, String dbName,
			String dbUser, String dbCredentials, String query,
			String destinationName) {
		// TODO Auto-generated method stub

	}

}
