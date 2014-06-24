package de.uni_stuttgart.ipvs_as;

public class MapReduceWSIException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5117788169530315446L;

	public MapReduceWSIException(String message, Throwable cause) {
		// Include the cause in the message string as it does not get
		// transmitted via SOAP
		super(String.format("%s (cause: %s)", message, cause.toString()), cause);
	}

	public MapReduceWSIException(String string) {
		super(string);
	}

}
