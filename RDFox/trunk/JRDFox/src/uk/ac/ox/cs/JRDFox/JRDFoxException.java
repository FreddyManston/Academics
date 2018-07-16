// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox;

public class JRDFoxException extends Exception {
	private static final long serialVersionUID = 3446980191962360600L;

	public JRDFoxException() {
		super();
	}
	
	public JRDFoxException(String message) {
		super(message);
	}
	
	public JRDFoxException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
