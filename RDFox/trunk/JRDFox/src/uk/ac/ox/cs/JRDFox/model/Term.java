// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.util.Map;

import uk.ac.ox.cs.JRDFox.Prefixes;

public abstract class Term {
	public abstract boolean isGround();

	public abstract Term applySubstitution(Map<Variable, ? extends Term> substitution);

	public abstract void print(PrintWriter output, Prefixes prefixes);

	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder, Prefixes.EMPTY_IMMUTABLE_INSTANCE);
		return builder.toString();
	}
	
	public String toString(Prefixes prefixes) {
		StringBuilder builder = new StringBuilder();
		toString(builder, prefixes);
		return builder.toString();
	}

	public abstract void toString(StringBuilder builder, Prefixes prefixes);
}
