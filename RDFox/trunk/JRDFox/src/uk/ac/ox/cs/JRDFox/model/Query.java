// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.util.Map;

import uk.ac.ox.cs.JRDFox.Prefixes;

public interface Query {
	int getNumberOfAnswerValues();

	Query applySubstitution(Map<Variable, Term> substitution);

	Query simplify();

	boolean isSafe();

	void toString(StringBuilder builder, Prefixes prefixes);
}
