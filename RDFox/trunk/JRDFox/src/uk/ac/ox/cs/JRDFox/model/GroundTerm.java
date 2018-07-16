// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.util.Map;

public abstract class GroundTerm extends Term {
	
	protected Datatype m_datatype;

	public Datatype getDatatype() {
		return m_datatype;
	}
	
	public boolean isGround() {
		return true;
	}

	public Term applySubstitution(Map<Variable, ? extends Term> substitution) {
		return this;
	}
}
