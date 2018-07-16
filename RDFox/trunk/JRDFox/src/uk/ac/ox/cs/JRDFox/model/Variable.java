// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map;

import uk.ac.ox.cs.JRDFox.Prefixes;

public class Variable extends Term implements Serializable {
	private static final long serialVersionUID = 519850891163499708L;

	protected final String m_name;

	protected Variable(String name) {
		m_name = name;
	}

	public String getName() {
		return m_name;
	}

	public boolean isGround() {
		return false;
	}

	public Term applySubstitution(Map<Variable, ? extends Term> substitution) {
		Term substitutedTerm = substitution.get(this);
		if (substitutedTerm == null)
			return this;
		else
			return substitutedTerm;
	}
	
	public void print(PrintWriter writer, Prefixes prefixes) {
		writer.print('?');
		writer.print(getName());
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		builder.append('?');
		builder.append(getName());
	}
	
	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

	protected static final InterningManager<Variable> s_interningManager = new InterningManager<Variable>() {
		protected boolean equal(Variable object1, Variable object2) {
			return object1.m_name.equals(object2.m_name);
		}

		protected int getHashCode(Variable object) {
			return object.m_name.hashCode();
		}
	};

	public static Variable create(String name) {
		return s_interningManager.intern(new Variable(name));
	}
}
