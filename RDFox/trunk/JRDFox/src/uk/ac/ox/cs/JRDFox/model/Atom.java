// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map;

import uk.ac.ox.cs.JRDFox.Prefixes;

public class Atom implements Serializable {
	private static final long serialVersionUID = -6432381113892460896L;

	protected final Predicate m_predicate;
	protected final Term[] m_arguments;

	protected Atom(Predicate predicate, Term[] arguments) {
		m_predicate = predicate;
		m_arguments = arguments.clone();
	}

	public Predicate getPredicate() {
		return m_predicate;
	}

	public int getNumberOfArguments() {
		return m_arguments.length;
	}

	public Term getArgument(int index) {
		return m_arguments[index];
	}

	public Term[] getArguments() {
		return m_arguments.clone();
	}

	public void getArguments(Term[] arguments) {
		System.arraycopy(m_arguments, 0, arguments, 0, m_arguments.length);
	}

	public boolean isGround() {
		for (int index = m_arguments.length - 1; index >= 0; --index)
			if (!m_arguments[index].isGround())
				return false;
		return true;
	}

	public Atom replacePredicate(Predicate newPredicate) {
		return create(newPredicate, m_arguments);
	}

	public Atom applySubstitution(Map<Variable, ? extends Term> substitution) {
		if (substitution.isEmpty())
			return this;
		else {
			Term[] arguments = new Term[m_arguments.length];
			for (int index = 0; index < m_arguments.length; index++)
				arguments[index] = m_arguments[index].applySubstitution(substitution);
			return Atom.create(m_predicate, arguments);
		}
	}

	public Atom simplify() {
		if (Predicate.NOTHING.equals(m_predicate))
			return Atom.FALSE;
		else
			return this;
	}

	public void print(PrintWriter output, Prefixes prefixes) {
		if (Predicate.SKOLEM.equals(m_predicate) && m_arguments.length >= 2 && (m_arguments[1] instanceof Literal)) {
			output.print("BIND(SKOLEM(");
			m_arguments[1].print(output, prefixes);
			for (int index = 2; index < m_arguments.length; ++index) {
				output.print(',');
				m_arguments[index].print(output, prefixes);
			}
			output.print(") AS ");
			m_arguments[0].print(output, prefixes);
			output.print(')');
		}
		else {
			m_predicate.print(output, prefixes);
			if (m_arguments.length > 0) {
				output.print('(');
				for (int index = 0; index < m_arguments.length; index++) {
					if (index > 0)
						output.print(',');
					m_arguments[index].print(output, prefixes);
				}
				output.print(')');
			}
		}
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder, Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
		return builder.toString();
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		if (Predicate.SKOLEM.equals(m_predicate) && m_arguments.length >= 2 && (m_arguments[1] instanceof Literal)) {
			builder.append("BIND(SKOLEM(");
			m_arguments[1].toString(builder, prefixes);
			for (int index = 2; index < m_arguments.length; ++index) {
				builder.append(',');
				m_arguments[index].toString(builder, prefixes);
			}
			builder.append(") AS ");
			m_arguments[0].toString(builder, prefixes);
			builder.append(')');
		}
		else {
			m_predicate.toString(builder, prefixes);
			if (m_arguments.length > 0) {
				builder.append('(');
				for (int index = 0; index < m_arguments.length; index++) {
					if (index > 0)
						builder.append(',');
					m_arguments[index].toString(builder, prefixes);
				}
				builder.append(')');
			}
		}
	}

	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

	protected static final InterningManager<Atom> s_interningManager = new InterningManager<Atom>() {
		protected boolean equal(Atom object1, Atom object2) {
			if (object1.m_predicate != object2.m_predicate || object1.m_arguments.length != object2.m_arguments.length)
				return false;
			for (int index = object1.m_arguments.length - 1; index >= 0; --index)
				if (object1.m_arguments[index] != object2.m_arguments[index])
					return false;
			return true;
		}

		protected int getHashCode(Atom object) {
			int hashCode = object.m_predicate.hashCode();
			for (int index = object.m_arguments.length - 1; index >= 0; --index)
				hashCode = hashCode * 7 + object.m_arguments[index].hashCode();
			return hashCode;
		}
	};

	public static Atom create(Predicate predicate, Term... arguments) {
		return s_interningManager.intern(new Atom(predicate, arguments));
	}

	public static final Atom FALSE = create(Predicate.NOTHING, Individual.FALSE);
}
