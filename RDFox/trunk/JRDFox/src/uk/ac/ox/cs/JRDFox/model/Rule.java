// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import uk.ac.ox.cs.JRDFox.Prefixes;

public class Rule implements Serializable {
	private static final long serialVersionUID = -4309228479749130507L;

	protected final Atom[] m_head;
	protected final Atom[] m_body;

	protected Rule(Atom[] head, Atom[] body) {
		m_head = head.clone();
		m_body = body.clone();
	}

    public int getNumberOfHeadAtoms() {
        return m_head.length;
    }

    public Atom getHeadAtom(int index) {
        return m_head[index];
    }

    public Atom[] getHeadAtoms() {
        return m_head.clone();
    }

    public int getNumberOfBodyAtoms() {
        return m_body.length;
    }

    public Atom getBodyAtom(int index) {
        return m_body[index];
    }

    public Atom[] getBodyAtoms() {
        return m_body.clone();
    }

	public Rule replaceHead(Atom newHeadAtom) {
		return create(newHeadAtom, m_body);
	}

    public Rule applySubstitution(Map<Variable, Term> substitution) {
        Atom[] head = new Atom[m_head.length];
        for (int index = 0;index < m_head.length;index++)
            head[index] = m_head[index].applySubstitution(substitution);
        Atom[] body = new Atom[m_body.length];
        for (int index = 0;index < m_body.length;index++)
            body[index] = m_body[index].applySubstitution(substitution);
        return Rule.create(head, body);
    }

	public Rule simplify() {
		Set<Atom> newHead = new LinkedHashSet<Atom>();
		for (Atom atom : m_head)
			newHead.add(atom.simplify());
		Set<Atom> newBody = new LinkedHashSet<Atom>();
		for (Atom atom : m_body) {
			atom = atom.simplify();
			if (Atom.FALSE.equals(atom))
				return null;
			else
				newBody.add(atom);
		}
		return create(newHead.toArray(new Atom[newHead.size()]), newBody.toArray(new Atom[newBody.size()]));
	}

    public boolean isSafe() {
        Set<Variable> bodyVariables = new LinkedHashSet<Variable>();
        for (Atom atom : m_body) {
            for (int index = atom.getNumberOfArguments() - 1;index >= 0;--index) {
                Term argument = atom.getArgument(index);
                if (argument instanceof Variable)
                    bodyVariables.add((Variable)argument);
            }
        }
        for (Atom head : m_head) {
            for (int index = head.getNumberOfArguments() - 1;index >= 0;--index) {
                Term argument = head.getArgument(index);
                if (argument instanceof Variable && !bodyVariables.contains(argument))
                    return false;
            }
        }
        return true;
    }

	public void print(PrintWriter output, Prefixes prefixes) {
		for (int index = 0; index < m_head.length; index++) {
			if (index > 0)
				output.print(", ");
			m_head[index].print(output, prefixes);
		}
		output.print(" :- ");
		for (int index = 0; index < m_body.length; index++) {
			if (index > 0)
				output.print(", ");
			m_body[index].print(output, prefixes);
		}
		output.append(" .");
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder, Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
		return builder.toString();
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		for (int index = 0; index < m_head.length; index++) {
			if (index > 0)
				builder.append(", ");
			m_head[index].toString(builder, prefixes);
		}
		builder.append(" :- ");
		for (int index = 0; index < m_body.length; index++) {
			if (index > 0)
				builder.append(", ");
			m_body[index].toString(builder, prefixes);
		}
		builder.append(" .");
	}

	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

    protected static final InterningManager<Rule> s_interningManager = new InterningManager<Rule>() {
        protected boolean equal(Rule object1, Rule object2) {
            if (object1.m_head != object2.m_head || object1.m_body.length != object2.m_body.length)
                return false;
            for (int index = object1.m_body.length - 1;index >= 0;--index)
                if (object1.m_body[index] != object2.m_body[index])
                    return false;
            return true;
        }
        protected int getHashCode(Rule object) {
            int hashCode = object.m_head.hashCode();
            for (int index = object.m_body.length - 1;index >= 0;--index)
                hashCode = hashCode * 7 + object.m_body[index].hashCode();
            return hashCode;
        }
    };

    public static Rule create(Atom head, Atom... body) {
        return s_interningManager.intern(new Rule(new Atom[] { head }, body));
    }

    public static Rule create(Atom[] head, Atom[] body) {
        return s_interningManager.intern(new Rule(head, body));
    }
}
