// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;

import uk.ac.ox.cs.JRDFox.Prefixes;

public class BlankNode extends GroundTerm implements Serializable {
	private static final long serialVersionUID = 1011338637407853168L;

	protected final String m_id;

	protected BlankNode(String id) {
		m_datatype = Datatype.BLANK_NODE;
		m_id = id;
	}

	public String getID() {
		return m_id;
	}

	public void print(PrintWriter output, Prefixes prefixes) {
		output.print("_:");
		output.print(m_id);
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		builder.append("_:");
		builder.append(m_id);
	}

	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

	protected static final InterningManager<BlankNode> s_interningManager = new InterningManager<BlankNode>() {
		protected boolean equal(BlankNode object1, BlankNode object2) {
			return object1.m_id.equals(object2.m_id);
		}

		protected int getHashCode(BlankNode object) {
			return object.m_id.hashCode();
		}
	};

	public static BlankNode create(String id) {
		return s_interningManager.intern(new BlankNode(id));
	}
}
