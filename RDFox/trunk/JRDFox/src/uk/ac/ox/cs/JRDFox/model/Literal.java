// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;

import uk.ac.ox.cs.JRDFox.Namespaces;
import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.store.Resource;

public class Literal extends GroundTerm implements Serializable {
	private static final long serialVersionUID = 3347513364700246887L;

	protected final String m_lexicalForm;
	protected final Datatype m_datatype;

	protected Literal(String lexicalForm, Datatype datatype) {
		m_lexicalForm = lexicalForm;
		m_datatype = datatype;
	}

	public String getLexicalForm() {
		return m_lexicalForm;
	}

	public Datatype getDatatype() {
		return m_datatype;
	}

	public void print(PrintWriter output, Prefixes prefixes) {
		Resource.print(output, prefixes, m_lexicalForm, m_datatype);
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		Resource.toString(builder, prefixes, m_lexicalForm, m_datatype);
	}

	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

	protected static final InterningManager<Literal> s_interningManager = new InterningManager<Literal>() {
		protected boolean equal(Literal object1, Literal object2) {
			return object1.m_lexicalForm.equals(object2.m_lexicalForm) && object1.m_datatype.getIRI().equals(object2.m_datatype.getIRI());
		}

		protected int getHashCode(Literal object) {
			return object.m_lexicalForm.hashCode() * 5 + object.m_datatype.getIRI().hashCode();
		}
	};

	public static Literal create(String lexicalForm, Datatype datatype) {
		return s_interningManager.intern(new Literal(lexicalForm, datatype));
	}

	public static final String XSD_STRING = Namespaces.XSD_NS + "string";
	public static final String RDF_PLAIN_LITERAL = Namespaces.RDF_NS + "PlainLiteral";
}
