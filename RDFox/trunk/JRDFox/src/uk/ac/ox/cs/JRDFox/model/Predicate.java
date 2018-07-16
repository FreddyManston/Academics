// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;

import uk.ac.ox.cs.JRDFox.Namespaces;
import uk.ac.ox.cs.JRDFox.Prefixes;

public class Predicate implements Serializable {
	private static final long serialVersionUID = -4106426797562502177L;

	protected final String m_IRIPrefix;
	protected final String m_IRIRest;

	protected Predicate(String iriPrefix, String iriRest) {
		String iriPrefixInterned = Individual.s_IRIPrefixes.putIfAbsent(iriPrefix, iriPrefix);
		m_IRIPrefix = (iriPrefixInterned == null ? iriPrefix : iriPrefixInterned);
		m_IRIRest = iriRest;
	}

	public String getIRI() {
		return m_IRIPrefix + m_IRIRest;
	}

	public void print(PrintWriter output, Prefixes prefixes) {
		output.print(prefixes.abbreviateIRI(getIRI()));
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder, Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
		return builder.toString();
	}
 
	public void toString(StringBuilder builder, Prefixes prefixes) {
		builder.append(prefixes.abbreviateIRI(getIRI()));
	}

	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

	protected static final InterningManager<Predicate> s_interningManager = new InterningManager<Predicate>() {
		protected boolean equal(Predicate object1, Predicate object2) {
			return object1.m_IRIPrefix.equals(object2.m_IRIPrefix) && object1.m_IRIRest.equals(object2.m_IRIRest);
		}

		protected int getHashCode(Predicate object) {
			return object.m_IRIPrefix.hashCode() * 7 + object.m_IRIRest.hashCode();
		}
	};

	public static Predicate create(String iri) {
		int splitPoint = iri.lastIndexOf('#');
		if (splitPoint == -1)
			splitPoint = iri.lastIndexOf('/');
		String iriPrefix = iri.substring(0, splitPoint + 1);
		String iriRest = iri.substring(splitPoint + 1);
		return s_interningManager.intern(new Predicate(iriPrefix, iriRest));
	}

	public static final Predicate INTERNAL_RDF = create("internal:rdf");
	public static final Predicate SAME_AS = create(Namespaces.OWL_NS + "sameAs");
	public static final Predicate DIFFERENT_FROM = create(Namespaces.OWL_NS + "differentFrom");
	public static final Predicate THING = create(Namespaces.OWL_NS + "Thing");
	public static final Predicate NOTHING = create(Namespaces.OWL_NS + "Nothing");
	public static final Predicate RDFS_LITERAL = create(Namespaces.RDFS_NS + "Literal");
	public static final Predicate RDF_TYPE = Predicate.create(Namespaces.RDF_NS + "type");
    public static final Predicate SKOLEM = create("SKOLEM");
}
