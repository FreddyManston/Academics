// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import uk.ac.ox.cs.JRDFox.Namespaces;
import uk.ac.ox.cs.JRDFox.Prefixes;

public class Individual extends GroundTerm implements Serializable {
	private static final long serialVersionUID = -7766079076267308584L;

	protected final String m_IRIPrefix;
	protected final String m_IRIRest;

	protected Individual(String iriPrefix, String iriRest) {
		m_datatype = Datatype.IRI_REFERENCE;
		String iriPrefixInterned = s_IRIPrefixes.putIfAbsent(iriPrefix, iriPrefix);
		m_IRIPrefix = (iriPrefixInterned == null ? iriPrefix : iriPrefixInterned);
		m_IRIRest = iriRest;
	}

	public String getIRI() {
		return m_IRIPrefix + m_IRIRest;
	}

	public void print(PrintWriter output, Prefixes prefixes) {
		output.print(prefixes.abbreviateIRI(getIRI()));
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		builder.append(prefixes.abbreviateIRI(getIRI()));
	}

	protected Object readResolve() {
		return s_interningManager.intern(this);
	}

	protected static final ConcurrentHashMap<String, String> s_IRIPrefixes = new ConcurrentHashMap<String, String>();

	protected static final InterningManager<Individual> s_interningManager = new InterningManager<Individual>() {
		protected boolean equal(Individual object1, Individual object2) {
			return object1.m_IRIPrefix.equals(object2.m_IRIPrefix) && object1.m_IRIRest.equals(object2.m_IRIRest);
		}

		protected int getHashCode(Individual object) {
			return object.m_IRIPrefix.hashCode() * 7 + object.m_IRIRest.hashCode();
		}
	};

	public static Individual create(String iri) {
		int splitPoint = iri.lastIndexOf('#');
		if (splitPoint == -1)
			splitPoint = iri.lastIndexOf('/');
		String iriPrefix = iri.substring(0, splitPoint + 1);
		String iriRest = iri.substring(splitPoint + 1);
		return s_interningManager.intern(new Individual(iriPrefix, iriRest));
	}

	public static final Individual RDF_TYPE = create(Namespaces.RDF_NS + "type");
	public static final Individual THING = create(Namespaces.OWL_NS + "Thing");
	public static final Individual NOTHING = create(Namespaces.OWL_NS + "Nothing");
	public static final Individual SAME_AS = create(Namespaces.OWL_NS + "sameAs");
	public static final Individual DIFFERENT_FROM = create(Namespaces.OWL_NS + "differentFrom");
	public static final Individual FALSE = create("int$false");
}
