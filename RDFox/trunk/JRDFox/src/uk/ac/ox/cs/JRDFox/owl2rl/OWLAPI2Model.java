// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLAnnotationSubject;
import org.semanticweb.owlapi.model.OWLAnnotationValue;
import org.semanticweb.owlapi.model.OWLAnonymousIndividual;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyExpression;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectInverseOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;

import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.Datatype;
import uk.ac.ox.cs.JRDFox.model.GroundTerm;
import uk.ac.ox.cs.JRDFox.model.Individual;
import uk.ac.ox.cs.JRDFox.model.Literal;
import uk.ac.ox.cs.JRDFox.model.Predicate;
import uk.ac.ox.cs.JRDFox.model.Term;

public class OWLAPI2Model {
	public static Predicate S(OWLClass owlClass) {
		return Predicate.create(owlClass.getIRI().toString());
	}

	public static Predicate S(OWLObjectProperty objectProperty) {
		return Predicate.create(objectProperty.getIRI().toString());
	}

	public static Predicate S(OWLDataProperty dataProperty) {
		return Predicate.create(dataProperty.getIRI().toString());
	}

	public static Predicate S(OWLAnnotationProperty annotationProperty) {
		return Predicate.create(annotationProperty.getIRI().toString());
	}

	public static Individual S(OWLIndividual individual) {
		if (individual instanceof OWLAnonymousIndividual)
			return Individual.create(((OWLAnonymousIndividual)individual).getID().toString());
		else
			return Individual.create(((OWLNamedIndividual)individual).getIRI().toString());
	}

	public static Literal S(OWLLiteral literal) {
		Datatype datatype = Datatype.value(literal.getDatatype().getIRI().toString());
		if (literal.isRDFPlainLiteral())
			return Literal.create(literal.getLiteral() + '@' + literal.getLang(), datatype);
		else
			return Literal.create(literal.getLiteral(), datatype);
	}

	public static Individual S(OWLAnnotationSubject annotationSubject) {
		if (annotationSubject instanceof IRI)
			return Individual.create(((IRI)annotationSubject).toString());
		else
			return S((OWLIndividual)annotationSubject);
	}

	public static GroundTerm S(OWLAnnotationValue annotationValue) {
		if (annotationValue instanceof IRI)
			return Individual.create(((IRI)annotationValue).toString());
		else if (annotationValue instanceof OWLLiteral)
			return S((OWLLiteral)annotationValue);
		else
			return S((OWLIndividual)annotationValue);
	}

	public static Individual E(OWLEntity entity) {
		return Individual.create(entity.getIRI().toString());
	}

	public static Atom A(OWLObjectPropertyExpression objectPropertyExpression, Term argument0, Term argument1) {
		if (objectPropertyExpression instanceof OWLObjectProperty)
			return Atom.create(S((OWLObjectProperty)objectPropertyExpression), argument0, argument1);
		else
			return A(((OWLObjectInverseOf)objectPropertyExpression).getInverse(), argument1, argument0);
	}

	public static Atom A(OWLDataPropertyExpression dataPropertyExpression, Term argument0, Term argument1) {
		return Atom.create(S(dataPropertyExpression.asOWLDataProperty()), argument0, argument1);
	}

	public static Atom A(OWLAnnotationProperty annotationProperty, Term argument0, Term argument1) {
		return Atom.create(S(annotationProperty), argument0, argument1);
	}
}
