// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLClassExpressionVisitorEx;
import org.semanticweb.owlapi.model.OWLDataAllValuesFrom;
import org.semanticweb.owlapi.model.OWLDataComplementOf;
import org.semanticweb.owlapi.model.OWLDataExactCardinality;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataIntersectionOf;
import org.semanticweb.owlapi.model.OWLDataMaxCardinality;
import org.semanticweb.owlapi.model.OWLDataMinCardinality;
import org.semanticweb.owlapi.model.OWLDataOneOf;
import org.semanticweb.owlapi.model.OWLDataRange;
import org.semanticweb.owlapi.model.OWLDataRangeVisitorEx;
import org.semanticweb.owlapi.model.OWLDataSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLDataUnionOf;
import org.semanticweb.owlapi.model.OWLDatatype;
import org.semanticweb.owlapi.model.OWLDatatypeRestriction;
import org.semanticweb.owlapi.model.OWLObjectAllValuesFrom;
import org.semanticweb.owlapi.model.OWLObjectComplementOf;
import org.semanticweb.owlapi.model.OWLObjectExactCardinality;
import org.semanticweb.owlapi.model.OWLObjectHasSelf;
import org.semanticweb.owlapi.model.OWLObjectHasValue;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectMaxCardinality;
import org.semanticweb.owlapi.model.OWLObjectMinCardinality;
import org.semanticweb.owlapi.model.OWLObjectOneOf;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLObjectUnionOf;

import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.Term;
import uk.ac.ox.cs.JRDFox.model.Variable;

public class SubclassProcessor implements OWLClassExpressionVisitorEx<BodyExpression>, OWLDataRangeVisitorEx<BodyExpression> {
	protected final OWLAxiom m_axiom;
	protected final Collection<JRDFoxException> m_errors;
	protected final VariableProducer m_variableProducer;
	protected Term m_forTerm;
	protected BodyExpression m_forBodyExpression;

	public SubclassProcessor(OWLAxiom axiom, Collection<JRDFoxException> errors, VariableProducer variableProducer) {
		m_axiom = axiom;
		m_errors = errors;
		m_variableProducer = variableProducer;
	}

	public BodyExpression getBodyExpressionFor(OWLClassExpression subClassExpression, Term forTerm, BodyExpression forBodyExpression) {
		Term currentForTerm = m_forTerm;
		BodyExpression currentForBodyExpression = m_forBodyExpression;
		m_forTerm = forTerm;
		m_forBodyExpression = forBodyExpression;
		try {
			return subClassExpression.accept(this);
		}
		finally {
			m_forTerm = currentForTerm;
			m_forBodyExpression = currentForBodyExpression;
		}
	}

	public BodyExpression getBodyExpressionFor(OWLDataRange subDataRange, Term forTerm, BodyExpression forBodyExpression) {
		Term currentForTerm = m_forTerm;
		BodyExpression currentForBodyExpression = m_forBodyExpression;
		m_forTerm = forTerm;
		m_forBodyExpression = forBodyExpression;
		try {
			return subDataRange.accept(this);
		}
		finally {
			m_forTerm = currentForTerm;
			m_forBodyExpression = currentForBodyExpression;
		}
	}

	protected void unsupportedClassExpression(OWLClassExpression classExpression) {
		if (m_errors != null)
			m_errors.add(new JRDFoxException("Expression '" + classExpression.toString() + "' cannot occur in axiom '" + m_axiom.toString() + "' in OWL 2 RL."));
	}

	protected void unsupportedDataRange(OWLDataRange dataRange) {
		if (m_errors != null)
			m_errors.add(new JRDFoxException("Expression '" + dataRange.toString() + "' cannot occur in axiom '" + m_axiom.toString() + "' in OWL 2 RL."));
	}

	// Class expressions
	public BodyExpression visit(OWLClass ce) {
		if (ce.isTopEntity()) {
			unsupportedClassExpression(ce);
			return null;
		}
		else
			return m_forBodyExpression.prepend(Atom.create(OWLAPI2Model.S(ce), m_forTerm));
	}

	public BodyExpression visit(OWLObjectIntersectionOf ce) {
		BodyExpression currentBodyExpression = m_forBodyExpression;
		Set<OWLClassExpression> operands = ce.getOperands();
		OWLClassExpression[] operandsArray = operands.toArray(new OWLClassExpression[operands.size()]);
		for (int index = operandsArray.length - 1; index >= 0; --index) {
			OWLClassExpression operand = operandsArray[index];
			currentBodyExpression = getBodyExpressionFor(operand, m_forTerm, currentBodyExpression);
			if (currentBodyExpression == null)
				return null;
		}
		return currentBodyExpression;
	}

	public BodyExpression visit(OWLObjectUnionOf ce) {
		List<BodyExpression> disjuncts = new ArrayList<BodyExpression>();
		for (OWLClassExpression operand : ce.getOperands()) {
			BodyExpression operandBody = getBodyExpressionFor(operand, m_forTerm, m_forBodyExpression);
			if (operandBody != null)
				disjuncts.add(operandBody);
		}
		if (disjuncts.isEmpty())
			return null;
		else if (disjuncts.size() == 1)
			return disjuncts.get(0);
		else
			return new BodyExpression.DisjunctiveBodyExpression(disjuncts.toArray(new BodyExpression[disjuncts.size()]));
	}

	public BodyExpression visit(OWLObjectComplementOf ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLObjectOneOf ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLObjectSomeValuesFrom ce) {
		BodyExpression currentBodyExpression = m_forBodyExpression;
		Variable newVariable = m_variableProducer.nextFreshVariable();
		if (!ce.getFiller().isTopEntity()) {
			currentBodyExpression = getBodyExpressionFor(ce.getFiller(), newVariable, currentBodyExpression);
			if (currentBodyExpression == null)
				return null;
		}
		Atom roleAtom = OWLAPI2Model.A(ce.getProperty(), m_forTerm, newVariable);
		return currentBodyExpression.prepend(roleAtom);
	}

	public BodyExpression visit(OWLObjectAllValuesFrom ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLObjectHasValue ce) {
		Atom roleAtom = OWLAPI2Model.A(ce.getProperty(), m_forTerm, OWLAPI2Model.S(ce.getValue()));
		return m_forBodyExpression.prepend(roleAtom);
	}

	public BodyExpression visit(OWLObjectHasSelf ce) {
		Atom roleAtom = OWLAPI2Model.A(ce.getProperty(), m_forTerm, m_forTerm);
		return m_forBodyExpression.prepend(roleAtom);
	}

	public BodyExpression visit(OWLObjectMinCardinality ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLObjectMaxCardinality ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLObjectExactCardinality ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLDataSomeValuesFrom ce) {
		BodyExpression currentBodyExpression = m_forBodyExpression;
		Variable newVariable = m_variableProducer.nextFreshVariable();
		if (!ce.getFiller().isTopEntity()) {
			currentBodyExpression = getBodyExpressionFor(ce.getFiller(), newVariable, currentBodyExpression);
			if (currentBodyExpression == null)
				return null;
		}
		Atom roleAtom = OWLAPI2Model.A(ce.getProperty(), m_forTerm, newVariable);
		return currentBodyExpression.prepend(roleAtom);
	}

	public BodyExpression visit(OWLDataAllValuesFrom ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLDataHasValue ce) {
		Atom roleAtom = OWLAPI2Model.A(ce.getProperty(), m_forTerm, OWLAPI2Model.S(ce.getValue()));
		return m_forBodyExpression.prepend(roleAtom);
	}

	public BodyExpression visit(OWLDataMinCardinality ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLDataMaxCardinality ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	public BodyExpression visit(OWLDataExactCardinality ce) {
		unsupportedClassExpression(ce);
		return null;
	}

	// Data ranges
	public BodyExpression visit(OWLDatatype dr) {
/*		Atom datatypeAtom = Atom.create(Predicate.create(Namespaces.SWRLB_NS + "inValueSpace", 2), m_forTerm, Individual.create(dr.getIRI().toString()));
		return m_forBodyExpression.prepend(datatypeAtom);*/
		unsupportedDataRange(dr);
		return null;
	}

	public BodyExpression visit(OWLDataIntersectionOf dr) {
		BodyExpression currentBodyExpression = m_forBodyExpression;
		Set<OWLDataRange> operands = dr.getOperands();
		OWLDataRange[] operandsArray = operands.toArray(new OWLDataRange[operands.size()]);
		for (int index = operandsArray.length - 1; index >= 0; --index) {
			OWLDataRange operand = operandsArray[index];
			currentBodyExpression = getBodyExpressionFor(operand, m_forTerm, currentBodyExpression);
			if (currentBodyExpression == null)
				return null;
		}
		return currentBodyExpression;
	}

	public BodyExpression visit(OWLDataUnionOf dr) {
		unsupportedDataRange(dr);
		return null;
	}

	public BodyExpression visit(OWLDataComplementOf dr) {
		unsupportedDataRange(dr);
		return null;
	}

	public BodyExpression visit(OWLDataOneOf dr) {
		unsupportedDataRange(dr);
		return null;
	}

	public BodyExpression visit(OWLDatatypeRestriction dr) {
/*		BodyExpression currentBodyExpression = m_forBodyExpression;
		Set<OWLFacetRestriction> facetRestrictions = dr.getFacetRestrictions();
		OWLFacetRestriction[] facetRestrictionsArray = facetRestrictions.toArray(new OWLFacetRestriction[facetRestrictions.size()]);
		for (int index = facetRestrictionsArray.length - 1; index >= 0; --index) {
			OWLFacetRestriction facetRestriction = facetRestrictionsArray[index];
			Literal facetValue = OWLAPI2Model.S(facetRestriction.getFacetValue());
			switch (facetRestriction.getFacet()) {
			case LENGTH:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "stringLength", 2), facetValue, m_forTerm));
				break;
			case MIN_LENGTH:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "stringMinLengthInclusive", 2), facetValue, m_forTerm));
				break;
			case MAX_LENGTH:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "stringMaxLengthInclusive", 2), facetValue, m_forTerm));
				break;
			case LANG_RANGE:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "languageTagMatches", 2), m_forTerm, facetValue));
				break;
			case PATTERN:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "matches", 2), m_forTerm, facetValue));
				break;
			case MIN_INCLUSIVE:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "greaterThanOrEqual", 2), m_forTerm, facetValue));
				break;
			case MIN_EXCLUSIVE:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "greaterThan", 2), m_forTerm, facetValue));
				break;
			case MAX_INCLUSIVE:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "lessThanOrEqual", 2), m_forTerm, facetValue));
				break;
			case MAX_EXCLUSIVE:
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "lessThan", 2), m_forTerm, facetValue));
				break;
			case TOTAL_DIGITS:
			case FRACTION_DIGITS:
				unsupportedDataRange(dr);
				break;
			}
		}
		OWLDatatype datatype = dr.getDatatype();
		Atom datatypeAtom = Atom.create(Predicate.create(Namespaces.SWRLB_NS + "inValueSpace", 2), m_forTerm, Individual.create(datatype.getIRI().toString()));
		return currentBodyExpression.prepend(datatypeAtom);*/
		unsupportedDataRange(dr);
		return null;
	}
}
