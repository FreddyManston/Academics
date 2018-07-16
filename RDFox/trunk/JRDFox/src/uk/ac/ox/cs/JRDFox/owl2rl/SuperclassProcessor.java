// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import java.util.Collection;

import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLClassExpressionVisitor;
import org.semanticweb.owlapi.model.OWLDataAllValuesFrom;
import org.semanticweb.owlapi.model.OWLDataComplementOf;
import org.semanticweb.owlapi.model.OWLDataExactCardinality;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataIntersectionOf;
import org.semanticweb.owlapi.model.OWLDataMaxCardinality;
import org.semanticweb.owlapi.model.OWLDataMinCardinality;
import org.semanticweb.owlapi.model.OWLDataOneOf;
import org.semanticweb.owlapi.model.OWLDataRange;
import org.semanticweb.owlapi.model.OWLDataRangeVisitor;
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
import uk.ac.ox.cs.JRDFox.model.Predicate;
import uk.ac.ox.cs.JRDFox.model.Term;
import uk.ac.ox.cs.JRDFox.model.Variable;

public class SuperclassProcessor implements OWLClassExpressionVisitor, OWLDataRangeVisitor {
	protected final RuleListener m_ruleListener;
	protected final OWLAxiom m_axiom;
	protected final Collection<JRDFoxException> m_errors;
	protected final VariableProducer m_variableProducer;
	protected Term m_forTerm;
	protected BodyExpression m_forBodyExpression;

	protected SuperclassProcessor(RuleListener ruleListener, OWLAxiom axiom, Collection<JRDFoxException> errors, VariableProducer variableProducer) {
		m_ruleListener = ruleListener;
		m_axiom = axiom;
		m_errors = errors;
		m_variableProducer = variableProducer;
	}

	public void generateRules(OWLClassExpression superClassExpression, Term forTerm, BodyExpression forBodyExpression) {
		Term currentForTerm = m_forTerm;
		BodyExpression currentForBodyExpression = m_forBodyExpression;
		m_forTerm = forTerm;
		m_forBodyExpression = forBodyExpression;
		try {
			superClassExpression.accept(this);
		}
		finally {
			m_forTerm = currentForTerm;
			m_forBodyExpression = currentForBodyExpression;
		}
	}

	public void generateRules(OWLDataRange superDataRange, Term forTerm, BodyExpression forBodyExpression) {
		Term currentForTerm = m_forTerm;
		BodyExpression currentForBodyExpression = m_forBodyExpression;
		m_forTerm = forTerm;
		m_forBodyExpression = forBodyExpression;
		try {
			superDataRange.accept(this);
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
	public void visit(OWLClass ce) {
		if (!ce.isTopEntity()) {
			Atom head = Atom.create(OWLAPI2Model.S(ce), m_forTerm);
			m_forBodyExpression.getRules(m_ruleListener, head);
		}
	}

	public void visit(OWLObjectIntersectionOf ce) {
		for (OWLClassExpression operand : ce.getOperands())
			generateRules(operand, m_forTerm, m_forBodyExpression);
	}

	public void visit(OWLObjectUnionOf ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLObjectComplementOf ce) {
		SubclassProcessor subclassTranslator = new SubclassProcessor(m_axiom, m_errors, m_variableProducer);
		BodyExpression currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getOperand(), m_forTerm, m_forBodyExpression);
		if (currentBodyExpression != null)
			currentBodyExpression.getRules(m_ruleListener, Atom.FALSE);
	}

	public void visit(OWLObjectOneOf ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLObjectSomeValuesFrom ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLObjectAllValuesFrom ce) {
		Variable newVariable = m_variableProducer.nextFreshVariable();
		BodyExpression currentBodyExpression = m_forBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, newVariable));
		generateRules(ce.getFiller(), newVariable, currentBodyExpression);
	}

	public void visit(OWLObjectHasValue ce) {
		Atom head = OWLAPI2Model.A(ce.getProperty(), m_forTerm, OWLAPI2Model.S(ce.getValue()));
		m_forBodyExpression.getRules(m_ruleListener, head);
	}

	public void visit(OWLObjectHasSelf ce) {
		Atom head = OWLAPI2Model.A(ce.getProperty(), m_forTerm, m_forTerm);
		m_forBodyExpression.getRules(m_ruleListener, head);
	}

	public void visit(OWLObjectMinCardinality ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLObjectMaxCardinality ce) {
		if (ce.getCardinality() == 0) {
			Variable Y = m_variableProducer.nextFreshVariable();
			BodyExpression currentBodyExpression = m_forBodyExpression;
			if (!ce.getFiller().isTopEntity()) {
				SubclassProcessor subclassTranslator = new SubclassProcessor(m_axiom, m_errors, m_variableProducer);
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getFiller(), Y, currentBodyExpression);
				if (currentBodyExpression == null)
					return;
			}
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, Y));
			currentBodyExpression.getRules(m_ruleListener, Atom.FALSE);
		}
		else if (ce.getCardinality() == 1) {
			Variable Y1 = m_variableProducer.nextFreshVariable();
			Variable Y2 = m_variableProducer.nextFreshVariable();
			BodyExpression currentBodyExpression = m_forBodyExpression;
			if (!ce.getFiller().isTopEntity()) {
				SubclassProcessor subclassTranslator = new SubclassProcessor(m_axiom, m_errors, m_variableProducer);
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getFiller(), Y1, currentBodyExpression);
				if (currentBodyExpression == null)
					return;
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getFiller(), Y2, currentBodyExpression);
				if (currentBodyExpression == null)
					return;
			}
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, Y1));
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, Y2));
			currentBodyExpression.getRules(m_ruleListener, Atom.create(Predicate.SAME_AS, Y1, Y2));
		}
		else
			unsupportedClassExpression(ce);
	}

	public void visit(OWLObjectExactCardinality ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLDataSomeValuesFrom ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLDataAllValuesFrom ce) {
		Variable newVariable = m_variableProducer.nextFreshVariable();
		BodyExpression currentBodyExpression = m_forBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, newVariable));
		generateRules(ce.getFiller(), newVariable, currentBodyExpression);
	}

	public void visit(OWLDataHasValue ce) {
		Atom head = OWLAPI2Model.A(ce.getProperty(), m_forTerm, OWLAPI2Model.S(ce.getValue()));
		m_forBodyExpression.getRules(m_ruleListener, head);
	}

	public void visit(OWLDataMinCardinality ce) {
		unsupportedClassExpression(ce);
	}

	public void visit(OWLDataMaxCardinality ce) {
		if (ce.getCardinality() == 0) {
			Variable Y = m_variableProducer.nextFreshVariable();
			BodyExpression currentBodyExpression = m_forBodyExpression;
			if (!ce.getFiller().isTopEntity()) {
				SubclassProcessor subclassTranslator = new SubclassProcessor(m_axiom, m_errors, m_variableProducer);
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getFiller(), Y, currentBodyExpression);
				if (currentBodyExpression == null)
					return;
			}
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, Y));
			currentBodyExpression.getRules(m_ruleListener, Atom.FALSE);
		}
		else if (ce.getCardinality() == 1) {
			Variable Y1 = m_variableProducer.nextFreshVariable();
			Variable Y2 = m_variableProducer.nextFreshVariable();
			BodyExpression currentBodyExpression = m_forBodyExpression;
			if (!ce.getFiller().isTopEntity()) {
				SubclassProcessor subclassTranslator = new SubclassProcessor(m_axiom, m_errors, m_variableProducer);
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getFiller(), Y1, currentBodyExpression);
				if (currentBodyExpression == null)
					return;
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(ce.getFiller(), Y2, currentBodyExpression);
				if (currentBodyExpression == null)
					return;
			}
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, Y1));
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ce.getProperty(), m_forTerm, Y2));
			currentBodyExpression.getRules(m_ruleListener, Atom.create(Predicate.SAME_AS, Y1, Y2));
		}
		else
			unsupportedClassExpression(ce);
	}

	public void visit(OWLDataExactCardinality ce) {
		unsupportedClassExpression(ce);
	}

	// Data ranges
	public void visit(OWLDatatype dr) {
		unsupportedDataRange(dr);
/*		if (!dr.isTopDatatype()) {
			Atom atom = Atom.create(Predicate.create(Namespaces.SWRLB_NS + "notInValueSpace", 2), m_forTerm, OWLAPI2Model.E(dr));
			m_forBodyExpression.prepend(atom).getRules(m_ruleListener, Atom.FALSE);
		}*/
	}

	public void visit(OWLDataIntersectionOf dr) {
		for (OWLDataRange operand : dr.getOperands())
			generateRules(operand, m_forTerm, m_forBodyExpression);
	}

	public void visit(OWLDataUnionOf dr) {
		unsupportedDataRange(dr);
	}

	public void visit(OWLDataComplementOf dr) {
		unsupportedDataRange(dr);
	}

	public void visit(OWLDataOneOf dr) {
		unsupportedDataRange(dr);
	}

	public void visit(OWLDatatypeRestriction dr) {
		unsupportedDataRange(dr);
/*		Atom atom = Atom.create(Predicate.create(Namespaces.SWRLB_NS + "notInValueSpace", 2), m_forTerm, OWLAPI2Model.E(dr.getDatatype()));
		m_forBodyExpression.prepend(atom).getRules(m_ruleListener, Atom.FALSE);
		for (OWLFacetRestriction facetRestriction : dr.getFacetRestrictions()) {
			Literal facetValue = OWLAPI2Model.S(facetRestriction.getFacetValue());
			switch (facetRestriction.getFacet()) {
			case LENGTH:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "stringNotLength", 2), facetValue, m_forTerm)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case MIN_LENGTH:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "stringMaxLengthExclusive", 2), facetValue, m_forTerm)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case MAX_LENGTH:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "stringMinLengthExclusive", 2), facetValue, m_forTerm)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case LANG_RANGE:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "languageTagNotMatches", 2), m_forTerm, facetValue)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case PATTERN:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "notMatches", 2), m_forTerm, facetValue)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case MIN_INCLUSIVE:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "lessThan", 2), m_forTerm, facetValue)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case MIN_EXCLUSIVE:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "lessThanOrEqual", 2), m_forTerm, facetValue)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case MAX_INCLUSIVE:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "greaterThan", 2), m_forTerm, facetValue)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case MAX_EXCLUSIVE:
				m_forBodyExpression.prepend(Atom.create(Predicate.create(Namespaces.SWRLB_NS + "greaterThanOrEqual", 2), m_forTerm, facetValue)).getRules(m_ruleListener, Atom.FALSE);
				break;
			case TOTAL_DIGITS:
			case FRACTION_DIGITS:
				// Not supported
				unsupportedDataRange(dr);
				break;
			}
		}*/
	}
}
