// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLAnnotationAssertionAxiom;
import org.semanticweb.owlapi.model.OWLAnnotationAxiom;
import org.semanticweb.owlapi.model.OWLAnnotationPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLAnnotationPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLAsymmetricObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLAxiomVisitor;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyExpression;
import org.semanticweb.owlapi.model.OWLDataPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLDataRange;
import org.semanticweb.owlapi.model.OWLDatatypeDefinitionAxiom;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.semanticweb.owlapi.model.OWLDifferentIndividualsAxiom;
import org.semanticweb.owlapi.model.OWLDisjointClassesAxiom;
import org.semanticweb.owlapi.model.OWLDisjointDataPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLDisjointObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLDisjointUnionAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentDataPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLFunctionalDataPropertyAxiom;
import org.semanticweb.owlapi.model.OWLFunctionalObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLHasKeyAxiom;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLIndividualAxiom;
import org.semanticweb.owlapi.model.OWLInverseFunctionalObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLInverseObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLIrreflexiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLNegativeDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLNegativeObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLReflexiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLSameIndividualAxiom;
import org.semanticweb.owlapi.model.OWLSubAnnotationPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubDataPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.model.OWLSymmetricObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.SWRLArgument;
import org.semanticweb.owlapi.model.SWRLAtom;
import org.semanticweb.owlapi.model.SWRLBuiltInAtom;
import org.semanticweb.owlapi.model.SWRLClassAtom;
import org.semanticweb.owlapi.model.SWRLDataPropertyAtom;
import org.semanticweb.owlapi.model.SWRLDataRangeAtom;
import org.semanticweb.owlapi.model.SWRLDifferentIndividualsAtom;
import org.semanticweb.owlapi.model.SWRLIndividualArgument;
import org.semanticweb.owlapi.model.SWRLLiteralArgument;
import org.semanticweb.owlapi.model.SWRLObjectPropertyAtom;
import org.semanticweb.owlapi.model.SWRLRule;
import org.semanticweb.owlapi.model.SWRLSameIndividualAtom;
import org.semanticweb.owlapi.model.SWRLVariable;

import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.Individual;
import uk.ac.ox.cs.JRDFox.model.Predicate;
import uk.ac.ox.cs.JRDFox.model.Rule;
import uk.ac.ox.cs.JRDFox.model.Term;
import uk.ac.ox.cs.JRDFox.model.Variable;

public class OWL2RLTranslator implements OWLAxiomVisitor {
	protected static final Variable X = Variable.create("X");
	protected static final Variable Y = Variable.create("Y");
	protected static final Variable Y1 = Variable.create("Y1");
	protected static final Variable Y2 = Variable.create("Y2");
	protected static final Variable Z = Variable.create("Z");

	protected final RuleListener m_ruleListener;
	protected final Collection<JRDFoxException> m_errors;
	protected final boolean m_processLogicalTBoxAxioms;
	protected final boolean m_processLogicalABoxAxioms;
	protected final boolean m_processAnnotationTBoxAxioms;
	protected final boolean m_processAnnotationABoxAxioms;
	
	public OWL2RLTranslator(RuleListener ruleListener, Collection<JRDFoxException> errors, boolean processLogicalTBoxAxioms, boolean processLogicalABoxAxioms, boolean processAnnotationTBoxAxioms, boolean processAnnotationABoxAxioms) {
		m_ruleListener = ruleListener;
		m_errors = errors;
		m_processLogicalTBoxAxioms = processLogicalTBoxAxioms;
		m_processLogicalABoxAxioms = processLogicalABoxAxioms;
		m_processAnnotationTBoxAxioms = processAnnotationTBoxAxioms;
		m_processAnnotationABoxAxioms = processAnnotationABoxAxioms;
	}

	public void translateAxiom(OWLAxiom axiom) {
		boolean shouldProcess = m_processLogicalABoxAxioms && (axiom instanceof OWLIndividualAxiom);
		shouldProcess |= m_processLogicalTBoxAxioms && (axiom instanceof OWLLogicalAxiom && !(axiom instanceof OWLIndividualAxiom));
		shouldProcess |= m_processAnnotationABoxAxioms && (axiom instanceof OWLAnnotationAssertionAxiom);
		shouldProcess |= m_processAnnotationTBoxAxioms && (axiom instanceof OWLAnnotationAxiom && !(axiom instanceof OWLAnnotationAssertionAxiom));
		if (shouldProcess)
			axiom.accept(this);		
	}

	public void translate(OWLOntology rootOntology, boolean includeImportsClosure) {
		Set<OWLOntology> toProcess = (includeImportsClosure ? rootOntology.getImportsClosure() : Collections.singleton(rootOntology));
		for (OWLOntology ontology : toProcess)
			for (OWLAxiom axiom : ontology.getAxioms())
				translateAxiom(axiom);
	}

	protected void add(Atom head, Atom... body) {
		Rule rule = Rule.create(head, body);
		Rule simplifiedRule = rule.simplify();
		if (simplifiedRule != null)
			m_ruleListener.addRule(simplifiedRule);
	}

	protected void unsupportedAxiom(OWLAxiom axiom) {
		if (m_errors != null)
			m_errors.add(new JRDFoxException("Axiom '" + axiom.toString() + "' is not supported in OWL 2 RL."));
	}

	protected void addSubClassOf(OWLAxiom axiom, OWLClassExpression subclass, OWLClassExpression superclass) {
		VariableProducer variableProducer = new VariableProducer();
		Variable newVariable = variableProducer.nextFreshVariable();
		BodyExpression subclassBodyExpression = new SubclassProcessor(axiom, m_errors, variableProducer).getBodyExpressionFor(subclass, newVariable, BodyExpression.TERMINAL);
		if (subclassBodyExpression != null)
			new SuperclassProcessor(m_ruleListener, axiom, m_errors, variableProducer).generateRules(superclass, newVariable, subclassBodyExpression);
	}

	// Declarations
	public void visit(OWLDeclarationAxiom axiom) {
	}

	// Class axioms
	public void visit(OWLSubClassOfAxiom axiom) {
		addSubClassOf(axiom, axiom.getSubClass(), axiom.getSuperClass());
	}

	public void visit(OWLEquivalentClassesAxiom axiom) {
		OWLClassExpression[] classes = axiom.getClassExpressions().toArray(new OWLClassExpression[axiom.getClassExpressions().size()]);
		for (int index = 0; index < classes.length; index++)
			addSubClassOf(axiom, classes[index], classes[(index + 1) % classes.length]);
	}

	public void visit(OWLDisjointClassesAxiom axiom) {
		OWLDataFactory factory = OWLManager.getOWLDataFactory();
		OWLClass owlNothing = factory.getOWLNothing();
		OWLClassExpression[] classes = axiom.getClassExpressions().toArray(new OWLClassExpression[axiom.getClassExpressions().size()]);
		for (int index1 = 0; index1 < classes.length; index1++)
			for (int index2 = index1 + 1; index2 < classes.length; index2++) {
				OWLObjectIntersectionOf conjunction = factory.getOWLObjectIntersectionOf(classes[index1], classes[index2]);
				addSubClassOf(axiom, conjunction, owlNothing);
			}
	}

	public void visit(OWLDisjointUnionAxiom axiom) {
		unsupportedAxiom(axiom);
	}

	// Object property axioms
	public void visit(OWLSubObjectPropertyOfAxiom axiom) {
		add(OWLAPI2Model.A(axiom.getSuperProperty(), X, Y), OWLAPI2Model.A(axiom.getSubProperty(), X, Y));		
	}

	public void visit(OWLSubPropertyChainOfAxiom axiom) {
		List<OWLObjectPropertyExpression> propertyChain = axiom.getPropertyChain();
		Variable[] variables = new Variable[propertyChain.size() + 1];
		for (int variableIndex = 0; variableIndex < variables.length; variableIndex++)
			variables[variableIndex] = Variable.create("X" + variableIndex);
		Atom head = OWLAPI2Model.A(axiom.getSuperProperty(), variables[0], variables[variables.length - 1]);
		Atom[] body = new Atom[propertyChain.size()];
		for (int index = 0; index < body.length; index++)
			body[index] = OWLAPI2Model.A(propertyChain.get(index), variables[index], variables[index + 1]);
		add(head, body);
	}

	public void visit(OWLEquivalentObjectPropertiesAxiom axiom) {
		OWLObjectPropertyExpression[] properties = axiom.getProperties().toArray(new OWLObjectPropertyExpression[axiom.getProperties().size()]);
		for (int index = 0; index < properties.length; index++)
			add(OWLAPI2Model.A(properties[index], X, Y), OWLAPI2Model.A(properties[(index + 1) % properties.length], X, Y));
	}

	public void visit(OWLDisjointObjectPropertiesAxiom axiom) {
		OWLObjectPropertyExpression[] properties = axiom.getProperties().toArray(new OWLObjectPropertyExpression[axiom.getProperties().size()]);
		for (int index1 = 0; index1 < properties.length; index1++)
			for (int index2 = index1 + 1; index2 < properties.length; index2++)
				add(Atom.FALSE, OWLAPI2Model.A(properties[index1], X, Y), OWLAPI2Model.A(properties[index2], X, Y));
	}

	public void visit(OWLInverseObjectPropertiesAxiom axiom) {
		add(OWLAPI2Model.A(axiom.getFirstProperty(), X, Y), OWLAPI2Model.A(axiom.getSecondProperty(), Y, X));
		add(OWLAPI2Model.A(axiom.getSecondProperty(), X, Y), OWLAPI2Model.A(axiom.getFirstProperty(), Y, X));
	}

	public void visit(OWLObjectPropertyDomainAxiom axiom) {
		VariableProducer variableProducer = new VariableProducer();
		Variable x = variableProducer.nextFreshVariable();
		Variable y = variableProducer.nextFreshVariable();
		BodyExpression bodyExpression = BodyExpression.TERMINAL.prepend(OWLAPI2Model.A(axiom.getProperty(), x, y));
		new SuperclassProcessor(m_ruleListener, axiom, m_errors, variableProducer).generateRules(axiom.getDomain(), x, bodyExpression);
	}

	public void visit(OWLObjectPropertyRangeAxiom axiom) {
		VariableProducer variableProducer = new VariableProducer();
		Variable x = variableProducer.nextFreshVariable();
		Variable y = variableProducer.nextFreshVariable();
		BodyExpression bodyExpression = BodyExpression.TERMINAL.prepend(OWLAPI2Model.A(axiom.getProperty(), x, y));
		new SuperclassProcessor(m_ruleListener, axiom, m_errors, variableProducer).generateRules(axiom.getRange(), y, bodyExpression);
	}

	public void visit(OWLFunctionalObjectPropertyAxiom axiom) {
		add(Atom.create(Predicate.SAME_AS, Y1, Y2), OWLAPI2Model.A(axiom.getProperty(), X, Y1), OWLAPI2Model.A(axiom.getProperty(), X, Y2));
	}

	public void visit(OWLInverseFunctionalObjectPropertyAxiom axiom) {
		add(Atom.create(Predicate.SAME_AS, Y1, Y2), OWLAPI2Model.A(axiom.getProperty(), Y1, X), OWLAPI2Model.A(axiom.getProperty(), Y2, X));
	}

	public void visit(OWLReflexiveObjectPropertyAxiom axiom) {
		unsupportedAxiom(axiom);
	}

	public void visit(OWLIrreflexiveObjectPropertyAxiom axiom) {
		add(Atom.FALSE, OWLAPI2Model.A(axiom.getProperty(), X, X));
	}

	public void visit(OWLSymmetricObjectPropertyAxiom axiom) {
		add(OWLAPI2Model.A(axiom.getProperty(), X, Y), OWLAPI2Model.A(axiom.getProperty(), Y, X));
	}

	public void visit(OWLAsymmetricObjectPropertyAxiom axiom) {
		add(Atom.FALSE, OWLAPI2Model.A(axiom.getProperty(), X, Y), OWLAPI2Model.A(axiom.getProperty(), Y, X));
	}

	public void visit(OWLTransitiveObjectPropertyAxiom axiom) {
		add(OWLAPI2Model.A(axiom.getProperty(), X, Z), OWLAPI2Model.A(axiom.getProperty(), X, Y), OWLAPI2Model.A(axiom.getProperty(), Y, Z));
	}

	// Data property axioms
	public void visit(OWLSubDataPropertyOfAxiom axiom) {
		add(OWLAPI2Model.A(axiom.getSuperProperty(), X, Y), OWLAPI2Model.A(axiom.getSubProperty(), X, Y));
	}

	public void visit(OWLEquivalentDataPropertiesAxiom axiom) {
		OWLDataPropertyExpression[] properties = axiom.getProperties().toArray(new OWLDataPropertyExpression[axiom.getProperties().size()]);
		for (int index = 0; index < properties.length; index++)
			add(OWLAPI2Model.A(properties[index], X, Y), OWLAPI2Model.A(properties[(index + 1) % properties.length], X, Y));
	}

	public void visit(OWLDisjointDataPropertiesAxiom axiom) {
		OWLDataPropertyExpression[] properties = axiom.getProperties().toArray(new OWLDataPropertyExpression[axiom.getProperties().size()]);
		for (int index1 = 0; index1 < properties.length; index1++)
			for (int index2 = index1 + 1; index2 < properties.length; index2++)
				add(Atom.FALSE, OWLAPI2Model.A(properties[index1], X, Y), OWLAPI2Model.A(properties[index2], X, Y));
	}

	public void visit(OWLDataPropertyDomainAxiom axiom) {
		VariableProducer variableProducer = new VariableProducer();
		Variable x = variableProducer.nextFreshVariable();
		Variable y = variableProducer.nextFreshVariable();
		BodyExpression bodyExpression = BodyExpression.TERMINAL.prepend(OWLAPI2Model.A(axiom.getProperty(), x, y));
		new SuperclassProcessor(m_ruleListener, axiom, m_errors, variableProducer).generateRules(axiom.getDomain(), x, bodyExpression);
	}

	public void visit(OWLDataPropertyRangeAxiom axiom) {
		VariableProducer variableProducer = new VariableProducer();
		Variable x = variableProducer.nextFreshVariable();
		Variable y = variableProducer.nextFreshVariable();
		BodyExpression bodyExpression = BodyExpression.TERMINAL.prepend(OWLAPI2Model.A(axiom.getProperty(), x, y));
		new SuperclassProcessor(m_ruleListener, axiom, m_errors, variableProducer).generateRules(axiom.getRange(), y, bodyExpression);
	}

	public void visit(OWLFunctionalDataPropertyAxiom axiom) {
		add(Atom.create(Predicate.SAME_AS, Y1, Y2), OWLAPI2Model.A(axiom.getProperty(), X, Y1), OWLAPI2Model.A(axiom.getProperty(), X, Y2));
	}

	// Keys
	public void visit(OWLHasKeyAxiom axiom) {
		OWLClassExpression classExpression = axiom.getClassExpression();
		BodyExpression currentBodyExpression = BodyExpression.TERMINAL;
		VariableProducer variableProducer = new VariableProducer();
		SubclassProcessor subclassTranslator = new SubclassProcessor(axiom, m_errors, variableProducer);
		Variable x = variableProducer.nextFreshVariable();
		if (!classExpression.isTopEntity()) {
			currentBodyExpression = subclassTranslator.getBodyExpressionFor(classExpression, x, currentBodyExpression);
			if (currentBodyExpression == null)
				return;
		}
		Variable y = variableProducer.nextFreshVariable();
		if (!classExpression.isTopEntity()) {
			currentBodyExpression = subclassTranslator.getBodyExpressionFor(classExpression, y, currentBodyExpression);
			if (currentBodyExpression == null)
				return;
		}
		for (OWLObjectPropertyExpression ope : axiom.getObjectPropertyExpressions()) {
			Variable z = variableProducer.nextFreshVariable();
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ope, x, z));
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(ope, y, z));
		}
		for (OWLDataPropertyExpression dpe : axiom.getDataPropertyExpressions()) {
			Variable w = variableProducer.nextFreshVariable();
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(dpe, x, w));
			currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(dpe, y, w));
		}
		currentBodyExpression.getRules(m_ruleListener, Atom.create(Predicate.SAME_AS, x, y));
	}

	// Datatype definitions
	public void visit(OWLDatatypeDefinitionAxiom axiom) {		
		unsupportedAxiom(axiom);
	}

	// Rules
	protected static Term[] getArguments(SWRLAtom atom, Map<SWRLVariable, Variable> variableMap, VariableProducer variableProducer) {
		Term[] arguments = new Term[atom.getAllArguments().size()];
		int argumentIndex = 0;
		for (SWRLArgument argument : atom.getAllArguments()) {
			if (argument instanceof SWRLIndividualArgument)
				arguments[argumentIndex] = OWLAPI2Model.S(((SWRLIndividualArgument)argument).getIndividual());
			else if (argument instanceof SWRLLiteralArgument)
				arguments[argumentIndex] = OWLAPI2Model.S(((SWRLLiteralArgument)argument).getLiteral());
			else {
				SWRLVariable swrlVariable = (SWRLVariable)argument;
				Variable variable = variableMap.get(swrlVariable);
				if (variable == null) {
					variable = variableProducer.nextFreshVariable();
					variableMap.put(swrlVariable, variable);
				}
				arguments[argumentIndex] = variable;
			}
			argumentIndex++;
		}
		return arguments;
	}

	public void visit(SWRLRule rule) {
		VariableProducer variableProducer = new VariableProducer();
		Map<SWRLVariable, Variable> variableMap = new HashMap<SWRLVariable, Variable>();
		SubclassProcessor subclassTranslator = new SubclassProcessor(rule, m_errors, variableProducer);
		SuperclassProcessor superclassTranslator = new SuperclassProcessor(m_ruleListener, rule, m_errors, variableProducer);
		BodyExpression currentBodyExpression = BodyExpression.TERMINAL;
		for (SWRLAtom atom : rule.getBody()) {
			Term[] arguments = getArguments(atom, variableMap, variableProducer);
			if (atom instanceof SWRLClassAtom) {
				OWLClassExpression classExpression = ((SWRLClassAtom)atom).getPredicate();
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(classExpression, arguments[0], currentBodyExpression);
			}
			else if (atom instanceof SWRLObjectPropertyAtom)
				currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(((SWRLObjectPropertyAtom)atom).getPredicate(), arguments[0], arguments[1]));
			else if (atom instanceof SWRLDataPropertyAtom)
				currentBodyExpression = currentBodyExpression.prepend(OWLAPI2Model.A(((SWRLDataPropertyAtom)atom).getPredicate(), arguments[0], arguments[1]));
			else if (atom instanceof SWRLDataRangeAtom) {
				OWLDataRange dataRange = ((SWRLDataRangeAtom)atom).getPredicate();
				currentBodyExpression = subclassTranslator.getBodyExpressionFor(dataRange, arguments[0], currentBodyExpression);
			}
			else if (atom instanceof SWRLSameIndividualAtom)
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.SAME_AS, arguments));
			else if (atom instanceof SWRLDifferentIndividualsAtom)
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.DIFFERENT_FROM, arguments));
			else if (atom instanceof SWRLBuiltInAtom) {
				String predicateIRI = ((SWRLBuiltInAtom)atom).getPredicate().toString();
				currentBodyExpression = currentBodyExpression.prepend(Atom.create(Predicate.create(predicateIRI), arguments));
			}
			else {
				m_errors.add(new JRDFoxException("Atom '" + atom.toString() + "' is not supported in rule '" + rule.toString() + "'."));
				return;
			}
			if (currentBodyExpression == null)
				return;
		}
		for (SWRLAtom atom : rule.getHead()) {
			Term[] arguments = getArguments(atom, variableMap, variableProducer);
			if (atom instanceof SWRLClassAtom) {
				OWLClassExpression classExpression = ((SWRLClassAtom)atom).getPredicate();
				superclassTranslator.generateRules(classExpression, arguments[0], currentBodyExpression);
			}
			else if (atom instanceof SWRLObjectPropertyAtom)
				currentBodyExpression.getRules(m_ruleListener, OWLAPI2Model.A(((SWRLObjectPropertyAtom)atom).getPredicate(), arguments[0], arguments[1]));
			else if (atom instanceof SWRLDataPropertyAtom)
				currentBodyExpression.getRules(m_ruleListener, OWLAPI2Model.A(((SWRLDataPropertyAtom)atom).getPredicate(), arguments[0], arguments[1]));
			else if (atom instanceof SWRLDataRangeAtom) {
				OWLDataRange dataRange = ((SWRLDataRangeAtom)atom).getPredicate();
				superclassTranslator.generateRules(dataRange, arguments[0], currentBodyExpression);
			}
			else if (atom instanceof SWRLSameIndividualAtom)
				currentBodyExpression.getRules(m_ruleListener, Atom.create(Predicate.SAME_AS, arguments));
			else if (atom instanceof SWRLDifferentIndividualsAtom)
				currentBodyExpression.getRules(m_ruleListener, Atom.create(Predicate.DIFFERENT_FROM, arguments));
			else
				m_errors.add(new JRDFoxException("Atom '" + atom.toString() + "' is not supported in rule '" + rule.toString() + "'."));
		}
	}

	// Assertions
	public void visit(OWLSameIndividualAxiom axiom) {
		List<OWLIndividual> individuals = axiom.getIndividualsAsList();
		for (int index = 0; index < individuals.size() - 1; index++)
			m_ruleListener.addAtom(Atom.create(Predicate.SAME_AS, OWLAPI2Model.S(individuals.get(index)), OWLAPI2Model.S(individuals.get(index + 1))));
	}

	public void visit(OWLDifferentIndividualsAxiom axiom) {
		List<OWLIndividual> individuals = axiom.getIndividualsAsList();
		for (int index1 = 0; index1 < individuals.size(); index1++)
			for (int index2 = index1 + 1; index2 < individuals.size(); index2++)
				m_ruleListener.addAtom(Atom.create(Predicate.DIFFERENT_FROM, OWLAPI2Model.S(individuals.get(index1)), OWLAPI2Model.S(individuals.get(index2))));
	}

	public void visit(OWLClassAssertionAxiom axiom) {
		Individual individual = OWLAPI2Model.S(axiom.getIndividual());
		if (axiom.getClassExpression() instanceof OWLClass) {
			if (!axiom.getClassExpression().isOWLThing())
				m_ruleListener.addAtom(Atom.create(OWLAPI2Model.S((OWLClass)axiom.getClassExpression()), individual));
		}
		else
			new SuperclassProcessor(m_ruleListener, axiom, m_errors, new VariableProducer()).generateRules(axiom.getClassExpression(), individual, BodyExpression.TERMINAL);
	}

	public void visit(OWLObjectPropertyAssertionAxiom axiom) {
		m_ruleListener.addAtom(OWLAPI2Model.A(axiom.getProperty(), OWLAPI2Model.S(axiom.getSubject()), OWLAPI2Model.S(axiom.getObject())));
	}

	public void visit(OWLNegativeObjectPropertyAssertionAxiom axiom) {
		unsupportedAxiom(axiom);
	}

	public void visit(OWLDataPropertyAssertionAxiom axiom) {
		m_ruleListener.addAtom(OWLAPI2Model.A(axiom.getProperty(), OWLAPI2Model.S(axiom.getSubject()), OWLAPI2Model.S(axiom.getObject())));
	}

	public void visit(OWLNegativeDataPropertyAssertionAxiom axiom) {
		unsupportedAxiom(axiom);
	}

	// Annotations
	public void visit(OWLAnnotationAssertionAxiom axiom) {
		m_ruleListener.addAtom(OWLAPI2Model.A(axiom.getProperty(), OWLAPI2Model.S(axiom.getSubject()), OWLAPI2Model.S(axiom.getValue())));
	}

	public void visit(OWLSubAnnotationPropertyOfAxiom axiom) {
	}

	public void visit(OWLAnnotationPropertyDomainAxiom axiom) {
	}

	public void visit(OWLAnnotationPropertyRangeAxiom axiom) {
	}
}
