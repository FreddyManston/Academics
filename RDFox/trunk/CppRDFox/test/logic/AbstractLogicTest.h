// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#if defined(WITH_TEST) && !defined(ABSTRACTLOGICTEST_H_)
#define ABSTRACTLOGICTEST_H_

#include "../../src/Common.h"
#include "../../src/logic/Logic.h"
#include "../../src/formats/turtle/SPARQLParser.h"
#include "../../src/util/Prefixes.h"

class AbstractLogicTest {

protected:

    LogicFactory m_factory;

public:

    AbstractLogicTest() : m_factory() {
    }

    void initialize() {
        m_factory = ::newLogicFactory();
    }

    Variable getVariableFromText(const char* const variableText) const {
        assert(variableText[0] == '?' || variableText[0] == '$');
        return m_factory->getVariable(variableText + 1);
    }

    Term getTermFromText(const char* const termText) const {
        if (termText[0] == '?' || termText[0] == '$')
            return getVariableFromText(termText);
        else
            return m_factory->getIRIReference(termText);
    }

    BuiltinFunctionCall UBFC(const char* const functionIRI, const BuiltinExpression& argument) const {
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(argument);
        return m_factory->getBuiltinFunctionCall(functionIRI, arguments);
    }

    BuiltinFunctionCall BBFC(const char* const functionIRI, const BuiltinExpression& argument1, const BuiltinExpression& argument2) const {
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(argument1);
        arguments.push_back(argument2);
        return m_factory->getBuiltinFunctionCall(functionIRI, arguments);
    }

    ExistenceExpression EXISTS(const Formula& formula) const {
        return m_factory->getExistenceExpression(true, formula);
    }

    ExistenceExpression NOT_EXISTS(const Formula& formula) const {
        return m_factory->getExistenceExpression(false, formula);
    }

    static std::vector<BuiltinExpression> BEA() {
        return std::vector<BuiltinExpression>();
    }

    static std::vector<Term> TA() {
        return std::vector<Term>();
    }

    static std::vector<GroundTerm> GTA() {
        return std::vector<GroundTerm>();
    }

    static std::vector<Formula> FA() {
        return std::vector<Formula>();
    }

    static std::vector<Variable> VA() {
        return std::vector<Variable>();
    }

    static std::vector<AtomicFormula> AFA() {
        return std::vector<AtomicFormula>();
    }

    static std::vector<Atom> AA() {
        return std::vector<Atom>();
    }

    static std::vector<Atom> AA(const Atom a) {
        std::vector<Atom> atomList;
        atomList.push_back(a);
        return atomList;
    }

    static std::vector<Atom> AA(const Atom a1, const Atom a2) {
        std::vector<Atom> atomList;
        atomList.push_back(a1);
        atomList.push_back(a2);
        return atomList;
    }

    static std::vector<Atom> AA(const Atom a1, const Atom a2, const  Atom a3) {
        std::vector<Atom> atomList;
        atomList.push_back(a1);
        atomList.push_back(a2);
        atomList.push_back(a3);
        return atomList;
    }
    
    static std::vector<Literal> LA() {
        return std::vector<Literal>();
    }

    static std::vector<AggregateBind> AGGBNDA() {
        return std::vector<AggregateBind>();
    }

    static Substitution S() {
        return Substitution();
    }

    Variable V(const char* const variableName) const {
        return m_factory->getVariable(variableName);
    }

    Variable V(const std::string& variableName) const {
        return V(variableName.c_str());
    }

    ResourceByID UNDEF() const {
        return m_factory->getResourceByID(INVALID_RESOURCE_ID);
    }

    ResourceByName I(const char* const iriReference) const {
        return m_factory->getIRIReference(iriReference);
    }

    ResourceByName I(const std::string& iriReference) const {
        return I(iriReference.c_str());
    }

    ResourceByName B(const char* const blankNode) const {
        return m_factory->getBlankNode(blankNode);
    }

    ResourceByName B(const std::string& blankNode) const {
        return B(blankNode.c_str());
    }

    ResourceByName L(const char* const lexicalForm, const char* const datatypeIRI) const {
        return m_factory->getLiteral(lexicalForm, datatypeIRI);
    }

    ResourceByName L(const std::string& lexicalForm, const std::string& datatypeIRI) const {
        return L(lexicalForm.c_str(), datatypeIRI.c_str());
    }

    Predicate P(const char* const name) const {
        return m_factory->getPredicate(name);
    }

    Predicate P(const std::string& name) const {
        return P(name.c_str());
    }

    BuiltinFunctionCall BFC(const char* const functionName, const std::vector<BuiltinExpression>& arguments) const {
        return m_factory->getBuiltinFunctionCall(functionName, arguments);
    }

    BuiltinFunctionCall BFC(const std::string& functionName, const std::vector<BuiltinExpression>& arguments) const {
        return m_factory->getBuiltinFunctionCall(functionName.c_str(), arguments);
    }

    Filter FLT(const BuiltinExpression& builtinExpression) const {
        return m_factory->getFilter(builtinExpression);
    }

    Bind BND(const BuiltinExpression& builtinExpression, const Variable& boundVariable) const {
        return m_factory->getBind(builtinExpression, boundVariable);
    }

    Atom A(const Predicate& predicate, const std::vector<Term>& arguments) const {
        return m_factory->getAtom(predicate, arguments);
    }

    Atom A(const Term& subject, const Term& predicate, const Term& object) const {
        return m_factory->getRDFAtom(subject, predicate, object);
    }

    Atom RA(const char* const subjectText, const char* const predicateText, const char* const objectText) const {
        Term subject = getTermFromText(subjectText);
        Term predicate = getTermFromText(predicateText);
        Term object = getTermFromText(objectText);
        return m_factory->getRDFAtom(subject, predicate, object);
    }

    Atom RA(const std::string& subjectText, const std::string& predicateText, const std::string& objectText) const {
        return RA(subjectText.c_str(), predicateText.c_str(), objectText.c_str());
    }

    Negation NOT(const AtomicFormula& atomicFormula) const {
        std::vector<Variable> existentialVariables;
        std::vector<AtomicFormula> atomicFormulas;
        atomicFormulas.push_back(atomicFormula);
        return m_factory->getNegation(existentialVariables, atomicFormulas);
    }

    Negation NOT(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas) const {
        return m_factory->getNegation(existentialVariables, atomicFormulas);
    }

    AggregateBind AGGBND(const std::string &functionName, const bool distinct, const std::vector<BuiltinExpression> &arguments, const Term &result) const {
        return m_factory->getAggregateBind(functionName, distinct, arguments, result);
    }

    Aggregate AGG(const std::vector<AtomicFormula> &atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable> &on, const std::vector<AggregateBind> &aggregateBinds) const {
        return m_factory->getAggregate(atomicFormulas, skipEmptyGroups, on, aggregateBinds);
    }

    Conjunction AND(const std::vector<Formula>& conjuncts) const {
        return m_factory->getConjunction(conjuncts);
    }

    Conjunction AND(const Formula& conjunct1, const Formula& conjunct2) const {
        return m_factory->getConjunction(conjunct1, conjunct2);
    }

    Disjunction OR(const std::vector<Formula>& disjuncts) const {
        return m_factory->getDisjunction(disjuncts);
    }

    Disjunction OR(const Formula& disjunct1, const Formula& disjunct2) const {
        return m_factory->getDisjunction(disjunct1, disjunct2);
    }

    Optional OPT(const Formula& main, const std::vector<Formula>& optionals) const {
        return m_factory->getOptional(main, optionals);
    }

    Optional OPT(const Formula& main, const Formula& optional) const {
        return m_factory->getOptional(main, optional);
    }

    Minus MINUS(const Formula& main, const std::vector<Formula>& subtrahends) const {
        return m_factory->getMinus(main, subtrahends);
    }

    Minus MINUS(const Formula& main, const Formula& subtrahend) const {
        return m_factory->getMinus(main, subtrahend);
    }

    Values VALUES(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data) const {
        return m_factory->getValues(variables, data);
    }

    Query Q(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula) const {
        return m_factory->getQuery(distinct, answerTerms, queryFormula);
    }

    Query Q(const bool distinct, const std::vector<Term>& answerTerms, const std::vector<Formula>& queryFormulas) const {
        return m_factory->getQuery(distinct, answerTerms, m_factory->getConjunction(queryFormulas));
    }

    Query Q(const char* const queryText) const {
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        SPARQLParser sparqlParser(prefixes);
        return sparqlParser.parse(m_factory, queryText, ::strlen(queryText));
    }

    Rule R(const Atom& head, const std::vector<Literal>& body) const {
        return m_factory->getRule(head, body);
    }

    Rule R(const std::vector<Atom>& head, const std::vector<Literal>& body) const {
        return m_factory->getRule(head, body);
    }

    static Substitution::value_type M(const Variable& variable, const Term& term) {
        return Substitution::value_type(variable, term);
    }

    Substitution::value_type M(const char* const variableText, const char* const termText) const {
        return M(getVariableFromText(variableText), getTermFromText(termText));
    }

};

template<class V, class E>
always_inline std::vector<V>&& operator<<(std::vector<V>&& vector, const E& element) {
    vector.push_back(element);
    return std::move(vector);
}

always_inline Substitution&& operator<<(Substitution&& substitution, const Substitution::value_type& mapping) {
    substitution.insert(mapping);
    return std::move(substitution);
}

#endif // ABSTRACTLOGICTEST_H_
