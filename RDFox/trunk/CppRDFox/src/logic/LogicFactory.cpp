// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/HashTableImpl.h"
#include "../util/InterningManagerImpl.h"
#include "Logic.h"

// _LogicFactory::FactoryInterningManager

template<class T>
always_inline _LogicFactory::FactoryInterningManager<T>::FactoryInterningManager(MemoryManager& memoryManager) :
    InterningManager<typename T::PointeeType, FactoryInterningManager<T> >(memoryManager)
{
}

template<class T>
always_inline void _LogicFactory::FactoryInterningManager<T>::setFactory(_LogicFactory* factory) {
    m_factory = factory;
}

template<class T>
always_inline size_t _LogicFactory::FactoryInterningManager<T>::getObjectHashCode(const typename T::PointeeType* const object) {
    return object->hash();
}

template<class T>
template<typename... Args>
always_inline size_t _LogicFactory::FactoryInterningManager<T>::hashCodeFor(Args&&... args) {
    return T::PointeeType::hashCodeFor(std::forward<Args>(args)...);
}

template<class T>
template<typename... Args>
always_inline bool _LogicFactory::FactoryInterningManager<T>::isEqual(const typename T::PointeeType* const object, const size_t valuesHashCode, Args&&... args) {
    return valuesHashCode == object->hash() && object->isEqual(std::forward<Args>(args)...);
}

template<class T>
template<typename... Args>
always_inline typename T::PointeeType* _LogicFactory::FactoryInterningManager<T>::makeNew(const size_t valueHashCode, Args&&... args) const {
    return new typename T::PointeeType(m_factory, valueHashCode, std::forward<Args>(args)...);
}

template<class T>
template<typename... Args>
always_inline T _LogicFactory::FactoryInterningManager<T>::getObject(Args&&... args) {
    return T(this->get(std::forward<Args>(args)...));
}

// _LogicFactory

_LogicFactory::_LogicFactory() :
    m_referenceCount(0),
    m_memoryManager(1024ULL * 1024ULL * 1024ULL * 1024ULL),
    m_predicates(m_memoryManager),
    m_builtinFunctionCalls(m_memoryManager),
    m_existenceExpressions(m_memoryManager),
    m_variables(m_memoryManager),
    m_resourceByIDs(m_memoryManager),
    m_resourceByNames(m_memoryManager),
    m_atoms(m_memoryManager),
    m_binds(m_memoryManager),
    m_filters(m_memoryManager),
    m_negations(m_memoryManager),
    m_aggregateBinds(m_memoryManager),
    m_aggregates(m_memoryManager),
    m_conjunctions(m_memoryManager),
    m_disjunctions(m_memoryManager),
    m_optionals(m_memoryManager),
    m_minuses(m_memoryManager),
    m_values(m_memoryManager),
    m_queries(m_memoryManager),
    m_rules(m_memoryManager)
{
    m_predicates.setFactory(this);
    m_builtinFunctionCalls.setFactory(this);
    m_existenceExpressions.setFactory(this);
    m_variables.setFactory(this);
    m_resourceByIDs.setFactory(this);
    m_resourceByNames.setFactory(this);
    m_atoms.setFactory(this);
    m_binds.setFactory(this);
    m_filters.setFactory(this);
    m_negations.setFactory(this);
    m_aggregateBinds.setFactory(this);
    m_aggregates.setFactory(this);
    m_conjunctions.setFactory(this);
    m_disjunctions.setFactory(this);
    m_optionals.setFactory(this);
    m_minuses.setFactory(this);
    m_queries.setFactory(this);
    m_values .setFactory(this);
    m_rules.setFactory(this);
    if (!m_predicates.initialize() ||
        !m_builtinFunctionCalls.initialize() ||
        !m_existenceExpressions.initialize() ||
        !m_variables.initialize() ||
        !m_resourceByIDs.initialize() ||
        !m_resourceByNames.initialize() ||
        !m_atoms.initialize() ||
        !m_binds.initialize() ||
        !m_filters.initialize() ||
        !m_negations.initialize() ||
        !m_aggregateBinds.initialize() ||
        !m_aggregates.initialize() ||
        !m_conjunctions.initialize() ||
        !m_disjunctions.initialize() ||
        !m_optionals.initialize() ||
        !m_minuses.initialize() ||
        !m_values.initialize() ||
        !m_queries.initialize() ||
        !m_rules.initialize())
        throw std::bad_alloc();
}

_LogicFactory::~_LogicFactory() {
}

void _LogicFactory::dispose(const _BuiltinFunctionCall* const object) {
    m_builtinFunctionCalls.dispose(object);
}

BuiltinFunctionCall _LogicFactory::getBuiltinFunctionCall(const std::string& functionName, const std::vector<BuiltinExpression>& arguments) {
    return getBuiltinFunctionCall(functionName.c_str(), arguments);
}

BuiltinFunctionCall _LogicFactory::getBuiltinFunctionCall(const char* const functionName, const std::vector<BuiltinExpression>& arguments) {
    return m_builtinFunctionCalls.getObject(functionName, arguments);
}

void _LogicFactory::dispose(const _ExistenceExpression* const object) {
    m_existenceExpressions.dispose(object);
}

ExistenceExpression _LogicFactory::getExistenceExpression(const bool positive, const Formula& formula) {
    return m_existenceExpressions.getObject(positive, formula);
}

void _LogicFactory::dispose(const _Variable* const object) {
    m_variables.dispose(object);
}

Variable _LogicFactory::getVariable(const std::string& name) {
    return getVariable(name.c_str());
}

Variable _LogicFactory::getVariable(const char* const name) {
    return m_variables.getObject(name);
}

void _LogicFactory::dispose(const _ResourceByID* const object) {
    m_resourceByIDs.dispose(object);
}

ResourceByID _LogicFactory::getResourceByID(const ResourceID resourceID) {
    return m_resourceByIDs.getObject(resourceID);
}

void _LogicFactory::dispose(const _ResourceByName* const object) {
    m_resourceByNames.dispose(object);
}

ResourceByName _LogicFactory::getResourceByName(const ResourceText& resourceText) {
    return m_resourceByNames.getObject(resourceText.m_resourceType, resourceText.m_lexicalForm.c_str(), resourceText.m_datatypeIRI.c_str());
}

ResourceByName _LogicFactory::getResourceByName(const ResourceType resourceType, const std::string& lexicalForm, std::string& datatypeIRI) {
    return m_resourceByNames.getObject(resourceType, lexicalForm.c_str(), datatypeIRI.c_str());
}

ResourceByName _LogicFactory::getResourceByName(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI) {
    return m_resourceByNames.getObject(resourceType, lexicalForm, datatypeIRI);
}

ResourceByName _LogicFactory::getIRIReference(const std::string& iriReference) {
    return _LogicFactory::getIRIReference(iriReference.c_str());
}

ResourceByName _LogicFactory::getIRIReference(const char* const iriReference) {
    return m_resourceByNames.getObject(IRI_REFERENCE, iriReference, "");
}

ResourceByName _LogicFactory::getBlankNode(const std::string& blankNode) {
    return _LogicFactory::getBlankNode(blankNode.c_str());
}

ResourceByName _LogicFactory::getBlankNode(const char* const blankNode) {
    return m_resourceByNames.getObject(BLANK_NODE, blankNode, "");
}

ResourceByName _LogicFactory::getLiteral(const std::string& lexicalForm, const std::string& datatypeIRI) {
    return getLiteral(lexicalForm.c_str(), datatypeIRI.c_str());
}

ResourceByName _LogicFactory::getLiteral(const char* const lexicalForm, const char* const datatypeIRI) {
    return m_resourceByNames.getObject(LITERAL, lexicalForm, datatypeIRI);
}

void _LogicFactory::dispose(const _Predicate* const object) {
    m_predicates.dispose(object);
}

Predicate _LogicFactory::getPredicate(const std::string& name) {
    return getPredicate(name.c_str());
}

Predicate _LogicFactory::getPredicate(const char* const name) {
    return m_predicates.getObject(name);
}

Predicate _LogicFactory::getRDFPredicate() {
    return getPredicate("internal$rdf");
}

void _LogicFactory::dispose(const _Atom* const object) {
    m_atoms.dispose(object);
}

Atom _LogicFactory::getAtom(const Predicate& predicate, const std::vector<Term>& arguments) {
    return m_atoms.getObject(predicate, arguments);
}

Atom _LogicFactory::getRDFAtom(const Term& subject, const Term& predicate, const Term& object) {
    return m_atoms.getObject(getRDFPredicate(), subject, predicate, object);
}

void _LogicFactory::dispose(const _Bind* const object) {
    m_binds.dispose(object);
}

Bind _LogicFactory::getBind(const BuiltinExpression& builtinExpression, const Term& boundTerm) {
    return m_binds.getObject(builtinExpression, boundTerm);
}

void _LogicFactory::dispose(const _Filter* const object) {
    m_filters.dispose(object);
}

Filter _LogicFactory::getFilter(const BuiltinExpression& builtinExpression) {
    return m_filters.getObject(builtinExpression);
}

void _LogicFactory::dispose(const _Negation* const object) {
    m_negations.dispose(object);
}

Negation _LogicFactory::getNegation(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas) {
    return m_negations.getObject(existentialVariables, atomicFormulas);
}

void _LogicFactory::dispose(const _AggregateBind* const object) {
    m_aggregateBinds.dispose(object);
}

AggregateBind _LogicFactory::getAggregateBind(const std::string& functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& result) {
    return getAggregateBind(functionName.c_str(), distinct, arguments, result);
}

AggregateBind _LogicFactory::getAggregateBind(const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& result) {
    return m_aggregateBinds.getObject(functionName, distinct, arguments, result);
}

AggregateBind _LogicFactory::getAggregateBind(const std::string& functionName, const bool distinct, const BuiltinExpression& argument, const Term& result) {
    return getAggregateBind(functionName.c_str(), distinct, argument, result);
}

AggregateBind _LogicFactory::getAggregateBind(const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& result) {
    return m_aggregateBinds.getObject(functionName, distinct, argument, result);
}

void _LogicFactory::dispose(const _Aggregate* const object) {
    m_aggregates.dispose(object);
}

Aggregate _LogicFactory::getAggregate(const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds) {
    return m_aggregates.getObject(atomicFormulas, skipEmptyGroups, groupVariables, aggregateBinds);
}

void _LogicFactory::dispose(const _Conjunction* const object) {
    m_conjunctions.dispose(object);
}

Conjunction _LogicFactory::getConjunction(const std::vector<Formula>& conjuncts) {
    return m_conjunctions.getObject(conjuncts);
}

Conjunction _LogicFactory::getConjunction(const Formula& conjunct1, const Formula& conjunct2) {
    return m_conjunctions.getObject(conjunct1, conjunct2);
}

void _LogicFactory::dispose(const _Disjunction* const object) {
    m_disjunctions.dispose(object);
}

Disjunction _LogicFactory::getDisjunction(const std::vector<Formula>& disjuncts) {
    return m_disjunctions.getObject(disjuncts);
}

Disjunction _LogicFactory::getDisjunction(const Formula& disjunct1, const Formula& disjunct2) {
    return m_disjunctions.getObject(disjunct1, disjunct2);
}

void _LogicFactory::dispose(const _Optional* const object) {
    m_optionals.dispose(object);
}

Optional _LogicFactory::getOptional(const Formula& main, const std::vector<Formula>& optionals) {
    return m_optionals.getObject(main, optionals);
}

Optional _LogicFactory::getOptional(const Formula& main, const Formula& optional) {
    return m_optionals.getObject(main, optional);
}

void _LogicFactory::dispose(const _Minus* const object) {
    m_minuses.dispose(object);
}

Minus _LogicFactory::getMinus(const Formula& main, const std::vector<Formula>& subtrahends) {
    return m_minuses.getObject(main, subtrahends);
}

Minus _LogicFactory::getMinus(const Formula& main, const Formula& subtrahend) {
    return m_minuses.getObject(main, subtrahend);
}

void _LogicFactory::dispose(const _Values* const object) {
    m_values.dispose(object);
}

Values _LogicFactory::getValues(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data) {
    return m_values.getObject(variables, data);
}

void _LogicFactory::dispose(const _Query* const object) {
    m_queries.dispose(object);
}

Query _LogicFactory::getQuery(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula) {
    return m_queries.getObject(distinct, answerTerms, queryFormula);
}

void _LogicFactory::dispose(const _Rule* const object) {
    m_rules.dispose(object);
}

Rule _LogicFactory::getRule(const Atom& head, const std::vector<Literal>& body) {
    return m_rules.getObject(head, body);
}

Rule _LogicFactory::getRule(const std::vector<Atom>& head, const std::vector<Literal>& body) {
    return m_rules.getObject(head, body);
}
