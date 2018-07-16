// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOGICFACTORY_H_
#define LOGICFACTORY_H_

#include "../Common.h"
#include "../util/MemoryManager.h"
#include "../util/InterningManager.h"

class _LogicFactory : private Unmovable {

    friend LogicFactory newLogicFactory();
    friend class DefaultReferenceManager<_LogicFactory>;
    friend class _LogicObject;
    friend class _BuiltinFunctionCall;
    friend class _ExistenceExpression;
    friend class _Variable;
    friend class _ResourceByID;
    friend class _ResourceByName;
    friend class _Predicate;
    friend class _Atom;
    friend class _Bind;
    friend class _Filter;
    friend class _Negation;
    friend class _AggregateBind;
    friend class _Aggregate;
    friend class _Conjunction;
    friend class _Disjunction;
    friend class _Optional;
    friend class _Minus;
    friend class _Values;
    friend class _Query;
    friend class _Rule;

protected:

    template<class T>
    class FactoryInterningManager : public InterningManager<typename T::PointeeType, FactoryInterningManager<T> > {

    protected:

        _LogicFactory* m_factory;

    public:

        FactoryInterningManager(MemoryManager& memoryManager);

        void setFactory(_LogicFactory* factory);

        static size_t getObjectHashCode(const typename T::PointeeType* const object);

        template<typename... Args>
        static size_t hashCodeFor(Args&&... args);

        template<typename... Args>
        static bool isEqual(const typename T::PointeeType* const object, const size_t valuesHashCode, Args&&... args);

        template<typename... Args>
        typename T::PointeeType* makeNew(const size_t valueHashCode, Args&&... args) const;

        template<typename... Args>
        T getObject(Args&&... args);

    };

    
    mutable int32_t m_referenceCount;
    MemoryManager m_memoryManager;
    FactoryInterningManager<Predicate> m_predicates;
    FactoryInterningManager<BuiltinFunctionCall> m_builtinFunctionCalls;
    FactoryInterningManager<ExistenceExpression> m_existenceExpressions;
    FactoryInterningManager<Variable> m_variables;
    FactoryInterningManager<ResourceByID> m_resourceByIDs;
    FactoryInterningManager<ResourceByName> m_resourceByNames;
    FactoryInterningManager<Atom> m_atoms;
    FactoryInterningManager<Bind> m_binds;
    FactoryInterningManager<Filter> m_filters;
    FactoryInterningManager<Negation> m_negations;
    FactoryInterningManager<AggregateBind> m_aggregateBinds;
    FactoryInterningManager<Aggregate> m_aggregates;
    FactoryInterningManager<Conjunction> m_conjunctions;
    FactoryInterningManager<Disjunction> m_disjunctions;
    FactoryInterningManager<Optional> m_optionals;
    FactoryInterningManager<Minus> m_minuses;
    FactoryInterningManager<Values> m_values;
    FactoryInterningManager<Query> m_queries;
    FactoryInterningManager<Rule> m_rules;

    _LogicFactory();

    void dispose(const _BuiltinFunctionCall* const object);
    void dispose(const _ExistenceExpression* const object);
    void dispose(const _Variable* const object);
    void dispose(const _ResourceByID* const object);
    void dispose(const _ResourceByName* const object);
    void dispose(const _Predicate* const object);
    void dispose(const _Atom* const object);
    void dispose(const _Bind* const object);
    void dispose(const _Filter* const object);
    void dispose(const _Negation* const object);
    void dispose(const _AggregateBind* const object);
    void dispose(const _Aggregate* const object);
    void dispose(const _Conjunction* const object);
    void dispose(const _Disjunction* const object);
    void dispose(const _Optional* const object);
    void dispose(const _Minus* const object);
    void dispose(const _Values* const object);
    void dispose(const _Query* const object);
    void dispose(const _Rule* const object);

public:

    ~_LogicFactory();

    BuiltinFunctionCall getBuiltinFunctionCall(const std::string& functionName, const std::vector<BuiltinExpression>& arguments);
    BuiltinFunctionCall getBuiltinFunctionCall(const char* const functionName, const std::vector<BuiltinExpression>& arguments);

    ExistenceExpression getExistenceExpression(const bool positive, const Formula& formula);

    Variable getVariable(const std::string& name);
    Variable getVariable(const char* const name);

    ResourceByID getResourceByID(const ResourceID resourceID);

    ResourceByName getResourceByName(const ResourceText& resourceText);
    ResourceByName getResourceByName(const ResourceType resourceType, const std::string& lexicalForm, std::string& datatypeIRI);
    ResourceByName getResourceByName(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI);

    ResourceByName getIRIReference(const std::string& iriReference);
    ResourceByName getIRIReference(const char* const iriReference);

    ResourceByName getBlankNode(const std::string& blankNode);
    ResourceByName getBlankNode(const char* const blankNode);

    ResourceByName getLiteral(const std::string& lexicalForm, const std::string& datatypeIRI);
    ResourceByName getLiteral(const char* const lexicalForm, const char* const datatypeIRI);

    Predicate getPredicate(const std::string& name);
    Predicate getPredicate(const char* const name);
    Predicate getRDFPredicate();

    Atom getAtom(const Predicate& predicate, const std::vector<Term>& arguments);
    Atom getRDFAtom(const Term& subject, const Term& predicate, const Term& object);

    Bind getBind(const BuiltinExpression& builtinExpression, const Term& boundTerm);

    Filter getFilter(const BuiltinExpression& builtinExpression);

    Negation getNegation(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas);

    AggregateBind getAggregateBind(const std::string& functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& result);
    AggregateBind getAggregateBind(const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& result);
    AggregateBind getAggregateBind(const std::string& functionName, const bool distinct, const BuiltinExpression& argument, const Term& result);
    AggregateBind getAggregateBind(const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& result);

    Aggregate getAggregate(const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds);

    Conjunction getConjunction(const std::vector<Formula>& conjuncts);
    Conjunction getConjunction(const Formula& conjunct1, const Formula& conjunct2);

    Disjunction getDisjunction(const std::vector<Formula>& disjuncts);
    Disjunction getDisjunction(const Formula& disjunct1, const Formula& disjunct2);

    Optional getOptional(const Formula& main, const std::vector<Formula>& optionals);
    Optional getOptional(const Formula& main, const Formula& optional);

    Minus getMinus(const Formula& main, const std::vector<Formula>& subtrahends);
    Minus getMinus(const Formula& main, const Formula& subtrahend);

    Values getValues(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data);

    Query getQuery(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula);

    Rule getRule(const Atom& head, const std::vector<Literal>& body);
    Rule getRule(const std::vector<Atom>& head, const std::vector<Literal>& body);

};

always_inline LogicFactory newLogicFactory() {
    return LogicFactory(new _LogicFactory());
}

#endif /* LOGICFACTORY_H_ */
