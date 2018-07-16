// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Aggregate::_Aggregate(_LogicFactory* const factory, const size_t hash, const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds) :
    _Literal(factory, hash),
    m_atomicFormulas(atomicFormulas),
    m_skipEmptyGroups(skipEmptyGroups),
    m_groupVariables(groupVariables),
    m_aggregateBinds(aggregateBinds)
{
    for (auto iterator = m_groupVariables.begin(); iterator != m_groupVariables.end(); ++iterator)
        m_arguments.push_back(*iterator);
    for (auto iterator = m_aggregateBinds.begin(); iterator != m_aggregateBinds.end(); ++iterator)
        m_arguments.push_back((*iterator)->getBoundTerm());
}

size_t _Aggregate::hashCodeFor(const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& on, const std::vector<AggregateBind>& aggregateBinds) {
    size_t result = 0;
    
    for (auto iterator = atomicFormulas.begin(); iterator != atomicFormulas.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }
    
    result += (skipEmptyGroups ? 11 : 0);
    result += (result << 10);
    result ^= (result >> 6);

    for (auto iterator = on.begin(); iterator != on.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    for (auto iterator = aggregateBinds.begin(); iterator != aggregateBinds.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Aggregate::isEqual(const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds) const {
    if (m_atomicFormulas.size() != atomicFormulas.size() || m_skipEmptyGroups != skipEmptyGroups || m_groupVariables.size() != groupVariables.size() || m_aggregateBinds.size() != aggregateBinds.size())
        return false;
    for (auto iterator1 = m_atomicFormulas.begin(), iterator2 = atomicFormulas.begin(); iterator1 != m_atomicFormulas.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    for (auto iterator1 = m_groupVariables.begin(), iterator2 = groupVariables.begin(); iterator1 != m_groupVariables.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    for (auto iterator1 = m_aggregateBinds.begin(), iterator2 = aggregateBinds.begin(); iterator1 != m_aggregateBinds.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

LogicObject _Aggregate::doClone(const LogicFactory& logicFactory) const {
    std::vector<AtomicFormula> newAtomicFormulas;
    for (auto iterator = m_atomicFormulas.begin(); iterator != m_atomicFormulas.end(); ++iterator)
        newAtomicFormulas.push_back((*iterator)->clone(logicFactory));
    std::vector<Variable> newGroupVariables;
    for (auto iterator = m_groupVariables.begin(); iterator != m_groupVariables.end(); ++iterator)
        newGroupVariables.push_back((*iterator)->clone(logicFactory));
    std::vector<AggregateBind> newAggregateBinds;
    for (auto iterator = m_aggregateBinds.begin(); iterator != m_aggregateBinds.end(); ++iterator)
        newAggregateBinds.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getAggregate(newAtomicFormulas, m_skipEmptyGroups, newGroupVariables, newAggregateBinds);
}

Formula _Aggregate::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::unordered_set<Variable> freeVariables(getFreeVariables());
    Substitution copySubstitution(substitution);
    for (auto iterator = copySubstitution.begin(); iterator != copySubstitution.end();)
        if (freeVariables.find(iterator->first) == freeVariables.end())
            iterator = copySubstitution.erase(iterator);
        else
            ++iterator;
    std::vector<AtomicFormula> newAtomicFormulas;
    for (auto iterator = m_atomicFormulas.begin(); iterator != m_atomicFormulas.end(); ++iterator)
        newAtomicFormulas.push_back((*iterator)->applyEx(copySubstitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    std::vector<Variable> newGroupVariables;
    for (auto iterator = m_groupVariables.begin(); iterator != m_groupVariables.end(); ++iterator) {
        Term term = (*iterator)->apply(copySubstitution);
        if (term->getType() == VARIABLE)
            newGroupVariables.push_back(static_pointer_cast<Variable>(term));
    }
    std::vector<AggregateBind> newAggregateBinds;
    for (auto iterator = m_aggregateBinds.begin(); iterator != m_aggregateBinds.end(); ++iterator)
        newAggregateBinds.push_back((*iterator)->apply(copySubstitution));
    return m_factory->getAggregate(newAtomicFormulas, m_skipEmptyGroups, newGroupVariables, newAggregateBinds);
}

_Aggregate::~_Aggregate() {
    m_factory->dispose(this);
}

const std::vector<AtomicFormula>& _Aggregate::getAtomicFormulas() const {
    return m_atomicFormulas;
}

size_t _Aggregate::getNumberOfAtomicFormulas() const {
    return m_atomicFormulas.size();
}

const AtomicFormula& _Aggregate::getAtomicFormula(const size_t index) const {
    return m_atomicFormulas[index];
}

const std::vector<Variable>& _Aggregate::getGroupVariables() const {
    return m_groupVariables;
}

size_t _Aggregate::getNumberOfGroupVariables() const {
    return m_groupVariables.size();
}

const Variable& _Aggregate::getGroupVariable(const size_t index) const {
    return m_groupVariables[index];
}

bool _Aggregate::getSkipEmptyGroups() const {
    return m_skipEmptyGroups;
}

const std::vector<AggregateBind>& _Aggregate::getAggregateBinds() const {
    return m_aggregateBinds;
}

size_t _Aggregate::getNumberOfAggregateBinds() const {
    return m_aggregateBinds.size();
}

const AggregateBind& _Aggregate::getAggregateBind(const size_t index) const {
    return m_aggregateBinds[index];
}

FormulaType _Aggregate::getType() const {
    return AGGREGATE_FORMULA;
}

bool _Aggregate::isGround() const {
    if (!m_groupVariables.empty())
        return false;
    for (auto iterator = m_aggregateBinds.begin(); iterator != m_aggregateBinds.end(); ++iterator)
        if (!(*iterator)->getBoundTerm()->isGround())
            return false;
    return true;
}

void _Aggregate::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_groupVariables.begin(); iterator != m_groupVariables.end(); ++iterator)
        freeVariables.insert(*iterator);
    for (auto iterator = m_aggregateBinds.begin(); iterator != m_aggregateBinds.end(); ++iterator)
        (*iterator)->getBoundTerm()->getFreeVariablesEx(freeVariables);
}

void _Aggregate::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Aggregate(this));
}

std::string _Aggregate::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "AGGREGATE(";
    for (auto iterator = m_atomicFormulas.begin(); iterator != m_atomicFormulas.end(); ++iterator) {
        if (iterator != m_atomicFormulas.begin())
            buffer << ", ";
        buffer << (*iterator)->toString(prefixes);
    }
    buffer << " ON";
    if (m_skipEmptyGroups)
        buffer << " NOT EMPTY";
    for (auto iterator = m_groupVariables.begin(); iterator != m_groupVariables.end(); ++iterator)
        buffer << ' ' << (*iterator)->toString(prefixes);
    for (auto iterator = m_aggregateBinds.begin(); iterator != m_aggregateBinds.end(); ++iterator)
        buffer << ' ' << (*iterator)->toString(prefixes);
    buffer << ')';
    return buffer.str();
}
