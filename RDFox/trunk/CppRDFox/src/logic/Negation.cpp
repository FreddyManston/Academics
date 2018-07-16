// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Negation::_Negation(_LogicFactory* const factory, const size_t hash, const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas) :
    _Literal(factory, hash),
    m_existentialVariables(existentialVariables),
    m_atomicFormulas(atomicFormulas)
{
    for (auto atomicFormulaIterator = m_atomicFormulas.begin(); atomicFormulaIterator != m_atomicFormulas.end(); ++atomicFormulaIterator) {
        const std::vector<Term>& atomArguments = (*atomicFormulaIterator)->getArguments();
        for (auto argumentIterator = atomArguments.begin(); argumentIterator != atomArguments.end(); ++argumentIterator)
            if ((*argumentIterator)->getType() == VARIABLE && std::find(m_existentialVariables.begin(), m_existentialVariables.end(), *argumentIterator) == m_existentialVariables.end())
                m_arguments.push_back(*argumentIterator);
    }
}

size_t _Negation::hashCodeFor(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas) {
    size_t result = 0;
    
    for (auto iterator = existentialVariables.begin(); iterator != existentialVariables.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }
    
    for (auto iterator = atomicFormulas.begin(); iterator != atomicFormulas.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Negation::isEqual(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas) const {
    if (m_existentialVariables.size() != existentialVariables.size() || m_atomicFormulas.size() != atomicFormulas.size())
        return false;
    for (auto iterator1 = m_existentialVariables.begin(), iterator2 = existentialVariables.begin(); iterator1 != m_existentialVariables.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    for (auto iterator1 = m_atomicFormulas.begin(), iterator2 = atomicFormulas.begin(); iterator1 != m_atomicFormulas.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

LogicObject _Negation::doClone(const LogicFactory& logicFactory) const {
    std::vector<Variable> newExistentialVariables;
    for (auto iterator = m_existentialVariables.begin(); iterator != m_existentialVariables.end(); ++iterator)
        newExistentialVariables.push_back((*iterator)->clone(logicFactory));
    std::vector<AtomicFormula> newAtomicFormulas;
    for (auto iterator = m_atomicFormulas.begin(); iterator != m_atomicFormulas.end(); ++iterator)
        newAtomicFormulas.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getNegation(newExistentialVariables, newAtomicFormulas);
}

Formula _Negation::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::unordered_set<Variable> freeVariables(getFreeVariables());
    Substitution copySubstitution(substitution);
    for (auto iterator = copySubstitution.begin(); iterator != copySubstitution.end();)
        if (freeVariables.find(iterator->first) == freeVariables.end())
            iterator = copySubstitution.erase(iterator);
        else
            ++iterator;
    std::vector<Variable> newExistentialVariables(m_existentialVariables);
    std::vector<AtomicFormula> newAtomicFormulas;
    for (auto iterator = m_atomicFormulas.begin(); iterator != m_atomicFormulas.end(); ++iterator)
        newAtomicFormulas.push_back((*iterator)->applyEx(copySubstitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    return m_factory->getNegation(newExistentialVariables, newAtomicFormulas);
}

_Negation::~_Negation() {
    m_factory->dispose(this);
}

const std::vector<Variable>& _Negation::getExistentialVariables() const {
    return m_existentialVariables;
}

size_t _Negation::getNumberOfExistentialVariables() const {
    return m_existentialVariables.size();
}

const Variable& _Negation::getExistentialVariable(const size_t index) const {
    return m_existentialVariables[index];
}

const std::vector<AtomicFormula>& _Negation::getAtomicFormulas() const {
    return m_atomicFormulas;
}

size_t _Negation::getNumberOfAtomicFormulas() const {
    return m_atomicFormulas.size();
}

const AtomicFormula& _Negation::getAtomicFormula(const size_t index) const {
    return m_atomicFormulas[index];
}

FormulaType _Negation::getType() const {
    return NEGATION_FORMULA;
}

void _Negation::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        freeVariables.insert(static_pointer_cast<Variable>(*iterator));
}

void _Negation::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Negation(this));
}

std::string _Negation::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "NOT";
    if (!m_existentialVariables.empty()) {
        if (m_existentialVariables.size() == 1)
            buffer << " EXISTS ";
        else
            buffer << " EXIST ";
        for (auto iterator = m_existentialVariables.begin(); iterator != m_existentialVariables.end(); ++iterator) {
            if (iterator != m_existentialVariables.begin())
                buffer << ", ";
            buffer << (*iterator)->toString(prefixes);
        }
        buffer << " IN";
    }
    if (m_atomicFormulas.size() == 1)
        buffer << ' ' << m_atomicFormulas[0]->toString(prefixes);
    else {
        buffer << '(';
        for (auto iterator = m_atomicFormulas.begin(); iterator != m_atomicFormulas.end(); ++iterator) {
            if (iterator != m_atomicFormulas.begin())
                buffer << ", ";
            buffer << (*iterator)->toString(prefixes);
        }
        buffer << ')';
    }
    return buffer.str();
}
