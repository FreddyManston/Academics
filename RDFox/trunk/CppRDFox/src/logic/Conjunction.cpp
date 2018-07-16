// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Conjunction::_Conjunction(_LogicFactory* const factory, const size_t hash, const std::vector<Formula>& conjuncts) : _Formula(factory, hash), m_conjuncts(conjuncts) {
}

_Conjunction::_Conjunction(_LogicFactory* const factory, const size_t hash, const Formula& conjunct1, const Formula& conjunct2) : _Formula(factory, hash), m_conjuncts() {
    m_conjuncts.push_back(conjunct1);
    m_conjuncts.push_back(conjunct2);
}

size_t _Conjunction::hashCodeFor(const std::vector<Formula>& conjuncts) {
    size_t result = 0;

    for (auto iterator = conjuncts.begin(); iterator != conjuncts.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Conjunction::isEqual(const std::vector<Formula>& conjuncts) const {
    if (m_conjuncts.size() != conjuncts.size())
        return false;
    for (auto iterator1 = m_conjuncts.begin(), iterator2 = conjuncts.begin(); iterator1 != m_conjuncts.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

size_t _Conjunction::hashCodeFor(const Formula& conjunct1, const Formula& conjunct2) {
    size_t result = 0;

    result += conjunct1->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += conjunct2->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Conjunction::isEqual(const Formula& conjunct1, const Formula& conjunct2) const {
    return
        m_conjuncts.size() == 2 &&
        m_conjuncts[0] == conjunct1 &&
        m_conjuncts[1] == conjunct2;
}

LogicObject _Conjunction::doClone(const LogicFactory& logicFactory) const {
    std::vector<Formula> newConjuncts;
    for (auto iterator = m_conjuncts.begin(); iterator != m_conjuncts.end(); ++iterator)
        newConjuncts.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getConjunction(newConjuncts);
}

Formula _Conjunction::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<Formula> newConjuncts;
    for (auto iterator = m_conjuncts.begin(); iterator != m_conjuncts.end(); ++iterator)
        newConjuncts.push_back((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    return m_factory->getConjunction(newConjuncts);
}

_Conjunction::~_Conjunction() {
    m_factory->dispose(this);
}

const std::vector<Formula>& _Conjunction::getConjuncts() const {
    return m_conjuncts;
}

size_t _Conjunction::getNumberOfConjuncts() const {
    return m_conjuncts.size();
}

const Formula& _Conjunction::getConjunct(const size_t index) const {
    return m_conjuncts[index];
}

FormulaType _Conjunction::getType() const {
    return CONJUNCTION_FORMULA;
}

bool _Conjunction::isGround() const {
    for (auto iterator = m_conjuncts.begin(); iterator != m_conjuncts.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _Conjunction::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_conjuncts.begin(); iterator != m_conjuncts.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _Conjunction::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Conjunction(this));
}

std::string _Conjunction::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "(and";
    for (auto iterator = m_conjuncts.begin(); iterator != m_conjuncts.end(); ++iterator)
        buffer << " " << (*iterator)->toString(prefixes);
    buffer << ")";
    return buffer.str();
}
