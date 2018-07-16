// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Disjunction::_Disjunction(_LogicFactory* const factory, const size_t hash, const std::vector<Formula>& disjuncts) : _Formula(factory, hash), m_disjuncts(disjuncts) {
}

_Disjunction::_Disjunction(_LogicFactory* const factory, const size_t hash, const Formula& disjunct1, const Formula& disjunct2) : _Formula(factory, hash), m_disjuncts() {
    m_disjuncts.push_back(disjunct1);
    m_disjuncts.push_back(disjunct2);
}

size_t _Disjunction::hashCodeFor(const std::vector<Formula>& disjuncts) {
    size_t result = 0;

    for (auto iterator = disjuncts.begin(); iterator != disjuncts.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Disjunction::isEqual(const std::vector<Formula>& disjuncts) const {
    if (m_disjuncts.size() != disjuncts.size())
        return false;
    for (auto iterator1 = m_disjuncts.begin(), iterator2 = disjuncts.begin(); iterator1 != m_disjuncts.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

size_t _Disjunction::hashCodeFor(const Formula& disjunct1, const Formula& disjunct2) {
    size_t result = 0;

    result += disjunct1->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += disjunct2->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Disjunction::isEqual(const Formula& disjunct1, const Formula& disjunct2) const {
    return
        m_disjuncts.size() == 2 &&
        m_disjuncts[0] == disjunct1 &&
        m_disjuncts[1] == disjunct2;
}

LogicObject _Disjunction::doClone(const LogicFactory& logicFactory) const {
    std::vector<Formula> newDisjuncts;
    for (auto iterator = m_disjuncts.begin(); iterator != m_disjuncts.end(); ++iterator)
        newDisjuncts.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getDisjunction(newDisjuncts);
}

Formula _Disjunction::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<Formula> newDisjuncts;
    for (auto iterator = m_disjuncts.begin(); iterator != m_disjuncts.end(); ++iterator)
        newDisjuncts.push_back((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    return m_factory->getDisjunction(newDisjuncts);
}

_Disjunction::~_Disjunction() {
    m_factory->dispose(this);
}

const std::vector<Formula>& _Disjunction::getDisjuncts() const {
    return m_disjuncts;
}

size_t _Disjunction::getNumberOfDisjuncts() const {
    return m_disjuncts.size();
}

const Formula& _Disjunction::getDisjunct(const size_t index) const {
    return m_disjuncts[index];
}

FormulaType _Disjunction::getType() const {
    return DISJUNCTION_FORMULA;
}

bool _Disjunction::isGround() const {
    for (auto iterator = m_disjuncts.begin(); iterator != m_disjuncts.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _Disjunction::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_disjuncts.begin(); iterator != m_disjuncts.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _Disjunction::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Disjunction(this));
}

std::string _Disjunction::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "(or";
    for (auto iterator = m_disjuncts.begin(); iterator != m_disjuncts.end(); ++iterator)
        buffer << " " << (*iterator)->toString(prefixes);
    buffer << ")";
    return buffer.str();
}
