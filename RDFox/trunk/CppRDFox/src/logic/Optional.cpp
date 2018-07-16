// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Optional::_Optional(_LogicFactory* const factory, const size_t hash, const Formula& main, const std::vector<Formula>& optionals) : _Formula(factory, hash), m_main(main), m_optionals(optionals) {
}

_Optional::_Optional(_LogicFactory* const factory, const size_t hash, const Formula& main, const Formula& optional) : _Formula(factory, hash), m_main(main), m_optionals() {
    m_optionals.push_back(optional);
}

size_t _Optional::hashCodeFor(const Formula& main, const std::vector<Formula>& optionals) {
    size_t result = 0;

    result += main->hash();
    result += (result << 10);
    result ^= (result >> 6);

    for (auto iterator = optionals.begin(); iterator != optionals.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Optional::isEqual(const Formula& main, const std::vector<Formula>& optionals) const {
    if (m_main != main || m_optionals.size() != optionals.size())
        return false;
    for (auto iterator1 = m_optionals.begin(), iterator2 = optionals.begin(); iterator1 != m_optionals.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

size_t _Optional::hashCodeFor(const Formula& main, const Formula& optional) {
    size_t result = 0;

    result += main->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += optional->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Optional::isEqual(const Formula& main, const Formula& optional) const {
    return
        m_main == main &&
        m_optionals.size() == 1 &&
        m_optionals[0] == optional;
}

LogicObject _Optional::doClone(const LogicFactory& logicFactory) const {
    std::vector<Formula> optionals;
    for (auto iterator = m_optionals.begin(); iterator != m_optionals.end(); ++iterator)
        optionals.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getOptional(m_main->clone(logicFactory), optionals);
}

Formula _Optional::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    Formula newMain = m_main->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    std::vector<Formula> newOptionals;
    for (auto iterator = m_optionals.begin(); iterator != m_optionals.end(); ++iterator)
        newOptionals.push_back((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    return m_factory->getOptional(newMain, newOptionals);
}

_Optional::~_Optional() {
    m_factory->dispose(this);
}

const Formula& _Optional::getMain() const {
    return m_main;
}

const std::vector<Formula>& _Optional::getOptionals() const {
    return m_optionals;
}

size_t _Optional::getNumberOfOptionals() const {
    return m_optionals.size();
}

const Formula& _Optional::getOptional(const size_t index) const {
    return m_optionals[index];
}

FormulaType _Optional::getType() const {
    return OPTIONAL_FORMULA;
}

bool _Optional::isGround() const {
    if (!m_main->isGround())
        return false;
    for (auto iterator = m_optionals.begin(); iterator != m_optionals.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _Optional::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    m_main->getFreeVariablesEx(freeVariables);
    for (auto iterator = m_optionals.begin(); iterator != m_optionals.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _Optional::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Optional(this));
}

std::string _Optional::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "(optional " << m_main->toString(prefixes);
    for (auto iterator = m_optionals.begin(); iterator != m_optionals.end(); ++iterator)
        buffer << " " << (*iterator)->toString(prefixes);
    buffer << ")";
    return buffer.str();
}
