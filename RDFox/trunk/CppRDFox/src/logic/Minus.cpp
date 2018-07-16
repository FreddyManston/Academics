// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Minus::_Minus(_LogicFactory* const factory, const size_t hash, const Formula& main, const std::vector<Formula>& subtrahends) : _Formula(factory, hash), m_main(main), m_subtrahends(subtrahends) {
}

_Minus::_Minus(_LogicFactory* const factory, const size_t hash, const Formula& main, const Formula& subtrahend) : _Formula(factory, hash), m_main(main), m_subtrahends() {
    m_subtrahends.push_back(subtrahend);
}

size_t _Minus::hashCodeFor(const Formula& main, const std::vector<Formula>& subtrahends) {
    size_t result = 0;

    result += main->hash();
    result += (result << 10);
    result ^= (result >> 6);

    for (auto iterator = subtrahends.begin(); iterator != subtrahends.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Minus::isEqual(const Formula& main, const std::vector<Formula>& subtrahends) const {
    if (m_main != main || m_subtrahends.size() != subtrahends.size())
        return false;
    for (auto iterator1 = m_subtrahends.begin(), iterator2 = subtrahends.begin(); iterator1 != m_subtrahends.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

size_t _Minus::hashCodeFor(const Formula& main, const Formula& subtrahend) {
    size_t result = 0;

    result += main->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += subtrahend->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Minus::isEqual(const Formula& main, const Formula& subtrahend) const {
    return
        m_main == main &&
        m_subtrahends.size() == 1 &&
        m_subtrahends[0] == subtrahend;
}

LogicObject _Minus::doClone(const LogicFactory& logicFactory) const {
    std::vector<Formula> optionals;
    for (auto iterator = m_subtrahends.begin(); iterator != m_subtrahends.end(); ++iterator)
        optionals.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getOptional(m_main->clone(logicFactory), optionals);
}

Formula _Minus::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    Formula newMain = m_main->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    std::unordered_set<Variable> freeVariables;
    getFreeVariablesEx(freeVariables);
    Substitution freeVariablesSubstitution(substitution);
    for (auto iterator = freeVariablesSubstitution.begin(); iterator != freeVariablesSubstitution.end();)
        if (freeVariables.find(iterator->first) == freeVariables.end())
            iterator = freeVariablesSubstitution.erase(iterator);
        else
            ++iterator;
    std::vector<Formula> newSubtrahends;
    newSubtrahends.reserve(m_subtrahends.size());
    if (renameImplicitExistentialVariables) {
        std::unordered_set<Variable> subtrahendFreeVariables;
        for (auto subtrahendIterator = m_subtrahends.begin(); subtrahendIterator != m_subtrahends.end(); ++subtrahendIterator) {
            subtrahendFreeVariables.clear();
            (*subtrahendIterator)->getFreeVariablesEx(subtrahendFreeVariables);
            Substitution subtrahendSubstitution(freeVariablesSubstitution);
            bool hasIncreasedCounter = false;
            for (auto iterator = subtrahendFreeVariables.begin(); iterator != subtrahendFreeVariables.end(); ++iterator) {
                if (freeVariables.find(*iterator) == freeVariables.end()) {
                    if (!hasIncreasedCounter) {
                        ++formulaWithImplicitExistentialVariablesCounter;
                        hasIncreasedCounter = true;
                    }
                    std::ostringstream newVariableName;
                    newVariableName << "__SM" << formulaWithImplicitExistentialVariablesCounter << "__" << (*iterator)->getName();
                    subtrahendSubstitution[*iterator] = m_factory->getVariable(newVariableName.str());
                }
            }
            newSubtrahends.push_back((*subtrahendIterator)->applyEx(subtrahendSubstitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
        }
    }
    else {
        for (auto subtrahendIterator = m_subtrahends.begin(); subtrahendIterator != m_subtrahends.end(); ++subtrahendIterator)
            newSubtrahends.push_back((*subtrahendIterator)->applyEx(freeVariablesSubstitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }
    return m_factory->getMinus(newMain, newSubtrahends);
}

_Minus::~_Minus() {
    m_factory->dispose(this);
}

const Formula& _Minus::getMain() const {
    return m_main;
}

const std::vector<Formula>& _Minus::getSubtrahends() const {
    return m_subtrahends;
}

size_t _Minus::getNumberOfSubtrahends() const {
    return m_subtrahends.size();
}

const Formula& _Minus::getSubtrahend(const size_t index) const {
    return m_subtrahends[index];
}

FormulaType _Minus::getType() const {
    return MINUS_FORMULA;
}

bool _Minus::isGround() const {
    if (!m_main->isGround())
        return false;
    for (auto iterator = m_subtrahends.begin(); iterator != m_subtrahends.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _Minus::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    m_main->getFreeVariablesEx(freeVariables);
}

void _Minus::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Minus(this));
}

std::string _Minus::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "(minus " << m_main->toString(prefixes);
    for (auto iterator = m_subtrahends.begin(); iterator != m_subtrahends.end(); ++iterator)
        buffer << " " << (*iterator)->toString(prefixes);
    buffer << ")";
    return buffer.str();
}
