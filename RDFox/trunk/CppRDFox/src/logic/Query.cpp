// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Query::_Query(_LogicFactory* const factory, const size_t hash, const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula) : _Formula(factory, hash), m_distinct(distinct), m_answerTerms(answerTerms), m_queryFormula(queryFormula) {
}

size_t _Query::hashCodeFor(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula) {
    size_t result = (distinct ? 31 : 0);

    for (auto iterator = answerTerms.begin(); iterator != answerTerms.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += queryFormula->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Query::isEqual(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula) const {
    size_t size;
    if (m_distinct != distinct || m_queryFormula != queryFormula || (size = m_answerTerms.size()) != answerTerms.size())
        return false;
    for (size_t index = 0; index < size; index++)
        if (m_answerTerms[index] != answerTerms[index])
            return false;
    return true;
}

LogicObject _Query::doClone(const LogicFactory& logicFactory) const {
    std::vector<Term> newAnswerTerms;
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator)
        newAnswerTerms.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getQuery(m_distinct, newAnswerTerms, m_queryFormula->clone(logicFactory));
}

Formula _Query::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<Term> newAnswerTerms;
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator)
        newAnswerTerms.push_back(static_pointer_cast<Term>((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter)));
    std::unordered_set<Variable> freeVariables(getFreeVariables());
    Substitution copySubstitution(substitution);
    for (auto iterator = copySubstitution.begin(); iterator != copySubstitution.end();)
        if (freeVariables.find(iterator->first) == freeVariables.end())
            iterator = copySubstitution.erase(iterator);
        else
            ++iterator;
    if (renameImplicitExistentialVariables) {
        bool hasIncreasedCounter = false;
        std::unordered_set<Variable> queryFormulaFreeVariables = m_queryFormula->getFreeVariables();
        for (auto iterator = queryFormulaFreeVariables.begin(); iterator != queryFormulaFreeVariables.end(); ++iterator) {
            if (freeVariables.find(*iterator) == freeVariables.end()) {
                if (!hasIncreasedCounter) {
                    ++formulaWithImplicitExistentialVariablesCounter;
                    hasIncreasedCounter = true;
                }
                std::ostringstream newVariableName;
                newVariableName << "__SQ" << formulaWithImplicitExistentialVariablesCounter << "__" << (*iterator)->getName();
                copySubstitution[*iterator] = m_factory->getVariable(newVariableName.str());
            }
        }
    }
    return m_factory->getQuery(m_distinct, newAnswerTerms, m_queryFormula->applyEx(copySubstitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
}

_Query::~_Query() {
    m_factory->dispose(this);
}

bool _Query::isDistinct() const {
    return m_distinct;
}

const std::vector<Term>& _Query::getAnswerTerms() const {
    return m_answerTerms;
}

size_t _Query::getNumberOfAnswerTerms() const {
    return m_answerTerms.size();
}

const Term& _Query::getAnswerTerm(const size_t index) const {
    return m_answerTerms[index];
}

const Formula& _Query::getQueryFormula() const {
    return m_queryFormula;
}

FormulaType _Query::getType() const {
    return QUERY_FORMULA;
}

bool _Query::isGround() const {
    return m_queryFormula->isGround();
}

void _Query::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _Query::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Query(this));
}

std::string _Query::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "SELECT";
    if (m_distinct)
        buffer << " DISTINCT";
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator)
        buffer << " " << (*iterator)->toString(prefixes);
    buffer << " WHERE { " << m_queryFormula->toString(prefixes) << " }";
    return buffer.str();
}
