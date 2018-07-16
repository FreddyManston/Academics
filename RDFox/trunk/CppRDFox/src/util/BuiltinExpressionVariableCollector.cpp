// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../logic/Logic.h"
#include "BuiltinExpressionVariableCollector.h"

BuiltinExpressionVariableCollector::BuiltinExpressionVariableCollector(std::vector<Term>& termsArray) : m_termsArray(termsArray), m_termsSet() {
}

void BuiltinExpressionVariableCollector::visit(const Variable& object) {
    if (m_termsSet.insert(object).second)
        m_termsArray.push_back(object);
}

void BuiltinExpressionVariableCollector::visit(const Query& object) {
    const std::vector<Term>& answerTerms = object->getAnswerTerms();
    for (std::vector<Term>::const_iterator iterator = answerTerms.begin(); iterator != answerTerms.end(); ++iterator)
        if ((*iterator)->getType() == VARIABLE && m_termsSet.insert(*iterator).second)
            m_termsArray.push_back(*iterator);
}

void BuiltinExpressionVariableCollector::visit(const Rule& object) {
}
