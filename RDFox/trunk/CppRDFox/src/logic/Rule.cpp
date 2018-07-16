// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Rule::_Rule(_LogicFactory* const factory, const size_t hash, const Atom& head, const std::vector<Literal>& body) :
    _Formula(factory, hash),
    m_head(),
    m_body(body)
{
    m_head.push_back(head);
}

_Rule::_Rule(_LogicFactory* const factory, const size_t hash, const std::vector<Atom>& head, const std::vector<Literal>& body) :
    _Formula(factory, hash),
    m_head(head),
    m_body(body)
{
}

size_t _Rule::hashCodeFor(const Atom& head, const std::vector<Literal>& body) {
    size_t result = head->hash();
    result += (result << 10);
    result ^= (result >> 6);

    for (auto iterator = body.begin(); iterator != body.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

size_t _Rule::hashCodeFor(const std::vector<Atom>& head, const std::vector<Literal>& body) {
    size_t result = 0;

    for (auto iterator = head.begin(); iterator != head.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    for (auto iterator = body.begin(); iterator != body.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Rule::isEqual(const Atom& head, const std::vector<Literal>& body) const {
    if (m_head.size() != 1 || m_body.size() != body.size() || m_head[0] != head)
        return false;
    for (auto iterator1 = m_body.begin(), iterator2 = body.begin(); iterator1 != m_body.end(); ++iterator1, ++iterator2)
        if (*iterator1 != *iterator2)
            return false;
    return true;
}

bool _Rule::isEqual(const std::vector<Atom>& head, const std::vector<Literal>& body) const {
    if (m_head.size() != head.size() || m_body.size() != body.size())
        return false;
    for (auto iterator1 = m_head.begin(), iterator2 = head.begin(); iterator1 != m_head.end(); ++iterator1, ++iterator2)
        if (*iterator1 != *iterator2)
            return false;
    for (auto iterator1 = m_body.begin(), iterator2 = body.begin(); iterator1 != m_body.end(); ++iterator1, ++iterator2)
        if (*iterator1 != *iterator2)
            return false;
    return true;
}

LogicObject _Rule::doClone(const LogicFactory& logicFactory) const {
    std::vector<Atom> newHead;
    for (auto iterator = m_head.begin(); iterator != m_head.end(); ++iterator)
        newHead.push_back((*iterator)->clone(logicFactory));
    std::vector<Literal> newBody;
    for (auto iterator = m_body.begin(); iterator != m_body.end(); ++iterator)
        newBody.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getRule(newHead, newBody);
}

Formula _Rule::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    return Rule(this);
}

_Rule::~_Rule() {
    m_factory->dispose(this);
}

const std::vector<Atom>& _Rule::getHead() const {
    return m_head;
}

size_t _Rule::getNumberOfHeadAtoms() const {
    return m_head.size();
}

const Atom& _Rule::getHead(const size_t index) const {
    return m_head[index];
}

const std::vector<Literal>& _Rule::getBody() const {
    return m_body;
}

size_t _Rule::getNumberOfBodyLiterals() const {
    return m_body.size();
}

const Literal& _Rule::getBody(const size_t index) const {
    return m_body[index];
}

FormulaType _Rule::getType() const {
    return RULE_FORMULA;
}

bool _Rule::isGround() const {
    for (auto iterator = m_head.begin(); iterator != m_head.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    for (auto iterator = m_body.begin(); iterator != m_body.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _Rule::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
}

void _Rule::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Rule(this));
}

std::string _Rule::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    bool first = true;
    for (auto iterator = m_head.begin(); iterator != m_head.end(); ++iterator) {
        if (first)
            first = false;
        else
            buffer << ", ";
        buffer << (*iterator)->toString(prefixes);
    }
    buffer << " :- ";
    first = true;
    for (auto iterator = m_body.begin(); iterator != m_body.end(); ++iterator) {
        if (first)
            first = false;
        else
            buffer << ", ";
        buffer << (*iterator)->toString(prefixes);
    }
    buffer << " .";
    return buffer.str();
}
