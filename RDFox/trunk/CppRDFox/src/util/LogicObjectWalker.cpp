// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../logic/Logic.h"
#include "LogicObjectWalker.h"

void LogicObjectWalker::visit(const Predicate& object) {
}

void LogicObjectWalker::visit(const BuiltinFunctionCall& object) {
    const std::vector<BuiltinExpression>& arguments = object->getArguments();
    for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const ExistenceExpression& object) {
    object->getFormula()->accept(*this);
}

void LogicObjectWalker::visit(const Variable& object) {
}

void LogicObjectWalker::visit(const ResourceByID& object) {
}

void LogicObjectWalker::visit(const ResourceByName& object) {
}

void LogicObjectWalker::visit(const Atom& object) {
    object->getPredicate()->accept(*this);
    const std::vector<Term>& arguments = object->getArguments();
    for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const Bind& object) {
    object->getBoundTerm()->accept(*this);
    object->getBuiltinExpression()->accept(*this);
}

void LogicObjectWalker::visit(const Filter& object) {
    object->getBuiltinExpression()->accept(*this);
}

void LogicObjectWalker::visit(const Negation& object) {
    const std::vector<Variable>& existentialVariables = object->getExistentialVariables();
    for (auto iterator = existentialVariables.begin(); iterator != existentialVariables.end(); ++iterator)
        (*iterator)->accept(*this);
    const std::vector<AtomicFormula>& atomicFormulas = object->getAtomicFormulas();
    for (auto iterator = atomicFormulas.begin(); iterator != atomicFormulas.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const AggregateBind& object) {
    const std::vector<BuiltinExpression>& conjuncts = object->getArguments();
    for (auto iterator = conjuncts.begin(); iterator != conjuncts.end(); ++iterator)
        (*iterator)->accept(*this);
    object->getBoundTerm()->accept(*this);
}

void LogicObjectWalker::visit(const Aggregate& object) {
    const std::vector<AtomicFormula>& atomicFormulas = object->getAtomicFormulas();
    for (auto iterator = atomicFormulas.begin(); iterator != atomicFormulas.end(); ++iterator)
        (*iterator)->accept(*this);
    const std::vector<Variable>& groupVariables = object->getGroupVariables();
    for (auto iterator = groupVariables.begin(); iterator != groupVariables.end(); ++iterator)
        (*iterator)->accept(*this);
    const std::vector<AggregateBind>& aggregateBinds = object->getAggregateBinds();
    for (auto iterator = aggregateBinds.begin(); iterator != aggregateBinds.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const Conjunction& object) {
    const std::vector<Formula>& conjuncts = object->getConjuncts();
    for (auto iterator = conjuncts.begin(); iterator != conjuncts.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const Disjunction& object) {
    const std::vector<Formula>& disjuncts = object->getDisjuncts();
    for (auto iterator = disjuncts.begin(); iterator != disjuncts.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const Optional& object) {
    object->getMain()->accept(*this);
    const std::vector<Formula>& optionals = object->getOptionals();
    for (auto iterator = optionals.begin(); iterator != optionals.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const Minus& object) {
    object->getMain()->accept(*this);
    const std::vector<Formula>& subtrahends = object->getSubtrahends();
    for (auto iterator = subtrahends.begin(); iterator != subtrahends.end(); ++iterator)
        (*iterator)->accept(*this);
}

void LogicObjectWalker::visit(const Values& object) {
    const std::vector<Variable>& variables = object->getVariables();
    for (auto iterator = variables.begin(); iterator != variables.end(); ++iterator)
        (*iterator)->accept(*this);
    const std::vector<std::vector<GroundTerm> >& data = object->getData();
    for (auto dataIterator = data.begin(); dataIterator != data.end(); ++dataIterator) {
        const std::vector<GroundTerm>& dataTuple = *dataIterator;
        for (auto dataTupleIterator = dataTuple.begin(); dataTupleIterator != dataTuple.end(); ++dataTupleIterator)
            (*dataTupleIterator)->accept(*this);
    }
}

void LogicObjectWalker::visit(const Query& object) {
    const std::vector<Term>& answerTerms = object->getAnswerTerms();
    for (auto iterator = answerTerms.begin(); iterator != answerTerms.end(); ++iterator)
        (*iterator)->accept(*this);
    object->getQueryFormula()->accept(*this);
}

void LogicObjectWalker::visit(const Rule& object) {
    const std::vector<Atom>& head = object->getHead();
    for (auto iterator = head.begin(); iterator != head.end(); ++iterator)
        (*iterator)->accept(*this);
    const std::vector<Literal>& body = object->getBody();
    for (auto iterator = body.begin(); iterator != body.end(); ++iterator)
        (*iterator)->accept(*this);
}
