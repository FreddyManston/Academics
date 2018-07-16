// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Values::_Values(_LogicFactory* const factory, const size_t hash, const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data) :
    _Formula(factory, hash),
    m_variables(variables),
    m_data(data)
{
}

size_t _Values::hashCodeFor(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data) {
    size_t result = 0;

    for (std::vector<Variable>::const_iterator iterator = variables.begin(); iterator != variables.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    for (std::vector<std::vector<GroundTerm> >::const_iterator iterator1 = data.begin(); iterator1 != data.end(); ++iterator1) {
        const std::vector<GroundTerm>& dataTuple = *iterator1;
        for (std::vector<GroundTerm>::const_iterator iterator2 = dataTuple.begin(); iterator2 != dataTuple.end(); ++iterator2) {
            result += (*iterator2)->hash();
            result += (result << 10);
            result ^= (result >> 6);
        }
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Values::isEqual(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data) const {
    if (m_variables.size() != variables.size() || m_data.size() != data.size())
        return false;
    std::vector<Variable>::const_iterator variableIterator1 = m_variables.begin();
    std::vector<Variable>::const_iterator variableIterator2 = variables.begin();
    while (variableIterator1 != m_variables.end()) {
        if (*variableIterator1 != *variableIterator2)
            return false;
        ++variableIterator1;
        ++variableIterator2;
    }
    std::vector<std::vector<GroundTerm> >::const_iterator dataIterator1 = m_data.begin();
    std::vector<std::vector<GroundTerm> >::const_iterator dataIterator2 = data.begin();
    while (dataIterator1 != m_data.end()) {
        const std::vector<GroundTerm>& dataTuple1 = *dataIterator1;
        const std::vector<GroundTerm>& dataTuple2 = *dataIterator2;
        if (dataTuple1.size() != dataTuple2.size())
            return false;
        std::vector<GroundTerm>::const_iterator dataTupleIterator1 = dataTuple1.begin();
        std::vector<GroundTerm>::const_iterator dataTupleIterator2 = dataTuple2.begin();
        while (dataTupleIterator1 != dataTuple1.end()) {
            if (*dataTupleIterator1 != *dataTupleIterator2)
                return false;
            ++dataTupleIterator1;
            ++dataTupleIterator2;
        }
        ++dataIterator1;
        ++dataIterator2;
    }
    return true;
}

LogicObject _Values::doClone(const LogicFactory& logicFactory) const {
    std::vector<Variable> variables;
    for (std::vector<Variable>::const_iterator iterator = m_variables.begin(); iterator != m_variables.end(); ++iterator)
        variables.push_back((*iterator)->clone(logicFactory));
    std::vector<std::vector<GroundTerm> > data;
    for (std::vector<std::vector<GroundTerm> >::const_iterator iterator1 = m_data.begin(); iterator1 != m_data.end(); ++iterator1) {
        const std::vector<GroundTerm>& dataTuple = *iterator1;
        data.emplace_back();
        for (std::vector<GroundTerm>::const_iterator iterator2 = dataTuple.begin(); iterator2 != dataTuple.end(); ++iterator2)
            data.back().push_back((*iterator2)->clone(logicFactory));
    }
    return logicFactory->getValues(variables, data);
}

Formula _Values::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<Variable> newVariables;
    newVariables.reserve(m_variables.size());
    std::vector<GroundTerm> checkGroundTermEquality;
    bool hasGroundTermEqualityToCheck = false;
    for (std::vector<Variable>::const_iterator iterator = m_variables.begin(); iterator != m_variables.end(); ++iterator) {
        Substitution::const_iterator termIterator = substitution.find(*iterator);
        if (termIterator == substitution.end()) {
            newVariables.push_back(*iterator);
            checkGroundTermEquality.push_back(GroundTerm());
        }
        else if (termIterator->second->getType() == VARIABLE) {
            newVariables.push_back(static_pointer_cast<Variable>(termIterator->second));
            checkGroundTermEquality.push_back(GroundTerm());
        }
        else {
            checkGroundTermEquality.push_back(static_pointer_cast<GroundTerm>(termIterator->second));
            hasGroundTermEqualityToCheck = true;
        }
    }
    if (hasGroundTermEqualityToCheck) {
        std::vector<std::vector<GroundTerm> > newData;
        for (std::vector<std::vector<GroundTerm> >::const_iterator dataIterator = m_data.begin(); dataIterator != m_data.end(); ++dataIterator) {
            const std::vector<GroundTerm>& dataTuple = *dataIterator;
            std::vector<GroundTerm> newDataTuple;
            const size_t minSize = std::min(checkGroundTermEquality.size(), dataTuple.size());
            bool passes = true;
            for (size_t index = 0; index < minSize && passes; ++index) {
                if (checkGroundTermEquality[index].get() == 0)
                    newDataTuple.push_back(dataTuple[index]);
                else if (checkGroundTermEquality[index] != dataTuple[index])
                    passes = false;
            }
            if (passes)
                newData.push_back(newDataTuple);
        }
        return m_factory->getValues(newVariables, newData);
    }
    else
        return m_factory->getValues(newVariables, m_data);
}

_Values::~_Values() {
    m_factory->dispose(this);
}

const std::vector<Variable>& _Values::getVariables() const {
    return m_variables;
}

size_t _Values::getNumberOfVariables() const {
    return m_variables.size();
}

const Variable& _Values::getVariable(const size_t index) const {
    return m_variables[index];
}

const std::vector<std::vector<GroundTerm> >& _Values::getData() const {
    return m_data;
}

size_t _Values::getNumberOfDataTuples() const {
    return m_data.size();
}

const std::vector<GroundTerm>& _Values::getDataTuple(const size_t tupleIndex) const {
    return m_data[tupleIndex];
}

const GroundTerm& _Values::getDataTupleElement(const size_t tupleIndex, const size_t elementIndex) const {
    return m_data[tupleIndex][elementIndex];
}

FormulaType _Values::getType() const {
    return VALUES_FORMULA;
}

bool _Values::isGround() const {
    return m_variables.empty();
}

void _Values::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (std::vector<Variable>::const_iterator iterator = m_variables.begin(); iterator != m_variables.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _Values::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Values(this));
}

std::string _Values::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "(values (";
    bool first = true;
    for (std::vector<Variable>::const_iterator iterator = m_variables.begin(); iterator != m_variables.end(); ++iterator) {
        if (first)
            first = false;
        else
            buffer << " ";
        buffer << (*iterator)->toString(prefixes);
    }
    buffer << ")";
    for (std::vector<std::vector<GroundTerm> >::const_iterator dataIterator = m_data.begin(); dataIterator != m_data.end(); ++dataIterator) {
        const std::vector<GroundTerm>& dataTuple = *dataIterator;
        buffer << " (";
        first = true;
        for (std::vector<GroundTerm>::const_iterator dataTupleIterator = dataTuple.begin(); dataTupleIterator != dataTuple.end(); ++dataTupleIterator) {
            if (first)
                first = false;
            else
                buffer << " ";
            (*dataTupleIterator)->toString(prefixes);
        }
        buffer << ")";
    }
    buffer << ")";
    return buffer.str();
}
