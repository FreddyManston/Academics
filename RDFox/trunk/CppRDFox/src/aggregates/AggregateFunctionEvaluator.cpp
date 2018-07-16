// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "AggregateFunctionEvaluator.h"

// AggregateFunctionDescriptor

AggregateFunctionDescriptor::AggregateFunctionDescriptor(const char* const functionName) : m_functionName(functionName) {
    AggregateFunctionEvaluator::getAggregateFunctionDescriptors()[m_functionName] = this;
}

AggregateFunctionDescriptor::~AggregateFunctionDescriptor() {
}

// AggregateFunctionEvaluator

std::unordered_map<std::string, AggregateFunctionDescriptor*>& AggregateFunctionEvaluator::getAggregateFunctionDescriptors() {
    static std::unordered_map<std::string, AggregateFunctionDescriptor*> s_aggregateFunctionDescriptors;
    return s_aggregateFunctionDescriptors;
}

std::unique_ptr<AggregateFunctionEvaluator> AggregateFunctionEvaluator::createAggregateFunctionEvaluator(const std::string& functionName, const size_t numberOfArguments) {
    const std::unordered_map<std::string, AggregateFunctionDescriptor*>& aggregateFunctionDescriptors = getAggregateFunctionDescriptors();
    std::unordered_map<std::string, AggregateFunctionDescriptor*>::const_iterator iterator = aggregateFunctionDescriptors.find(functionName);
    if (iterator == aggregateFunctionDescriptors.end()) {
        std::ostringstream message;
        message << "Aggregate function with IRI '" << functionName << "' does not exist.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
    else
        return iterator->second->createAggregateFunctionEvaluator(numberOfArguments);
}

AggregateFunctionEvaluator::~AggregateFunctionEvaluator() {
}
