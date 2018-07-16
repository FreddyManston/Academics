// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef AGGREGATEFUNCTIONEVALUATOR_H_
#define AGGREGATEFUNCTIONEVALUATOR_H_

#include "../RDFStoreException.h"
#include "../Common.h"

class CloneReplacements;
class ThreadContext;

// AggregateFunctionDescriptor

class AggregateFunctionEvaluator;

class AggregateFunctionDescriptor : private Unmovable {

protected:

    const std::string m_functionName;

public:

    AggregateFunctionDescriptor(const char* const functionName);

    virtual ~AggregateFunctionDescriptor();

    always_inline const std::string& getFunctionName() const {
        return m_functionName;
    }

    virtual std::unique_ptr<AggregateFunctionEvaluator> createAggregateFunctionEvaluator(const size_t numberOfArguments) const = 0;

};

// AggregateFunctionEvaluator

class AggregateFunctionEvaluator : private Unmovable {

protected:

    friend class AggregateFunctionDescriptor;

    static std::unordered_map<std::string, AggregateFunctionDescriptor*>& getAggregateFunctionDescriptors();

public:

    static std::unique_ptr<AggregateFunctionEvaluator> createAggregateFunctionEvaluator(const std::string& functionName, const size_t numberOfArguments);

    virtual ~AggregateFunctionEvaluator();

    virtual std::unique_ptr<AggregateFunctionEvaluator> clone(CloneReplacements& cloneReplacements) const = 0;

    virtual void open(ThreadContext& threadContext) = 0;

    virtual bool accummulate(ThreadContext& threadContext, const std::vector<ResourceValue>& argumentValues, const size_t multiplicity) = 0;

    virtual bool finish(ThreadContext& threadContext, ResourceValue& result) = 0;

};

// GenericAggregateFunctionDescriptor

template<class AFE, size_t minArity, size_t maxArity>
class GenericAggregateFunctionDescriptor : public AggregateFunctionDescriptor {

public:

    GenericAggregateFunctionDescriptor(const char* const functionName) : AggregateFunctionDescriptor(functionName) {
    }

    virtual std::unique_ptr<AggregateFunctionEvaluator> createAggregateFunctionEvaluator(const size_t numberOfArguments) const {
        if (minArity > numberOfArguments || numberOfArguments > maxArity) {
            std::ostringstream message;
            message << "Invalid number of arguments (" << numberOfArguments << ") for aggregate function '" << m_functionName << "'.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return std::unique_ptr<AggregateFunctionEvaluator>(new AFE());
    }
    
};

#define DECLARE_AGGREGATE_FUNCTION(AFE, functionName, minArity, maxArity) \
    static GenericAggregateFunctionDescriptor<AFE, minArity, maxArity> s_registration_##AFE(functionName)

#endif /* AGGREGATEFUNCTIONEVALUATOR_H_ */
