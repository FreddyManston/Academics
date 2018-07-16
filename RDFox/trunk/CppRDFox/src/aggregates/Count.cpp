// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AggregateFunctionEvaluator.h"

class Count : public AggregateFunctionEvaluator {

protected:

    size_t m_numberOfItems;

public:

    virtual std::unique_ptr<AggregateFunctionEvaluator> clone(CloneReplacements& cloneReplacements) const {
        return std::unique_ptr<AggregateFunctionEvaluator>(new Count());
    }

    virtual void open(ThreadContext& threadContext) {
        m_numberOfItems = 0;
    }

    virtual bool accummulate(ThreadContext& threadContext, const std::vector<ResourceValue>& argumentValues, const size_t multiplicity) {
        m_numberOfItems += multiplicity;
        return false;
    }

    virtual bool finish(ThreadContext& threadContext, ResourceValue& result) {
        result.setInteger(static_cast<int64_t>(m_numberOfItems));
        return false;
    }

};

DECLARE_AGGREGATE_FUNCTION(Count, "COUNT", 0, 1);