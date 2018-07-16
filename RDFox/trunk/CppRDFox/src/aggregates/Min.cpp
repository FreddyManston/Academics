// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AggregateFunctionEvaluator.h"

class Min : public AggregateFunctionEvaluator {

protected:

    DatatypeID m_datatypeID;
    union {
        int64_t m_integerValue;
        float m_floatValue;
        double m_doubleValue;
    };

public:

    virtual std::unique_ptr<AggregateFunctionEvaluator> clone(CloneReplacements& cloneReplacements) const {
        return std::unique_ptr<AggregateFunctionEvaluator>(new Min());
    }

    virtual void open(ThreadContext& threadContext) {
        m_datatypeID = D_INVALID_DATATYPE_ID;
    }

    virtual bool accummulate(ThreadContext& threadContext, const std::vector<ResourceValue>& argumentValues, const size_t multiplicity) {
        const ResourceValue& argumentValue = argumentValues[0];
        switch (m_datatypeID) {
        case D_INVALID_DATATYPE_ID:
            switch (argumentValue.getDatatypeID()) {
            case D_XSD_INTEGER:
                m_datatypeID = D_XSD_INTEGER;
                m_integerValue = argumentValue.getInteger();
                return false;
            case D_XSD_FLOAT:
                m_datatypeID = D_XSD_FLOAT;
                m_floatValue = argumentValue.getFloat();
                return false;
            case D_XSD_DOUBLE:
                m_datatypeID = D_XSD_DOUBLE;
                m_doubleValue = argumentValue.getDouble();
                return false;
            default:
                return true;
            }
        case D_XSD_INTEGER:
            switch (argumentValue.getDatatypeID()) {
            case D_XSD_INTEGER:
                if (argumentValue.getInteger() < m_integerValue)
                    m_integerValue = argumentValue.getInteger();
                return false;
            case D_XSD_FLOAT:
                if (argumentValue.getFloat() < m_integerValue) {
                    m_floatValue = argumentValue.getFloat();
                    m_datatypeID = D_XSD_FLOAT;
                }
                return false;
            case D_XSD_DOUBLE:
                if (argumentValue.getDouble() < m_integerValue) {
                    m_doubleValue = argumentValue.getDouble();
                    m_datatypeID = D_XSD_DOUBLE;
                }
                return false;
            default:
                return true;
            }
        case D_XSD_FLOAT:
            switch (argumentValue.getDatatypeID()) {
            case D_XSD_INTEGER:
                if (argumentValue.getInteger() < m_floatValue)
                    m_floatValue = static_cast<float>(argumentValue.getInteger());
                return false;
            case D_XSD_FLOAT:
                if (argumentValue.getFloat() < m_floatValue)
                    m_floatValue = argumentValue.getFloat();
                return false;
            case D_XSD_DOUBLE:
                if (argumentValue.getDouble() < m_floatValue) {
                    m_doubleValue = argumentValue.getDouble();
                    m_datatypeID = D_XSD_DOUBLE;
                }
                return false;
            default:
                return true;
            }
        case D_XSD_DOUBLE:
            switch (argumentValue.getDatatypeID()) {
            case D_XSD_INTEGER:
                if (argumentValue.getInteger() < m_doubleValue)
                    m_doubleValue = static_cast<double>(argumentValue.getInteger());
                return false;
            case D_XSD_FLOAT:
                if (argumentValue.getFloat() < m_doubleValue)
                    m_doubleValue = argumentValue.getFloat();
                return false;
            case D_XSD_DOUBLE:
                if (argumentValue.getDouble() < m_doubleValue)
                    m_doubleValue = argumentValue.getDouble();
                return false;
            default:
                return true;
            }
        default:
            return true;
        }
        return false;
    }

    virtual bool finish(ThreadContext& threadContext, ResourceValue& result) {
        switch (m_datatypeID) {
        case D_XSD_INTEGER:
            result.setInteger(m_integerValue);
            return false;
        case D_XSD_FLOAT:
            result.setFloat(m_floatValue);
            return false;
        case D_XSD_DOUBLE:
            result.setDouble(m_doubleValue);
            return false;
        default:
            return true;
        }
    }

};

DECLARE_AGGREGATE_FUNCTION(Min, "MIN", 1, 1);