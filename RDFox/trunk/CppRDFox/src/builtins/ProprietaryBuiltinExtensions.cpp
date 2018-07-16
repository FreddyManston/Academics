// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/ResourceValueCache.h"
#include "../util/XSDDateTime.h"
#include "CommonBuiltinExpressionEvaluators.h"

// SkolemEvaluator

class SkolemEvaluator : public BuiltinExpressionEvaluator {

protected:

    ResourceValueCache& m_resourceValueCache;
    unique_ptr_vector<BuiltinExpressionEvaluator> m_arguments;

    void resizeBuffer(std::unique_ptr<uint8_t[]>& buffer, size_t& bufferSize, const size_t dataSize, const size_t extraSize);

    always_inline void ensureBufferSize(std::unique_ptr<uint8_t[]>& buffer, size_t& bufferSize, const size_t dataSize, const size_t extraSize) {
        if (dataSize + extraSize > bufferSize)
            resizeBuffer(buffer, bufferSize, dataSize, extraSize);
    }

public:

    SkolemEvaluator(ResourceValueCache& resourceValueCache, unique_ptr_vector<BuiltinExpressionEvaluator> arguments);

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const;

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

always_inline SkolemEvaluator::SkolemEvaluator(ResourceValueCache& resourceValueCache, unique_ptr_vector<BuiltinExpressionEvaluator> arguments) :
    m_resourceValueCache(resourceValueCache),
    m_arguments(std::move(arguments))
{
}

void SkolemEvaluator::resizeBuffer(std::unique_ptr<uint8_t[]>& buffer, size_t& bufferSize, const size_t dataSize, const size_t extraSize) {
    const size_t newBufferSize = dataSize + extraSize + 128;
    std::unique_ptr<uint8_t[]> newBuffer(new uint8_t[newBufferSize]);
    std::memcpy(newBuffer.get(), buffer.get(), dataSize);
    buffer = std::move(newBuffer);
    bufferSize = newBufferSize;
}

std::unique_ptr<BuiltinExpressionEvaluator> SkolemEvaluator::clone(CloneReplacements& cloneReplacements) const {
    unique_ptr_vector<BuiltinExpressionEvaluator> newArguments;
    for (unique_ptr_vector<BuiltinExpressionEvaluator>::const_iterator iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        newArguments.push_back((*iterator)->clone(cloneReplacements));
    return std::unique_ptr<BuiltinExpressionEvaluator>(new SkolemEvaluator(m_resourceValueCache, std::move(newArguments)));
}

bool SkolemEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    auto iterator = m_arguments.begin();
    if ((*iterator)->evaluate(threadContext, result))
        return true;
    DatatypeID datatypeID;
    size_t bufferSize;
    size_t dataSize;
    std::unique_ptr<uint8_t[]> buffer = result.getDataRaw(datatypeID, bufferSize, dataSize);
    if (datatypeID != D_XSD_STRING || dataSize <= 1)
        return true;
    --dataSize; // This is done so that the next append overwrites the trailing zero.
    ResourceValue argumentNValue;
    for (++iterator; iterator != m_arguments.end(); ++iterator) {
        if ((*iterator)->evaluate(threadContext, argumentNValue) || argumentNValue.isUndefined())
            return true;
        const ResourceID resourceID = m_resourceValueCache.tryResolveResource(threadContext, argumentNValue);
        if (resourceID == INVALID_RESOURCE_ID) {
            ensureBufferSize(buffer, bufferSize, dataSize, dataSize + 2);
            buffer[dataSize++] = '_';
            buffer[dataSize++] = '0';
        }
        else {
            ResourceID resourceIDCopy = resourceID;
            size_t numberOfCharactes = 1; // One character is needed for the underscore!
            size_t multiplier = 1;
            while (resourceIDCopy != 0) {
                ++numberOfCharactes;
                multiplier *= 10;
                resourceIDCopy /= 10;
            }
            ensureBufferSize(buffer, bufferSize, dataSize, dataSize + numberOfCharactes);
            buffer[dataSize++] = '_';
            resourceIDCopy = resourceID;
            while (multiplier >= 10) {
                multiplier /= 10;
                buffer[dataSize++] = '0' + static_cast<uint8_t>(resourceIDCopy / multiplier);
                resourceIDCopy %= multiplier;
            }
        }
    }
    ensureBufferSize(buffer, bufferSize, dataSize, dataSize + 1);
    buffer[dataSize++] = 0;
    result.setDataRaw(D_BLANK_NODE, std::move(buffer), bufferSize, dataSize);
    return false;
}

// SkolemEvaluatorDescriptor

class SkolemEvaluatorDescriptor : public AbstractBuiltinFunctionDescriptor {

public:

    SkolemEvaluatorDescriptor() : AbstractBuiltinFunctionDescriptor("SKOLEM", 1000) {
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> createBuiltinExpressionEvaluator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, unique_ptr_vector<BuiltinExpressionEvaluator> arguments) const {
        return std::unique_ptr<BuiltinExpressionEvaluator>(new SkolemEvaluator(resourceValueCache, std::move(arguments)));
    }

};

static SkolemEvaluatorDescriptor s_registration_SkolemEvaluator;

// DateTimeEvaluator

class DateTimeEvaluator : public NAryFunctionEvaluator<DateTimeEvaluator> {

public:

    DateTimeEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<DateTimeEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);
    
};

bool DateTimeEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    LOAD_ARGUMENT_OF_DATATYPE(yearValue, m_arguments[0], D_XSD_INTEGER);
    LOAD_ARGUMENT_OF_DATATYPE(monthValue, m_arguments[1], D_XSD_INTEGER);
    LOAD_ARGUMENT_OF_DATATYPE(dayValue, m_arguments[2], D_XSD_INTEGER);
    LOAD_ARGUMENT_OF_DATATYPE(hourValue, m_arguments[3], D_XSD_INTEGER);
    LOAD_ARGUMENT_OF_DATATYPE(minuteValue, m_arguments[4], D_XSD_INTEGER);
    LOAD_ARGUMENT_OF_DATATYPE(secondValue, m_arguments[5], D_XSD_INTEGER);
    if (m_arguments.size() == 7) {
        LOAD_ARGUMENT_OF_DATATYPE(timezoneOffsetValue, m_arguments[6], D_XSD_INTEGER);
        XSDDateTime::initialize(result.setDataRaw<XSDDateTime>(D_XSD_DATE_TIME), static_cast<int32_t>(yearValue.getInteger()), static_cast<uint8_t>(monthValue.getInteger()), static_cast<uint8_t>(dayValue.getInteger()), static_cast<uint8_t>(hourValue.getInteger()), static_cast<uint8_t>(minuteValue.getInteger()), static_cast<uint8_t>(secondValue.getInteger()), 0, static_cast<int16_t>(timezoneOffsetValue.getInteger()));
    }
    else
        XSDDateTime::initialize(result.setDataRaw<XSDDateTime>(D_XSD_DATE_TIME), static_cast<int32_t>(yearValue.getInteger()), static_cast<uint8_t>(monthValue.getInteger()), static_cast<uint8_t>(dayValue.getInteger()), static_cast<uint8_t>(hourValue.getInteger()), static_cast<uint8_t>(minuteValue.getInteger()), static_cast<uint8_t>(secondValue.getInteger()), 0, TIME_ZONE_OFFSET_ABSENT);
    return false;
}

static GenericBuiltinFunctionDescriptor<DateTimeEvaluator, 6, 7> s_registration_DateTimeEvaluator("dateTime", 1000);
