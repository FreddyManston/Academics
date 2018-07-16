// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef COMMON_H_
#define COMMON_H_

#include "all.h"

class ThreadContext;
class Prefixes;

typedef uint64_t ResourceID;
typedef aligned_uint64_t aligned_ResourceID;
const ResourceID INVALID_RESOURCE_ID = 0;
const ResourceID MAX_RESOURCE_ID = static_cast<ResourceID>(-1);
const size_t SEQUENTIAL_RESOURCE_ID_SIZE = 4;

typedef uint64_t TupleIndex;
typedef aligned_uint64_t aligned_TupleIndex;
const TupleIndex INVALID_TUPLE_INDEX = 0;
const TupleIndex GROUPED_MASK = 0;
const TupleIndex NOT_GROUPED_MASK = 0xffffffffffffffffULL;
const size_t SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE = 6;

typedef uint8_t TupleStatus;
const TupleStatus TUPLE_STATUS_INVALID          = 0x00;
const TupleStatus TUPLE_STATUS_EDB_DEL          = 0x04;
const TupleStatus TUPLE_STATUS_EDB_INS          = 0x08;
const TupleStatus TUPLE_STATUS_EDB              = 0x10;
const TupleStatus TUPLE_STATUS_IDB              = 0x20;
const TupleStatus TUPLE_STATUS_IDB_MERGED       = 0x40;
const TupleStatus TUPLE_STATUS_COMPLETE         = 0x80;

const size_t HASH_TABLE_INITIAL_SIZE = 32768;
const double HASH_TABLE_LOAD_FACTOR = 0.7;

const static size_t PROXY_WINDOW_SIZE = 20 * CACHE_LINE_SIZE;

enum ResourceComponent {
    RC_S = 0,
    RC_P = 1,
    RC_O = 2
};

always_inline const char* getResourceComponentName(ResourceComponent component) {
    switch (component) {
    case RC_S:
        return "S";
    case RC_P:
        return "P";
    case RC_O:
        return "O";
    default:
        UNREACHABLE;
    }
}

template<ResourceComponent component>
always_inline ResourceID getTripleComponent(ResourceID s, ResourceID p, ResourceID o) {
    switch (component) {
    case RC_S:
        return s;
    case RC_P:
        return p;
    case RC_O:
        return o;
    default:
        UNREACHABLE;
    }
}

template<ResourceComponent component>
always_inline TupleIndex getNextTripleIndex(TupleIndex nextS, TupleIndex nextP, TupleIndex nextO) {
    switch (component) {
    case RC_S:
        return nextS;
    case RC_P:
        return nextP;
    case RC_O:
        return nextO;
    default:
        UNREACHABLE;
    }
}

typedef uint32_t ArgumentIndex;
const uint32_t INVALID_ARGUMENT_INDEX = static_cast<uint32_t>(-1);

typedef uint64_t LexicalFormHandle;
const uint64_t INVALID_LEXICAL_FORM_HANDLE = 0;

// DatatypeID

typedef uint8_t DatatypeID;
const DatatypeID D_INVALID_DATATYPE_ID = 0;
const DatatypeID D_IRI_REFERENCE = 1;
const DatatypeID D_BLANK_NODE = 2;
const DatatypeID D_XSD_STRING = 3;
const DatatypeID D_RDF_PLAIN_LITERAL = 4;
const DatatypeID D_XSD_INTEGER = 5;
const DatatypeID D_XSD_FLOAT = 6;
const DatatypeID D_XSD_DOUBLE = 7;
const DatatypeID D_XSD_BOOLEAN = 8;
const DatatypeID D_XSD_DATE_TIME = 9;
const DatatypeID D_XSD_TIME = 10;
const DatatypeID D_XSD_DATE = 11;
const DatatypeID D_XSD_G_YEAR_MONTH = 12;
const DatatypeID D_XSD_G_YEAR = 13;
const DatatypeID D_XSD_G_MONTH_DAY = 14;
const DatatypeID D_XSD_G_DAY = 15;
const DatatypeID D_XSD_G_MONTH = 16;
const DatatypeID D_XSD_DURATION = 17;

// Known resources

const ResourceID OWL_SAME_AS_ID = 1;
const ResourceID OWL_DIFFERENT_FROM_ID = 2;
const ResourceID RDF_TYPE_ID = 3;
const ResourceID XSD_FALSE_ID = 4;
const ResourceID XSD_TRUE_ID = 4;

// ResourceType

enum ResourceType { UNDEFINED_RESOURCE, IRI_REFERENCE, BLANK_NODE, LITERAL };

// Resource

struct ResourceText {
    ResourceType m_resourceType;
    std::string m_lexicalForm;
    std::string m_datatypeIRI;

    always_inline ResourceText() : m_resourceType(UNDEFINED_RESOURCE), m_lexicalForm(), m_datatypeIRI() {
    }

    always_inline ResourceText(const ResourceText& resourceText) :
        m_resourceType(resourceText.m_resourceType), m_lexicalForm(resourceText.m_lexicalForm), m_datatypeIRI(resourceText.m_datatypeIRI)
    {
    }

    always_inline ResourceText(ResourceText&& resourceText) :
        m_resourceType(resourceText.m_resourceType), m_lexicalForm(std::move(resourceText.m_lexicalForm)), m_datatypeIRI(std::move(resourceText.m_datatypeIRI))
    {
    }

    always_inline ResourceText(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI) :
        m_resourceType(resourceType), m_lexicalForm(lexicalForm), m_datatypeIRI(datatypeIRI)
    {
    }

    always_inline ResourceText(const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI) :
        m_resourceType(resourceType), m_lexicalForm(lexicalForm), m_datatypeIRI(datatypeIRI)
    {
    }

    always_inline void clear() {
        m_resourceType = UNDEFINED_RESOURCE;
        m_lexicalForm.clear();
        m_datatypeIRI.clear();
    }

    always_inline ResourceText& operator=(const ResourceText& resourceText) {
        m_resourceType = resourceText.m_resourceType;
        m_lexicalForm = resourceText.m_lexicalForm;
        m_datatypeIRI = resourceText.m_datatypeIRI;
        return *this;
    }

    always_inline ResourceText& operator=(ResourceText&& resourceText) {
        m_resourceType = resourceText.m_resourceType;
        m_lexicalForm = std::move(resourceText.m_lexicalForm);
        m_datatypeIRI = std::move(resourceText.m_datatypeIRI);
        return *this;
    }

    always_inline bool operator==(const ResourceText& resourceText) const {
        return m_resourceType == resourceText.m_resourceType && m_lexicalForm == resourceText.m_lexicalForm && (m_resourceType != LITERAL || m_datatypeIRI == resourceText.m_datatypeIRI);
    }

    always_inline bool operator!=(const ResourceText& resourceText) const {
        return m_resourceType != resourceText.m_resourceType || m_lexicalForm != resourceText.m_lexicalForm || (m_resourceType == LITERAL && m_datatypeIRI != resourceText.m_datatypeIRI);
    }

    std::string toString(const Prefixes& prefixes) const;

};

extern std::ostream& operator<<(std::ostream& output, const ResourceText& resourceText);

// EffectiveBooleanValue

enum EffectiveBooleanValue { EBV_FALSE, EBV_TRUE, EBV_ERROR };

// ResourceValue

class ResourceValue {

protected:

    DatatypeID m_datatypeID;
    size_t m_bufferSize;
    size_t m_dataSize;
    std::unique_ptr<uint8_t[]> m_buffer;

    always_inline void ensureBufferSize(const size_t minimumBufferSize) {
        if (m_bufferSize < minimumBufferSize) {
            m_bufferSize = minimumBufferSize;
            m_buffer.reset(new uint8_t[m_bufferSize]);
        }
    }

public:

    always_inline ResourceValue() : m_datatypeID(D_INVALID_DATATYPE_ID), m_bufferSize(0), m_dataSize(0), m_buffer(nullptr) {
    }

    always_inline ResourceValue(const ResourceValue& other) : m_datatypeID(other.m_datatypeID), m_bufferSize(other.m_bufferSize), m_dataSize(other.m_dataSize), m_buffer(m_bufferSize == 0 ? nullptr : new uint8_t[m_bufferSize]) {
        if (m_dataSize != 0)
            ::memcpy(m_buffer.get(), other.m_buffer.get(), m_dataSize);
    }

    always_inline ResourceValue(ResourceValue&& other) : m_datatypeID(other.m_datatypeID), m_bufferSize(other.m_bufferSize), m_dataSize(other.m_dataSize), m_buffer(std::move(other.m_buffer)) {
    }

    always_inline ~ResourceValue() {
    }

    always_inline ResourceValue& operator=(const ResourceValue& other) {
        if (this != &other) {
            m_datatypeID = other.m_datatypeID;
            m_bufferSize = other.m_bufferSize;
            m_dataSize = other.m_dataSize;
            if (m_bufferSize == 0)
                m_buffer.reset(nullptr);
            else {
                m_buffer.reset(new uint8_t[m_bufferSize]);
                ::memcpy(m_buffer.get(), other.m_buffer.get(), m_dataSize);
            }
        }
        return *this;
    }

    always_inline ResourceValue& operator=(ResourceValue&& other) {
        if (this != &other) {
            m_datatypeID = other.m_datatypeID;
            m_bufferSize = other.m_bufferSize;
            m_dataSize = other.m_dataSize;
            m_buffer = std::move(other.m_buffer);
        }
        return *this;
    }

    always_inline bool operator==(const ResourceValue& other) const {
        return equals(other.m_datatypeID, other.m_dataSize, other.m_buffer.get());
    }

    always_inline bool operator!=(const ResourceValue& other) const {
        return notEquals(other.m_datatypeID, other.m_dataSize, other.m_buffer.get());
    }

    always_inline bool equals(const DatatypeID datatypeID, const size_t dataSize, const uint8_t* const buffer) const {
        if (m_datatypeID != datatypeID || m_dataSize != dataSize)
            return false;
        for (size_t count = 0; count < m_dataSize; ++count)
            if (m_buffer[count] != buffer[count])
                return false;
        return true;
    }

    always_inline bool notEquals(const DatatypeID datatypeID, const size_t dataSize, const uint8_t* const buffer) const {
        if (m_datatypeID != datatypeID || m_dataSize != dataSize)
            return true;
        for (size_t count = 0; count < m_dataSize; ++count)
            if (m_buffer[count] != buffer[count])
                return true;
        return false;
    }

    always_inline DatatypeID getDatatypeID() const {
        return m_datatypeID;
    }

    always_inline bool isUndefined() const {
        return m_datatypeID == D_INVALID_DATATYPE_ID;
    }

    always_inline size_t getDataSize() const {
        return m_dataSize;
    }

    always_inline const uint8_t* getDataRaw() const {
        return m_buffer.get();
    }

    always_inline std::unique_ptr<uint8_t[]> getDataRaw(DatatypeID& datatypeID, size_t& bufferSize, size_t& dataSize) {
        datatypeID = m_datatypeID;
        bufferSize = m_bufferSize;
        dataSize = m_dataSize;
        m_datatypeID = D_INVALID_DATATYPE_ID;
        m_bufferSize = 0;
        m_dataSize = 0;
        return std::move(m_buffer);
    }

    template<typename T>
    always_inline const T& getData() const {
        return *reinterpret_cast<const T*>(m_buffer.get());
    }

    always_inline bool getBoolean() const {
        return getData<bool>();
    }

    always_inline int64_t getInteger() const {
        return getData<int64_t>();
    }

    always_inline float getFloat() const {
        return getData<float>();
    }

    always_inline double getDouble() const {
        return getData<double>();
    }

    always_inline const char* getString() const {
        return reinterpret_cast<const char*>(m_buffer.get());
    }

    always_inline const size_t getStringLength() const {
        return m_dataSize - 1;
    }

    always_inline void setUndefined() {
        m_datatypeID = D_INVALID_DATATYPE_ID;
    }

    always_inline void setDataRaw(const DatatypeID datatypeID, std::unique_ptr<uint8_t[]> buffer, const size_t bufferSize, const size_t dataSize) {
        m_datatypeID = datatypeID;
        m_bufferSize = bufferSize;
        m_dataSize = dataSize;
        m_buffer = std::move(buffer);
    }

    always_inline void setDataRaw(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize) {
        ensureBufferSize(dataSize);
        m_datatypeID = datatypeID;
        m_dataSize = dataSize;
        ::memcpy(m_buffer.get(), data, dataSize);
    }

    always_inline uint8_t* setDataRaw(const DatatypeID datatypeID, const uint8_t dataSize) {
        ensureBufferSize(dataSize);
        m_datatypeID = datatypeID;
        m_dataSize = dataSize;
        return m_buffer.get();
    }

    template<typename T>
    always_inline T* setDataRaw(const DatatypeID datatypeID) {
        return reinterpret_cast<T*>(setDataRaw(datatypeID, sizeof(T)));
    }

    template<typename T>
    always_inline void setData(const DatatypeID datatypeID, const T& data) {
        ensureBufferSize(sizeof(T));
        m_datatypeID = datatypeID;
        m_dataSize = sizeof(T);
        *reinterpret_cast<T*>(m_buffer.get()) = data;
    }

    always_inline void setBoolean(const bool value) {
        setData(D_XSD_BOOLEAN, value);
    }

    always_inline void setInteger(const int64_t value) {
        setData(D_XSD_INTEGER, value);
    }

    always_inline void setFloat(const float value) {
        setData(D_XSD_FLOAT, value);
    }

    always_inline void setDouble(const double value) {
        setData(D_XSD_DOUBLE, value);
    }

    always_inline void setString(const DatatypeID datatypeID, const char* const value, const size_t length) {
        ensureBufferSize(length + 1);
        m_datatypeID = datatypeID;
        m_dataSize = length + 1;
        ::memcpy(m_buffer.get(), value, length);
        m_buffer[length] = 0;
    }

    always_inline void setString(const DatatypeID datatypeID, const char* const value1, const size_t length1, const char* const value2, const size_t length2) {
        const size_t totalLength = length1 + length2;
        const size_t totalLengthPlusOne = totalLength + 1;
        ensureBufferSize(totalLengthPlusOne);
        m_datatypeID = datatypeID;
        m_dataSize = totalLengthPlusOne;
        ::memcpy(m_buffer.get(), value1, length1);
        ::memcpy(m_buffer.get() + length1, value2, length2);
        m_buffer[totalLength] = 0;
    }

    always_inline void setString(const DatatypeID datatypeID, const char* const value) {
        setString(datatypeID, value, ::strlen(value));
    }

    always_inline void setString(const DatatypeID datatypeID, const std::string& value) {
        setString(datatypeID, value.c_str(), value.length());
    }

    always_inline void setString(const DatatypeID datatypeID, const std::string& value, const size_t pos, const size_t len = std::string::npos) {
        setString(datatypeID, value.c_str() + pos, len == std::string::npos ? value.length() - pos : len);
    }

    std::string toString(const Prefixes& prefixes) const;

    always_inline size_t hashCode() const {
        return hashCodeFor(m_datatypeID, m_dataSize, m_buffer.get());
    }

    always_inline static size_t hashCodeFor(const DatatypeID datatypeID, const size_t dataSize, const uint8_t* data) {
        size_t hash = 0;
        hash += datatypeID;
        hash += (hash << 10);
        hash ^= (hash >> 6);

        if (data != nullptr) {
            const uint8_t* const dataEnd = data + dataSize;
            for (; data != dataEnd; ++data) {
                hash += *data;
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }
        }

        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return hash;
    }
    

};

extern std::ostream& operator<<(std::ostream& output, const ResourceValue& resourceValue);

// Hash table stuff

always_inline size_t combineHashCodes(const size_t hashCode1, const size_t hashCode2) {
    return (17 * 31 + hashCode1) * 31 + hashCode2;
}

always_inline size_t combineHashCodes(const size_t hashCode1, const size_t hashCode2, const size_t hashCode3) {
    return ((17 * 31 + hashCode1) * 31 + hashCode2) * 31 + hashCode3;
}

always_inline size_t getHashTableSize(const size_t numberOfEntries) {
    const size_t withLoadfactor = static_cast<size_t>(numberOfEntries / HASH_TABLE_LOAD_FACTOR) + 1;
    size_t hashTableSize = 1;
    while (hashTableSize < withLoadfactor)
        hashTableSize *= 2;
    if (hashTableSize < HASH_TABLE_INITIAL_SIZE)
        hashTableSize = HASH_TABLE_INITIAL_SIZE;
    return hashTableSize;
}

// Update mode

enum UpdateType { ADD, SCHEDULE_FOR_ADDITION, SCHEDULE_FOR_DELETION };

// Types for the node server

typedef uint8_t NodeID;
typedef uint32_t QueryID;

always_inline QueryID getQueryID(const NodeID nodeID, const uint16_t localQueryID) {
    return nodeID << 16 | static_cast<QueryID>(localQueryID);
}

always_inline NodeID getNodeID(const QueryID queryID) {
    return queryID >> 16;
}

// Utilities for formatting numbers

extern size_t getNumberOfDigits(uint64_t value);

extern size_t getNumberOfDigitsFormated(uint64_t value);

extern void printNumberFormatted(std::ostream& output, uint64_t value, const size_t width);

extern void printNumberAbbreviated(std::ostream& output, uint64_t value);

// Binary store format versions

const char* const CURRENT_FORMATTED_STORE_VERSION = "DataStore-Formatted-v2";
const char* const CURRENT_UNFORMATTED_STORE_VERSION = "DataStore-Unformatted-v2";

template<typename SizeType>
bool always_inline copyTextToMemory(const std::string& text, char* const buffer, SizeType& bufferSize) {
    const size_t available = static_cast<size_t>(bufferSize);
    const size_t sizeToCopy = text.length() + 1;
    bufferSize = static_cast<SizeType>(sizeToCopy);
    if (text.length() < available) {
        std::memcpy(buffer, text.c_str(), sizeToCopy);
        return true;
    }
    else
        return false;
}

// Types of cardinalities in some tuple iterators

enum CardinalityType { CARDINALITY_NOT_EXACT, CARDINALITY_EXACT_NO_EQUALITY, CARDINALITY_EXACT_WITH_EQUALITY };

// Types of equality treatment in some places

enum EqualityAxiomatizationType { EQUALITY_AXIOMATIZATION_OFF, EQUALITY_AXIOMATIZATION_NO_UNA, EQUALITY_AXIOMATIZATION_UNA };

// Reasoning modes in some places

enum ReasoningModeType { REASONING_MODE_NO_EQUALITY_BY_LEVELS, REASONING_MODE_NO_EQUALITY_NO_LEVELS, REASONING_MODE_EQUALITY_NO_UNA, REASONING_MODE_EQUALITY_UNA };

always_inline bool isEqualityReasoningMode(const ReasoningModeType reasoningMode) {
    return reasoningMode == REASONING_MODE_EQUALITY_NO_UNA || reasoningMode == REASONING_MODE_EQUALITY_UNA;
}

// Types of SPARQL queries

enum SPARQLQueryType { SPARQL_SELECT_QUERY, SPARQL_ASK_QUERY, SPARQL_CONSTRUCT_QUERY };

#endif /* COMMON_H_ */
