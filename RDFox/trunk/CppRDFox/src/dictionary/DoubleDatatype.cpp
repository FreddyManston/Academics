// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/Prefixes.h"
#include "../util/SequentialHashTableImpl.h"
#include "../util/ParallelHashTableImpl.h"
#include "../util/ComponentStatistics.h"
#include "../util/Vocabulary.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/ThreadContext.h"
#include "Datatype.h"

always_inline static double parseDouble(const std::string& lexicalForm) {
    // An explicit test for "inf" is needed because the implementation of ::strtod in MSVC does not recognise "inf" correctly.
    if (lexicalForm == "inf")
        return std::numeric_limits<double>::infinity();
    else {
        char* end;
        errno = 0;
        // strtod is used because MSVC does not support strtof.
        const double result = ::strtod(lexicalForm.c_str(), &end);
        if (errno == ERANGE || lexicalForm.length() == 0 || *end != '\0') {
            std::ostringstream message;
            message << "Lexical form '" << lexicalForm << "' is invalid for the xsd:double datatype.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return result;
    }
}

always_inline static double toDouble(const LexicalFormHandle lexicalFormHandle) {
    return ::doubleFromBits(static_cast<uint64_t>(lexicalFormHandle));
}

// AbstractDoubleDatatypePolicy

class AbstractDoubleDatatypePolicy {

public:

    static const size_t BUCKET_SIZE = sizeof(ResourceID) + sizeof(double);

    struct BucketContents {
        ResourceID m_resourceID;
        double m_value;
    };

    always_inline static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const double value) {
        if (bucketContents.m_resourceID == INVALID_RESOURCE_ID)
            return BUCKET_EMPTY;
        else
            return bucketContents.m_value == value ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
    }

    always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_resourceID == INVALID_RESOURCE_ID;
    }

    always_inline static size_t getBucketContentsHashCode(const BucketContents& bucketContents) {
        return hashCodeFor(bucketContents.m_value);
    }

    always_inline static size_t hashCodeFor(const double value) {
        return ::doubleToBits(value) * 2654435761;
    }

};

// SequentialDoubleDatatypePolicy

class SequentialDoubleDatatypePolicy : public AbstractDoubleDatatypePolicy {

public:

    static const bool IS_PARALLEL = false;

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        bucketContents.m_resourceID = getResourceID(bucket);
        bucketContents.m_value = getValue(bucket);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        if (getResourceID(bucket) == INVALID_RESOURCE_ID) {
            setResourceID(bucket, bucketContents.m_resourceID);
            setValue(bucket, bucketContents.m_value);
            return true;
        }
        else
            return false;
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return true;
    }

    always_inline static ResourceID getResourceID(const uint8_t* const bucket) {
        return *reinterpret_cast<const ResourceID*>(bucket);
    }

    always_inline static void setResourceID(uint8_t* const bucket, const ResourceID resourceID) {
        *reinterpret_cast<ResourceID*>(bucket) = resourceID;
    }

    always_inline static double getValue(const uint8_t* const bucket) {
        return *reinterpret_cast<const double*>(bucket + sizeof(ResourceID));
    }

    always_inline static void setValue(uint8_t* const bucket, const double value) {
        *reinterpret_cast<double*>(bucket + sizeof(ResourceID)) = value;
    }

};

// ConcurrentDoubleDatatypePolicy

class ConcurrentDoubleDatatypePolicy : public AbstractDoubleDatatypePolicy {

public:

    static const bool IS_PARALLEL = true;

    static const ResourceID IN_INSERTION_RESOURCE_ID = static_cast<ResourceID>(-1);

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        do {
            bucketContents.m_resourceID = getResourceID(bucket);
        } while (bucketContents.m_resourceID == IN_INSERTION_RESOURCE_ID);
        bucketContents.m_value = getValue(bucket);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        if (::atomicConditionalSet(*reinterpret_cast<ResourceID*>(bucket), INVALID_RESOURCE_ID, IN_INSERTION_RESOURCE_ID)) {
            setValue(bucket, bucketContents.m_value);
            setResourceID(bucket, bucketContents.m_resourceID);
            return true;
        }
        else
            return false;
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return ::atomicConditionalSet(*reinterpret_cast<ResourceID*>(bucket), INVALID_RESOURCE_ID, IN_INSERTION_RESOURCE_ID);
    }

    always_inline static ResourceID getResourceID(const uint8_t* const bucket) {
        return ::atomicRead(*reinterpret_cast<const ResourceID*>(bucket));
    }

    always_inline static void setResourceID(uint8_t* const bucket, const ResourceID resourceID) {
        ::atomicWrite(*reinterpret_cast<ResourceID*>(bucket), resourceID);
    }

    always_inline static double getValue(const uint8_t* const bucket) {
        return ::atomicRead(*reinterpret_cast<const double*>(bucket + sizeof(ResourceID)));
    }

    always_inline static void setValue(uint8_t* const bucket, const double value) {
        ::atomicWrite(*reinterpret_cast<double*>(bucket + sizeof(ResourceID)), value);
    }

};

// Declaration DoubleDatatype

class MemoryManager;

static const std::string s_xsdDouble(XSD_DOUBLE);

template<class HTT>
class DoubleDatatype : public Datatype {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    mutable HashTableType m_hashTable;

    static LexicalFormHandle toLexicalFormHandle(const double value);

public:

    DoubleDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

    void initialize(const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const double value) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const;

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const double value, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// Implementation DoubleDatatype

template<class HTT>
always_inline LexicalFormHandle DoubleDatatype<HTT>::toLexicalFormHandle(const double value) {
    return static_cast<LexicalFormHandle>(::doubleToBits(value));
}

template<class HTT>
DoubleDatatype<HTT>::DoubleDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR)
{
}

template<class HTT>
void DoubleDatatype<HTT>::initialize(const size_t initialResourceCapacity) {
    if (!m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize the hash table in DoubleDatatype.");
}

template<class HTT>
void DoubleDatatype<HTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_hashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT>
void DoubleDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    resourceValue.setDouble(toDouble(lexicalFormHandle));
}

template<class HTT>
void DoubleDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    std::ostringstream buffer;
    buffer << toDouble(lexicalFormHandle);
    lexicalForm = buffer.str();
    datatypeIRI = s_xsdDouble;
}

template<class HTT>
void DoubleDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    std::ostringstream buffer;
    buffer << toDouble(lexicalFormHandle);
    lexicalForm = buffer.str();
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const double value) const {
    typename HTT::BucketDescriptor bucketDescriptor;
    ResourceID resourceID;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, value);
    if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, value) == BUCKET_CONTAINS)
        resourceID = bucketDescriptor.m_bucketContents.m_resourceID;
    else
        resourceID = INVALID_RESOURCE_ID;
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    return tryResolveResource(threadContext, resourceValue.getDouble());
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    return tryResolveResource(threadContext, parseDouble(lexicalForm));
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    return tryResolveResource(threadContext, parseDouble(lexicalForm));
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const double value, const ResourceID preferredResourceID) {
    typename HTT::BucketDescriptor bucketDescriptor;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, value);
    BucketStatus bucketStatus;
    while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, value)) == BUCKET_EMPTY) {
        if (m_hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    ResourceID resourceID;
    if (bucketStatus == BUCKET_EMPTY) {
        resourceID = nextResourceID<HTT::PolicyType::IS_PARALLEL>(dictionaryUsageContext, preferredResourceID);
        ::atomicWrite(m_lexicalFormHandlesByResourceID[resourceID], toLexicalFormHandle(value));
        ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_XSD_DOUBLE);
        m_hashTable.getPolicy().setValue(bucketDescriptor.m_bucket, value);
        m_hashTable.getPolicy().setResourceID(bucketDescriptor.m_bucket, resourceID);
        m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        resourceID = bucketDescriptor.m_bucketContents.m_resourceID;
    else {
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        throw RDF_STORE_EXCEPTION("The hash table for xsd:double is full.");
    }
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue.getDouble(), preferredResourceID);
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, parseDouble(lexicalForm), preferredResourceID);
}

template<class HTT>
ResourceID DoubleDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, parseDouble(lexicalForm), preferredResourceID);
}

template<class HTT>
void DoubleDatatype<HTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("DoubleDatatype");
    m_hashTable.save(outputStream);
}

template<class HTT>
void DoubleDatatype<HTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("DoubleDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load DoubleDatatype.");
    m_hashTable.load(inputStream);
}

template<class HTT>
std::unique_ptr<ComponentStatistics> DoubleDatatype<HTT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("DoubleDatatype"));
    const size_t size = m_hashTable.getNumberOfBuckets() * m_hashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Total number of buckets", m_hashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets", m_hashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%)", (m_hashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_hashTable.getNumberOfBuckets()));
    result->addIntegerItem("Aggregate size", size);
    return result;
}

// DoubleDatatypeFactory

DATATYPE_FACTORY(DoubleDatatypeFactory, DoubleDatatype<ParallelHashTable<ConcurrentDoubleDatatypePolicy> >, DoubleDatatype<SequentialHashTable<SequentialDoubleDatatypePolicy> >);

DoubleDatatypeFactory::DoubleDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{ { D_XSD_DOUBLE, s_xsdDouble } }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{ { s_xsdDouble, D_XSD_DOUBLE } })
{
}

void DoubleDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    resourceValue.setDouble(parseDouble(lexicalForm));
}

void DoubleDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    resourceValue.setDouble(parseDouble(lexicalForm));
}

void DoubleDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    std::ostringstream buffer;
    buffer << *reinterpret_cast<const double*>(data);
    lexicalForm = buffer.str();
}

void DoubleDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    std::ostringstream buffer;
    buffer << '"' << *reinterpret_cast<const double*>(data) << "\"^^" << prefixes.encodeIRI(s_xsdDouble);
    literalText = buffer.str();
}

EffectiveBooleanValue DoubleDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return resourceValue.getDouble() != 0 && !::isNaN(resourceValue.getDouble()) ? EBV_TRUE : EBV_FALSE;
}
