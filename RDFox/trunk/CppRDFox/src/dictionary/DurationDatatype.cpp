// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/Prefixes.h"
#include "../util/XSDDuration.h"
#include "../util/SequentialHashTableImpl.h"
#include "../util/ParallelHashTableImpl.h"
#include "../util/ComponentStatistics.h"
#include "../util/Vocabulary.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/ThreadContext.h"
#include "DataPoolImpl.h"
#include "Datatype.h"

static const std::string s_xsdDuration(XSD_DURATION);
static const std::string s_xsdDayTimeDuration(XSD_DAY_TIME_DURATION);
static const std::string s_xsdYearMonthDuration(XSD_YEAR_MONTH_DURATION);

always_inline static void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm) {
    resourceValue.setData(D_XSD_DURATION, XSDDuration::parseDuration(lexicalForm));
}

always_inline static void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) {
    if (datatypeIRI == s_xsdDuration)
        parseResourceValue(resourceValue, lexicalForm);
    else
        throw RDF_STORE_EXCEPTION("Internal error: invalid datatype IRI in DurationDatatype.");
}

// AbstractDurationPolicy

typedef size_t ALIGNMENT_TYPE;
const size_t RESOURCE_ID_TO_XSDDURATION_PACKING = (SUPPORTS_UNALIGNED_ACCESS ? 0 : sizeof(ALIGNMENT_TYPE) - sizeof(ResourceID));

class AbstractDurationPolicy {

protected:

    DataPool& m_dataPool;

public:

    struct BucketContents {
        uint64_t m_chunkIndex;
    };

    always_inline AbstractDurationPolicy(DataPool& dataPool) : m_dataPool(dataPool) {
    }


    always_inline BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const XSDDuration* duration) const {
        if (bucketContents.m_chunkIndex == DataPool::INVALID_CHUNK_INDEX)
            return BUCKET_EMPTY;
        else {
            const XSDDuration* bucketDuration = reinterpret_cast<const XSDDuration*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING);
            if (*bucketDuration == *duration)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }
    }

    always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_chunkIndex == DataPool::INVALID_CHUNK_INDEX;
    }

    always_inline size_t getBucketContentsHashCode(const BucketContents& bucketContents) const {
        return hashCodeFor(reinterpret_cast<const XSDDuration*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING));
    }

    always_inline static size_t hashCodeFor(const XSDDuration* duration) {
        size_t result = static_cast<size_t>(14695981039346656037ULL);
        result ^= static_cast<size_t>(duration->getMonths());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(duration->getSeconds());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(duration->getMilliseconds());
        result *= static_cast<size_t>(1099511628211ULL);
        return result;
    }

};

// SequentialDurationPolicy

class SequentialDurationPolicy : public AbstractDurationPolicy {

public:

    static const bool IS_PARALLEL = false;

    static const size_t BUCKET_SIZE = 6;

    always_inline SequentialDurationPolicy(DataPool& dataPool) : AbstractDurationPolicy(dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        bucketContents.m_chunkIndex = getChunkIndex(bucket);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        if (getChunkIndex(bucket) == 0) {
            setChunkIndex(bucket, bucketContents.m_chunkIndex);
            return true;
        }
        else
            return false;
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::read6(bucket);
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::write6(bucket, chunkIndex);
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return true;
    }

};

// ConcurrentDurationPolicy

class ConcurrentDurationPolicy : public AbstractDurationPolicy {

public:

    static const bool IS_PARALLEL = true;

    static const size_t BUCKET_SIZE = sizeof(uint64_t);

    static const uint64_t IN_INSERTION_CHUNK_INDEX = static_cast<uint64_t>(-1);

    always_inline ConcurrentDurationPolicy(DataPool& dataPool) : AbstractDurationPolicy(dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        do {
            bucketContents.m_chunkIndex = getChunkIndex(bucket);
        } while (bucketContents.m_chunkIndex == IN_INSERTION_CHUNK_INDEX);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, bucketContents.m_chunkIndex);
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::atomicRead(*reinterpret_cast<const uint64_t*>(bucket));
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::atomicWrite(*reinterpret_cast<uint64_t*>(bucket), chunkIndex);
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, IN_INSERTION_CHUNK_INDEX);
    }

};

// Declaration DurationDatatype

class MemoryManager;

template<class HTT>
class DurationDatatype : public Datatype {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    mutable HashTableType m_hashTable;

public:

    DurationDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

    void initialize(const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const;

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// Implementation DurationDatatype

template<class HTT>
DurationDatatype<HTT>::DurationDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, m_dataPool)
{
}

template<class HTT>
void DurationDatatype<HTT>::initialize(const size_t initialResourceCapacity) {
    if (!m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize the hash table in DurationDatatype.");
}

template<class HTT>
void DurationDatatype<HTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_hashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT>
void DurationDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    resourceValue.setData(datatypeID, *reinterpret_cast<const XSDDuration*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING));
}

template<class HTT>
void DurationDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    lexicalForm = reinterpret_cast<const XSDDuration*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING)->toString();
    datatypeIRI = s_xsdDuration;
}

template<class HTT>
void DurationDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const XSDDuration*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING)->toString();
}

template<class HTT>
ResourceID DurationDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    const XSDDuration* const duration = &resourceValue.getData<XSDDuration>();
    typename HTT::BucketDescriptor bucketDescriptor;
    ResourceID resourceID;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, duration);
    if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, duration) == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else
        resourceID = INVALID_RESOURCE_ID;
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID DurationDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
    return tryResolveResource(threadContext, resourceValue);
}

template<class HTT>
ResourceID DurationDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm);
    return tryResolveResource(threadContext, resourceValue);
}

template<class HTT>
ResourceID DurationDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    const XSDDuration* const duration = &resourceValue.getData<XSDDuration>();
    typename HTT::BucketDescriptor bucketDescriptor;
    BucketStatus bucketStatus;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, duration);
    while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, duration)) == BUCKET_EMPTY) {
        if (m_hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    ResourceID resourceID;
    if (bucketStatus == BUCKET_EMPTY) {
        resourceID = nextResourceID<HTT::PolicyType::IS_PARALLEL>(dictionaryUsageContext, preferredResourceID);
        const uint64_t chunkIndex = m_dataPool.newDataChunk<HTT::PolicyType::IS_PARALLEL, ALIGNMENT_TYPE>(dictionaryUsageContext, sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING + sizeof(XSDDuration));
        if (chunkIndex == DataPool::INVALID_CHUNK_INDEX) {
            m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
            ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_INVALID_DATATYPE_ID);
            throw RDF_STORE_EXCEPTION("The data pool is full.");
        }
        uint8_t* durationData = m_dataPool.getDataFor(chunkIndex);
        *reinterpret_cast<ResourceID*>(durationData) = resourceID;
        durationData += sizeof(ResourceID) + RESOURCE_ID_TO_XSDDURATION_PACKING;
        *reinterpret_cast<XSDDuration*>(durationData) = *duration;
        ::atomicWrite(m_lexicalFormHandlesByResourceID[resourceID], chunkIndex);
        ::atomicWrite(m_datatypeIDsByResourceID[resourceID], resourceValue.getDatatypeID());
        m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, chunkIndex);
        m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else {
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        throw  RDF_STORE_EXCEPTION("The hash table for date/time values is full.");
    }
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID DurationDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue, preferredResourceID);
}

template<class HTT>
ResourceID DurationDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm);
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue, preferredResourceID);
}

template<class HTT>
void DurationDatatype<HTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("DurationDatatype");
    m_hashTable.save(outputStream);
}

template<class HTT>
void DurationDatatype<HTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("DurationDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load StringDatatype.");
    m_hashTable.load(inputStream);
}

template<class HTT>
std::unique_ptr<ComponentStatistics> DurationDatatype<HTT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("DurationDatatype"));
    const size_t size = m_hashTable.getNumberOfBuckets() * m_hashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Total number of buckets", m_hashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets", m_hashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%)", (m_hashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_hashTable.getNumberOfBuckets()));
    result->addIntegerItem("Aggregate size", size);
    return result;
}

// DurationDatatypeFactory

DATATYPE_FACTORY(DurationDatatypeFactory, DurationDatatype<ParallelHashTable<ConcurrentDurationPolicy> >, DurationDatatype<SequentialHashTable<SequentialDurationPolicy> >);

DurationDatatypeFactory::DurationDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{
        { D_XSD_DURATION, s_xsdDuration }
    }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{
        { s_xsdDuration, D_XSD_DURATION },
        { s_xsdDayTimeDuration, D_XSD_DURATION },
        { s_xsdYearMonthDuration, D_XSD_DURATION }
    })
{
}

void DurationDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    ::parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
}

void DurationDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    ::parseResourceValue(resourceValue, lexicalForm);
}

void DurationDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const XSDDuration*>(data)->toString();
}

void DurationDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    literalText = '"';
    literalText.append(reinterpret_cast<const XSDDuration*>(data)->toString());
    literalText.append("\"^^");
    literalText.append(prefixes.encodeIRI(s_xsdDuration));
}

EffectiveBooleanValue DurationDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return EBV_ERROR;
}
