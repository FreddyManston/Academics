// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/Prefixes.h"
#include "../util/XSDDateTime.h"
#include "../util/SequentialHashTableImpl.h"
#include "../util/ParallelHashTableImpl.h"
#include "../util/ComponentStatistics.h"
#include "../util/Vocabulary.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/ThreadContext.h"
#include "Dictionary.h"
#include "DataPoolImpl.h"
#include "Datatype.h"

static const std::string s_xsdDateTime(XSD_DATE_TIME);
static const std::string s_xsdDateTimeStamp(XSD_DATE_TIME_STAMP);
static const std::string s_xsdTime(XSD_TIME);
static const std::string s_xsdDate(XSD_DATE);
static const std::string s_xsdGYearMonth(XSD_G_YEAR_MONTH);
static const std::string s_xsdGYear(XSD_G_YEAR);
static const std::string s_xsdGMonthDay(XSD_G_MONTH_DAY);
static const std::string s_xsdGDay(XSD_G_DAY);
static const std::string s_xsdGMonth(XSD_G_MONTH);

always_inline static void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) {
    if (datatypeIRI == s_xsdDateTime)
        resourceValue.setData(D_XSD_DATE_TIME, XSDDateTime::parseDateTime(lexicalForm));
    else if (datatypeIRI == s_xsdDateTimeStamp) {
        resourceValue.setData(D_XSD_DATE_TIME, XSDDateTime::parseDateTime(lexicalForm));
        if (resourceValue.getData<XSDDateTime>().isTimeZoneOffsetAbsent())
            throw RDF_STORE_EXCEPTION("xsd:dateTimeStamp values must have a time zone.");
    }
    else if (datatypeIRI == s_xsdTime)
        resourceValue.setData(D_XSD_TIME, XSDDateTime::parseTime(lexicalForm));
    else if (datatypeIRI == s_xsdDate)
        resourceValue.setData(D_XSD_DATE, XSDDateTime::parseDate(lexicalForm));
    else if (datatypeIRI == s_xsdGYearMonth)
        resourceValue.setData(D_XSD_G_YEAR_MONTH, XSDDateTime::parseGYearMonth(lexicalForm));
    else if (datatypeIRI == s_xsdGYear)
        resourceValue.setData(D_XSD_G_YEAR, XSDDateTime::parseGYear(lexicalForm));
    else if (datatypeIRI == s_xsdGMonthDay)
        resourceValue.setData(D_XSD_G_MONTH_DAY, XSDDateTime::parseGMonthDay(lexicalForm));
    else if (datatypeIRI == s_xsdGDay)
        resourceValue.setData(D_XSD_G_DAY, XSDDateTime::parseGDay(lexicalForm));
    else if (datatypeIRI == s_xsdGMonth)
        resourceValue.setData(D_XSD_G_MONTH, XSDDateTime::parseGMonth(lexicalForm));
    else
        throw RDF_STORE_EXCEPTION("Internal error: invalid datatype IRI in DateTimeDatatype.");
}

always_inline static void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) {
    switch (datatypeID) {
    case D_XSD_DATE_TIME:
        resourceValue.setData(D_XSD_DATE_TIME, XSDDateTime::parseDateTime(lexicalForm));
        break;
    case D_XSD_TIME:
        resourceValue.setData(D_XSD_TIME, XSDDateTime::parseTime(lexicalForm));
        break;
    case D_XSD_DATE:
        resourceValue.setData(D_XSD_DATE, XSDDateTime::parseDate(lexicalForm));
        break;
    case D_XSD_G_YEAR_MONTH:
        resourceValue.setData(D_XSD_G_YEAR_MONTH, XSDDateTime::parseGYearMonth(lexicalForm));
        break;
    case D_XSD_G_YEAR:
        resourceValue.setData(D_XSD_G_YEAR, XSDDateTime::parseGYear(lexicalForm));
        break;
    case D_XSD_G_MONTH_DAY:
        resourceValue.setData(D_XSD_G_MONTH_DAY, XSDDateTime::parseGMonthDay(lexicalForm));
        break;
    case D_XSD_G_DAY:
        resourceValue.setData(D_XSD_G_DAY, XSDDateTime::parseGDay(lexicalForm));
        break;
    case D_XSD_G_MONTH:
        resourceValue.setData(D_XSD_G_MONTH, XSDDateTime::parseGMonth(lexicalForm));
        break;
    default:
        throw RDF_STORE_EXCEPTION("Internal error: invalid datatype ID in DateTimeDatatype.");
    }
}

// AbstractDateTimePolicy

typedef size_t ALIGNMENT_TYPE;
const size_t RESOURCE_ID_TO_XSDDATETIME_PACKING = (SUPPORTS_UNALIGNED_ACCESS ? 0 : sizeof(ALIGNMENT_TYPE) - sizeof(ResourceID));

class AbstractDateTimePolicy {

protected:

    DataPool& m_dataPool;

public:

    struct BucketContents {
        uint64_t m_chunkIndex;
    };

    always_inline AbstractDateTimePolicy(DataPool& dataPool) : m_dataPool(dataPool) {
    }

    always_inline BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const XSDDateTime* dateTime) const {
        if (bucketContents.m_chunkIndex == DataPool::INVALID_CHUNK_INDEX)
            return BUCKET_EMPTY;
        else {
            const XSDDateTime* bucketDateTime = reinterpret_cast<const XSDDateTime*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING);
            if (*bucketDateTime == *dateTime)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }
    }

    always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_chunkIndex == DataPool::INVALID_CHUNK_INDEX;
    }

    always_inline size_t getBucketContentsHashCode(const BucketContents& bucketContents) const {
        return hashCodeFor(reinterpret_cast<const XSDDateTime*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING));
    }

    always_inline static size_t hashCodeFor(const XSDDateTime* dateTime) {
        size_t result = static_cast<size_t>(14695981039346656037ULL);
        result ^= static_cast<size_t>(dateTime->getYear());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getMonth());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getDay());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getHour());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getMinute());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getSecond());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getMillisecond());
        result *= static_cast<size_t>(1099511628211ULL);
        result ^= static_cast<size_t>(dateTime->getTimeZoneOffset());
        result *= static_cast<size_t>(1099511628211ULL);
        return result;
    }

};

// SequentialDateTimePolicy

class SequentialDateTimePolicy : public AbstractDateTimePolicy {

public:

    static const bool IS_PARALLEL = false;

    static const size_t BUCKET_SIZE = 6;

    always_inline SequentialDateTimePolicy(DataPool& dataPool) : AbstractDateTimePolicy(dataPool) {
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

// ConcurrentDateTimePolicy

class ConcurrentDateTimePolicy : public AbstractDateTimePolicy {

public:

    static const bool IS_PARALLEL = true;

    static const size_t BUCKET_SIZE = sizeof(uint64_t);

    static const uint64_t IN_INSERTION_CHUNK_INDEX = static_cast<uint64_t>(-1);

    always_inline ConcurrentDateTimePolicy(DataPool& dataPool) : AbstractDateTimePolicy(dataPool) {
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

// Declaration DateTimeDatatype

class MemoryManager;

template<class HTT>
class DateTimeDatatype : public Datatype {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    mutable HashTableType m_hashTable;

public:

    DateTimeDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

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

// Implementation DateTimeDatatype

template<class HTT>
DateTimeDatatype<HTT>::DateTimeDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, m_dataPool)
{
}

template<class HTT>
void DateTimeDatatype<HTT>::initialize(const size_t initialResourceCapacity) {
    if (!m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize the hash table in DateTimeDatatype.");
}

template<class HTT>
void DateTimeDatatype<HTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_hashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT>
void DateTimeDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    resourceValue.setData(datatypeID, *reinterpret_cast<const XSDDateTime*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING));
}

template<class HTT>
void DateTimeDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    lexicalForm = reinterpret_cast<const XSDDateTime*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING)->toString();
    datatypeIRI = Dictionary::getDatatypeIRI(datatypeID);
}

template<class HTT>
void DateTimeDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const XSDDateTime*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING)->toString();
}

template<class HTT>
ResourceID DateTimeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    const XSDDateTime* const dateTime = &resourceValue.getData<XSDDateTime>();
    typename HTT::BucketDescriptor bucketDescriptor;
    ResourceID resourceID;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, dateTime);
    if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, dateTime) == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else
        resourceID = INVALID_RESOURCE_ID;
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID DateTimeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
    return tryResolveResource(threadContext, resourceValue);
}

template<class HTT>
ResourceID DateTimeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm, datatypeID);
    return tryResolveResource(threadContext, resourceValue);
}

template<class HTT>
ResourceID DateTimeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    const XSDDateTime* const dateTime = &resourceValue.getData<XSDDateTime>();
    typename HTT::BucketDescriptor bucketDescriptor;
    BucketStatus bucketStatus;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, dateTime);
    while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, dateTime)) == BUCKET_EMPTY) {
        if (m_hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    ResourceID resourceID;
    if (bucketStatus == BUCKET_EMPTY) {
        resourceID = nextResourceID<HTT::PolicyType::IS_PARALLEL>(dictionaryUsageContext, preferredResourceID);
        const uint64_t chunkIndex = m_dataPool.newDataChunk<HTT::PolicyType::IS_PARALLEL, ALIGNMENT_TYPE>(dictionaryUsageContext, sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING + sizeof(XSDDateTime));
        if (chunkIndex == DataPool::INVALID_CHUNK_INDEX) {
            m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
            ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_INVALID_DATATYPE_ID);
            throw  RDF_STORE_EXCEPTION("The data pool is full.");
        }
        uint8_t* dateTimeData = m_dataPool.getDataFor(chunkIndex);
        *reinterpret_cast<ResourceID*>(dateTimeData) = resourceID;
        dateTimeData += sizeof(ResourceID) + RESOURCE_ID_TO_XSDDATETIME_PACKING;
        *reinterpret_cast<XSDDateTime*>(dateTimeData) = *dateTime;
        ::atomicWrite(m_lexicalFormHandlesByResourceID[resourceID], chunkIndex);
        ::atomicWrite(m_datatypeIDsByResourceID[resourceID], resourceValue.getDatatypeID());
        m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, chunkIndex);
        m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else {
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        throw RDF_STORE_EXCEPTION("The hash table for date/time values is full.");
    }
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID DateTimeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue, preferredResourceID);
}

template<class HTT>
ResourceID DateTimeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    ResourceValue resourceValue;
    parseResourceValue(resourceValue, lexicalForm, datatypeID);
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue, preferredResourceID);
}

template<class HTT>
void DateTimeDatatype<HTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("DateTimeDatatype");
    m_hashTable.save(outputStream);
}

template<class HTT>
void DateTimeDatatype<HTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("DateTimeDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load StringDatatype.");
    m_hashTable.load(inputStream);
}

template<class HTT>
std::unique_ptr<ComponentStatistics> DateTimeDatatype<HTT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("DateTimeDatatype"));
    const size_t size = m_hashTable.getNumberOfBuckets() * m_hashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Total number of buckets", m_hashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets", m_hashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%)", (m_hashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_hashTable.getNumberOfBuckets()));
    result->addIntegerItem("Aggregate size", size);
    return result;
}

// DateTimeDatatypeFactory

DATATYPE_FACTORY(DateTimeDatatypeFactory, DateTimeDatatype<ParallelHashTable<ConcurrentDateTimePolicy> >, DateTimeDatatype<SequentialHashTable<SequentialDateTimePolicy> >);

DateTimeDatatypeFactory::DateTimeDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{
        { D_XSD_DATE_TIME, s_xsdDateTime },
        { D_XSD_TIME, s_xsdTime },
        { D_XSD_DATE, s_xsdDate },
        { D_XSD_G_YEAR_MONTH, s_xsdGYearMonth },
        { D_XSD_G_YEAR, s_xsdGYear },
        { D_XSD_G_MONTH_DAY, s_xsdGMonthDay },
        { D_XSD_G_DAY, s_xsdGDay },
        { D_XSD_G_MONTH, s_xsdGMonth }
    }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{
        { s_xsdDateTime, D_XSD_DATE_TIME },
        { s_xsdDateTimeStamp, D_XSD_DATE_TIME },
        { s_xsdTime, D_XSD_TIME },
        { s_xsdDate, D_XSD_DATE },
        { s_xsdGYearMonth, D_XSD_G_YEAR_MONTH },
        { s_xsdGYear, D_XSD_G_YEAR },
        { s_xsdGMonthDay, D_XSD_G_MONTH_DAY},
        { s_xsdGDay, D_XSD_G_DAY },
        { s_xsdGMonth, D_XSD_G_MONTH }
    })
{
}

void DateTimeDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    ::parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
}

void DateTimeDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    ::parseResourceValue(resourceValue, lexicalForm, datatypeID);
}

void DateTimeDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const XSDDateTime*>(data)->toString();
}

void DateTimeDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    auto iterator = m_datatypeIRIsByID.find(datatypeID);
    if (iterator == m_datatypeIRIsByID.end()) {
        std::ostringstream message;
        message << "Datatype ID '" << static_cast<uint32_t>(datatypeID) << "' is not handled by the date/time handler.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
    literalText = '"';
    literalText.append(reinterpret_cast<const XSDDateTime*>(data)->toString());
    literalText.append("\"^^");
    literalText.append(prefixes.encodeIRI(iterator->second));
}

EffectiveBooleanValue DateTimeDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return EBV_ERROR;
}
