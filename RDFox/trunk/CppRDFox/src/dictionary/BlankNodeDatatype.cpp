// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/SequentialHashTableImpl.h"
#include "../util/ParallelHashTableImpl.h"
#include "../util/ComponentStatistics.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/ThreadContext.h"
#include "DataPoolImpl.h"
#include "Datatype.h"
#include "StringPolicies.h"

// Declaration BlankNodeDatatype

class MemoryManager;

static const std::string s_blankNode("internal:blank-node");

template<class HTT>
class BlankNodeDatatype : public Datatype {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    mutable HashTableType m_hashTable;

public:

    BlankNodeDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

    void initialize(const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const;

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// Implementation BlankNodeDatatype

template<class HTT>
BlankNodeDatatype<HTT>::BlankNodeDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, m_dataPool)
{
}

template<class HTT>
void BlankNodeDatatype<HTT>::initialize(const size_t initialResourceCapacity) {
    if (!m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize the hash table in BlankNodeDatatype.");
}

template<class HTT>
void BlankNodeDatatype<HTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_hashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT>
void BlankNodeDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    const char* const lexicalFormData = reinterpret_cast<const char*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID));
    resourceValue.setString(datatypeID, lexicalFormData);
}

template<class HTT>
void BlankNodeDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    const char* const lexicalFormData = reinterpret_cast<const char*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID));
    lexicalForm = lexicalFormData;
    datatypeIRI = s_blankNode;
}

template<class HTT>
void BlankNodeDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    const char* const lexicalFormData = reinterpret_cast<const char*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID));
    lexicalForm = lexicalFormData;
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm) const {
    const char* const lexicalFormData = lexicalForm.c_str();
    typename HTT::BucketDescriptor bucketDescriptor;
    ResourceID resourceID;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, lexicalFormData);
    if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, lexicalFormData) == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else
        resourceID = INVALID_RESOURCE_ID;
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    return tryResolveResource(threadContext, resourceValue.getString());
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    return tryResolveResource(threadContext, lexicalForm);
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    return tryResolveResource(threadContext, lexicalForm);
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const ResourceID preferredResourceID) {
    const char* lexicalFormData = lexicalForm.c_str();
    typename HTT::BucketDescriptor bucketDescriptor;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, lexicalFormData);
    BucketStatus bucketStatus;
    while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, lexicalFormData)) == BUCKET_EMPTY) {
        if (m_hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    ResourceID resourceID;
    if (bucketStatus == BUCKET_EMPTY) {
        resourceID = nextResourceID<HTT::PolicyType::IS_PARALLEL>(dictionaryUsageContext, preferredResourceID);
        const uint64_t chunkIndex = m_dataPool.newDataChunk<HTT::PolicyType::IS_PARALLEL, ResourceID>(dictionaryUsageContext, sizeof(ResourceID) + lexicalForm.length() + 1);
        if (chunkIndex == DataPool::INVALID_CHUNK_INDEX) {
            m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
            ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_INVALID_DATATYPE_ID);
            throw RDF_STORE_EXCEPTION("The data pool is full.");
        }
        uint8_t* stringData = m_dataPool.getDataFor(chunkIndex);
        *reinterpret_cast<ResourceID*>(stringData) = resourceID;
        stringData += sizeof(ResourceID);
        uint8_t value;
        do {
            value = *(lexicalFormData++);
            *(stringData++) = value;
        } while (value != 0);
        ::atomicWrite(m_lexicalFormHandlesByResourceID[resourceID], chunkIndex);
        ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_BLANK_NODE);
        m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, chunkIndex);
        m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else {
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        throw RDF_STORE_EXCEPTION("The hash table for strings is full.");
    }
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue.getString(), preferredResourceID);
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, lexicalForm, preferredResourceID);
}

template<class HTT>
ResourceID BlankNodeDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, lexicalForm, preferredResourceID);
}

template<class HTT>
void BlankNodeDatatype<HTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("BlankNodeDatatype");
    m_hashTable.save(outputStream);
}

template<class HTT>
void BlankNodeDatatype<HTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("BlankNodeDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load StringDatatype.");
    m_hashTable.load(inputStream);
}

template<class HTT>
std::unique_ptr<ComponentStatistics> BlankNodeDatatype<HTT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("BlankNodeDatatype"));
    const size_t size = m_hashTable.getNumberOfBuckets() * m_hashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Total number of buckets", m_hashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets", m_hashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%)", (m_hashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_hashTable.getNumberOfBuckets()));
    result->addIntegerItem("Aggregate size", size);
    return result;
}

// BlankNodeDatatypeFactory

DATATYPE_FACTORY(BlankNodeDatatypeFactory, BlankNodeDatatype<ParallelHashTable<ConcurrentStringPolicy> >, BlankNodeDatatype<SequentialHashTable<SequentialStringPolicy> >);

BlankNodeDatatypeFactory::BlankNodeDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{ { D_BLANK_NODE, s_blankNode } }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{ { s_blankNode, D_BLANK_NODE } })
{
}

void BlankNodeDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    resourceValue.setString(D_BLANK_NODE, lexicalForm);
}

void BlankNodeDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    resourceValue.setString(D_BLANK_NODE, lexicalForm);
}

void BlankNodeDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const char*>(data);
}

void BlankNodeDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    literalText = "_:";
    literalText.append(reinterpret_cast<const char*>(data));
}

EffectiveBooleanValue BlankNodeDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return resourceValue.getStringLength() != 0 ? EBV_TRUE : EBV_FALSE;
}
