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
#include "DataPoolImpl.h"
#include "Datatype.h"

typedef uint32_t PrefixID;
typedef size_t ALIGNMENT_TYPE;
const size_t PREFIX_ID_TO_SIZE_T_PACKING = (SUPPORTS_UNALIGNED_ACCESS ? 0 : sizeof(size_t) - sizeof(PrefixID));

const PrefixID INVALID_PREFIX_ID = 0;

// AbstractPrefixManagerPolicy

class AbstractPrefixManagerPolicy {

protected:

    DataPool& m_dataPool;

public:

    struct BucketContents {
        uint64_t m_chunkIndex;
    };

    always_inline AbstractPrefixManagerPolicy(DataPool& dataPool) : m_dataPool(dataPool) {
    }

    always_inline DataPool& getDataPool() {
        return m_dataPool;
    }

    always_inline BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const char* lexicalForm, const size_t lexicalFormLength) const {
        if (bucketContents.m_chunkIndex == 0)
            return BUCKET_EMPTY;
        else {
            const uint8_t* bucketData = m_dataPool.getDataFor(bucketContents.m_chunkIndex);
            if (*reinterpret_cast<const size_t*>(bucketData + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING) != valuesHashCode || *reinterpret_cast<const size_t*>(bucketData + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t)) != lexicalFormLength)
                return BUCKET_NOT_CONTAINS;
            const char* bucketLexicalForm = reinterpret_cast<const char*>(bucketData + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t) + sizeof(size_t));
            for (size_t index = 0; index < lexicalFormLength; ++index) {
                if (*bucketLexicalForm != *lexicalForm)
                    return BUCKET_NOT_CONTAINS;
                ++bucketLexicalForm;
                ++lexicalForm;
            }
            return BUCKET_CONTAINS;
        }
    }

    always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_chunkIndex == 0;
    }

    always_inline size_t getBucketContentsHashCode(const BucketContents& bucketContents) const {
        return *reinterpret_cast<const size_t*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING);
    }

    always_inline static size_t hashCodeFor(const char* lexicalForm, const size_t lexicalFormLength) {
        size_t result = static_cast<size_t>(14695981039346656037ULL);
        for (size_t index = 0; index < lexicalFormLength; ++index) {
            result ^= static_cast<size_t>(*(lexicalForm)++);
            result *= static_cast<size_t>(1099511628211ULL);
        }
        return result;
    }

};

// SequentialPrefixManagerPolicy

class SequentialPrefixManagerPolicy : public AbstractPrefixManagerPolicy {

public:

    static const bool IS_PARALLEL = false;

    static const size_t BUCKET_SIZE = 6;

    always_inline SequentialPrefixManagerPolicy(DataPool& dataPool) : AbstractPrefixManagerPolicy(dataPool) {
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

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return true;
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::read6(bucket);
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::write6(bucket, chunkIndex);
    }

};

// ConcurrentPrefixManagerPolicy

class ConcurrentPrefixManagerPolicy : public AbstractPrefixManagerPolicy {

public:

    static const bool IS_PARALLEL = true;

    static const size_t BUCKET_SIZE = sizeof(uint64_t);

    static const uint64_t IN_INSERTION_CHUNK_INDEX = static_cast<uint64_t>(-1);

    always_inline ConcurrentPrefixManagerPolicy(DataPool& dataPool) : AbstractPrefixManagerPolicy(dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        do {
            bucketContents.m_chunkIndex = getChunkIndex(bucket);
        } while (bucketContents.m_chunkIndex == IN_INSERTION_CHUNK_INDEX);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, bucketContents.m_chunkIndex);
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, IN_INSERTION_CHUNK_INDEX);
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::atomicRead(*reinterpret_cast<const uint64_t*>(bucket));
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::atomicWrite(*reinterpret_cast<uint64_t*>(bucket), chunkIndex);
    }

};

// PrefixManager

template<class HTT>
class PrefixManager {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    PrefixID m_nextPrefixID;
    mutable HashTableType m_hashTable;
    MemoryRegion<uint64_t> m_prefixDataByIDs;

    static const PrefixID PREFIX_ID_WINDOW_SIZE = 1024;

public:

    PrefixManager(MemoryManager& memoryManager, DataPool& dataPool);

    DataPool& getDataPool();

    const DataPool& getDataPool() const;

    void initialize();

    void setNumberOfThreads(const size_t numberOfThreads);

    void getPrefixIDLexicalForm(const PrefixID prefixID, const char * & prefixLexicalForm, size_t& prefixLexicalFormLength) const;

    size_t getPrefixIDHashCode(const PrefixID prefixID) const;

    PrefixID getPrefixID(ThreadContext& threadContext, const char* const prefixLexicalForm, const size_t prefixLexicalFormLength, size_t& prefixHashCode) const;

    PrefixID resolvePrefix(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const char* prefixLexicalForm, const size_t prefixLexicalFormLength, size_t& prefixHashCode);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

    __ALIGNED(PrefixManager)

};

// Definition PrefixManager

template<class HTT>
PrefixManager<HTT>::PrefixManager(MemoryManager& memoryManager, DataPool& dataPool) :
    m_nextPrefixID(1),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, dataPool),
    m_prefixDataByIDs(memoryManager)
{
}

template<class HTT>
always_inline DataPool& PrefixManager<HTT>::getDataPool() {
    return m_hashTable.getPolicy().getDataPool();
}

template<class HTT>
always_inline const DataPool& PrefixManager<HTT>::getDataPool() const {
    return m_hashTable.getPolicy().getDataPool();
}

template<class HTT>
always_inline void PrefixManager<HTT>::initialize() {
    m_nextPrefixID = 1;
    m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE);
    m_prefixDataByIDs.initialize(0xffffffff);
}

template<class HTT>
void PrefixManager<HTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_hashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT>
always_inline void PrefixManager<HTT>::getPrefixIDLexicalForm(const PrefixID prefixID, const char * & prefixLexicalForm, size_t& prefixLexicalFormLength) const {
    const uint8_t* prefixData = getDataPool().getDataFor(m_prefixDataByIDs[prefixID]);
    prefixLexicalForm =  reinterpret_cast<const char*>(prefixData + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t) + sizeof(size_t));
    prefixLexicalFormLength = *reinterpret_cast<const size_t*>(prefixData + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t));
}

template<class HTT>
always_inline size_t PrefixManager<HTT>::getPrefixIDHashCode(const PrefixID prefixID) const {
    return *reinterpret_cast<const size_t*>(getDataPool().getDataFor(m_prefixDataByIDs[prefixID]) + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING);
}

template<class HTT>
PrefixID PrefixManager<HTT>::getPrefixID(ThreadContext& threadContext, const char* const prefixLexicalForm, const size_t prefixLexicalFormLength, size_t& prefixHashCode) const {
    typename HTT::BucketDescriptor bucketDescriptor;
    PrefixID prefixID;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, prefixLexicalForm, prefixLexicalFormLength);
    prefixHashCode = bucketDescriptor.m_hashCode;
    if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, prefixLexicalForm, prefixLexicalFormLength) == BUCKET_CONTAINS)
        prefixID = *reinterpret_cast<const PrefixID*>(getDataPool().getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else
        prefixID = INVALID_PREFIX_ID;
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return prefixID;
}

template<class HTT>
PrefixID PrefixManager<HTT>::resolvePrefix(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const char* prefixLexicalForm, const size_t prefixLexicalFormLength, size_t& prefixHashCode) {
    typename HTT::BucketDescriptor bucketDescriptor;
    BucketStatus bucketStatus;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, prefixLexicalForm, prefixLexicalFormLength);
    prefixHashCode = bucketDescriptor.m_hashCode;
    while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, prefixLexicalForm, prefixLexicalFormLength)) == BUCKET_EMPTY) {
        if (m_hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    PrefixID prefixID;
    if (bucketStatus == BUCKET_EMPTY) {
        if (HTT::PolicyType::IS_PARALLEL) {
            if (dictionaryUsageContext == nullptr) {
                prefixID = ::atomicIncrement(m_nextPrefixID) - 1;
                if (prefixID >= static_cast<PrefixID>(-1) - PREFIX_ID_WINDOW_SIZE) {
                    m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
                    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
                    throw RDF_STORE_EXCEPTION("The capacity of RDFox for the number of prefixes has been exceeded.");
                }
            }
            else {
                if (dictionaryUsageContext->m_nextFreePrefixID >= dictionaryUsageContext->m_afterLastFreePrefixID) {
                    PrefixID nextPrefixID;
                    PrefixID newNextPrefixID;
                    do {
                        nextPrefixID = ::atomicRead(m_nextPrefixID);
                        newNextPrefixID = nextPrefixID + PREFIX_ID_WINDOW_SIZE;
                        if (newNextPrefixID >= static_cast<PrefixID>(-1) - PREFIX_ID_WINDOW_SIZE) {
                            m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
                            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
                            throw RDF_STORE_EXCEPTION("The capacity of RDFox for the number of prefixes has been exceeded.");
                        }
                    } while(!::atomicConditionalSet(m_nextPrefixID, nextPrefixID, newNextPrefixID));
                    dictionaryUsageContext->m_nextFreePrefixID = nextPrefixID;
                    dictionaryUsageContext->m_afterLastFreePrefixID = newNextPrefixID;
                }
                prefixID = dictionaryUsageContext->m_nextFreePrefixID++;
            }
        }
        else
            prefixID = m_nextPrefixID++;
        if (!m_prefixDataByIDs.ensureEndAtLeast(prefixID, 1)) {
            m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
            throw RDF_STORE_EXCEPTION("The array for resolving prefix IDs to chunks is full.");
        }
        else {
            const uint64_t chunkIndex = getDataPool().template newDataChunk<HTT::PolicyType::IS_PARALLEL, ALIGNMENT_TYPE>(dictionaryUsageContext, sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t) + sizeof(size_t) + prefixLexicalFormLength);
            if (chunkIndex == DataPool::INVALID_CHUNK_INDEX) {
                m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
                m_hashTable.releaseBucket(threadContext, bucketDescriptor);
                throw RDF_STORE_EXCEPTION("The data pool is full.");
            }
            else {
                uint8_t* prefixData = getDataPool().getDataFor(chunkIndex);
                *reinterpret_cast<PrefixID*>(prefixData) = prefixID;
                prefixData += sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING;
                *reinterpret_cast<size_t*>(prefixData) = bucketDescriptor.m_hashCode;
                prefixData += sizeof(size_t);
                *reinterpret_cast<size_t*>(prefixData) = prefixLexicalFormLength;
                prefixData += sizeof(size_t);
                for (size_t index = 0; index < prefixLexicalFormLength; ++index)
                    *(prefixData++) = *(prefixLexicalForm++);
                ::atomicWrite(m_prefixDataByIDs[prefixID], chunkIndex);
                m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, chunkIndex);
                m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
            }
        }
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        prefixID = *reinterpret_cast<const PrefixID*>(getDataPool().getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else {
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        throw RDF_STORE_EXCEPTION("The hash table for strings is full.");
    }
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return prefixID;
}

template<class HTT>
void PrefixManager<HTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("PrefixManager");
    outputStream.write<PrefixID>(m_nextPrefixID);
    m_hashTable.save(outputStream);
    outputStream.writeMemoryRegion(m_prefixDataByIDs);
}

template<class HTT>
void PrefixManager<HTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("PrefixManager"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load PrefixManager.");
    m_nextPrefixID = inputStream.read<PrefixID>();
    m_hashTable.load(inputStream);
    inputStream.readMemoryRegion(m_prefixDataByIDs);
}

template<class HTT>
std::unique_ptr<ComponentStatistics> PrefixManager<HTT>::getComponentStatistics() const {
    size_t dataPoolSize = 0;
    for (PrefixID prefixID = 1; prefixID < m_nextPrefixID; ++prefixID) {
        const uint64_t chunkIndex = m_prefixDataByIDs[prefixID];
        if (chunkIndex != 0) {
            const uint8_t* const prefixData = getDataPool().getDataFor(chunkIndex);
            dataPoolSize += sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t) + sizeof(size_t) + *reinterpret_cast<const size_t*>(prefixData + sizeof(PrefixID) + PREFIX_ID_TO_SIZE_T_PACKING + sizeof(size_t));
        }
    }
    uint64_t hashTableSize = m_hashTable.getNumberOfBuckets() * m_hashTable.getPolicy().BUCKET_SIZE;
    uint64_t prefixesByIndexSize = m_nextPrefixID * sizeof(uint64_t);
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("PrefixManager"));
    result->addIntegerItem("Hash table size", hashTableSize);
    result->addIntegerItem("Total number of buckets", m_hashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets", m_hashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%)", (m_hashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_hashTable.getNumberOfBuckets()));
    result->addIntegerItem("Prefix ID mapping size", prefixesByIndexSize);
    result->addIntegerItem("Aggregate size", hashTableSize + prefixesByIndexSize);
    result->addIntegerItem("Size in the data pool", dataPoolSize);
    return result;
}

// AbstractIRIReferencePolicy

template<class PMHTT>
class AbstractIRIReferencePolicy {

public:

    PrefixManager<PMHTT> m_prefixManager;

    struct BucketContents {
        uint64_t m_chunkIndex;
    };

    always_inline AbstractIRIReferencePolicy(MemoryManager& memoryManager, DataPool& dataPool) : m_prefixManager(memoryManager, dataPool) {
    }

    always_inline BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const PrefixID prefixID, const size_t prefixHashCode, const char* suffixLexicalForm) const {
        if (bucketContents.m_chunkIndex == 0)
            return BUCKET_EMPTY;
        else {
            const uint8_t* bucketData = m_prefixManager.getDataPool().getDataFor(bucketContents.m_chunkIndex);
            if (*reinterpret_cast<const PrefixID*>(bucketData + sizeof(ResourceID)) != prefixID)
                return BUCKET_NOT_CONTAINS;
            const char* bucketSuffixLexicalForm = reinterpret_cast<const char*>(bucketData + sizeof(ResourceID) + sizeof(PrefixID));
            while (*suffixLexicalForm != 0 && *bucketSuffixLexicalForm != 0) {
                if (*suffixLexicalForm != *bucketSuffixLexicalForm)
                    return BUCKET_NOT_CONTAINS;
                ++suffixLexicalForm;
                ++bucketSuffixLexicalForm;
            }
            if (*suffixLexicalForm == *bucketSuffixLexicalForm)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }
    }

    always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_chunkIndex == 0;
    }

    always_inline size_t getBucketContentsHashCode(const BucketContents& bucketContents) const {
        const uint8_t* bucketData = m_prefixManager.getDataPool().getDataFor(bucketContents.m_chunkIndex);
        const PrefixID prefixID = *reinterpret_cast<const PrefixID*>(bucketData + sizeof(ResourceID));
        return hashCodeFor(prefixID, m_prefixManager.getPrefixIDHashCode(prefixID), reinterpret_cast<const char*>(bucketData + sizeof(ResourceID) + sizeof(PrefixID)));
    }

    always_inline static size_t hashCodeFor(const PrefixID prefixID, const size_t prefixHashCode, const char* suffixLexicalForm) {
        size_t result = prefixHashCode;
        uint8_t value;
        while ((value = *(suffixLexicalForm++)) != 0) {
            result ^= static_cast<size_t>(value);
            result *= static_cast<size_t>(1099511628211ULL);
        }
        return result;
    }

};

// SequentialIRIReferencePolicy

template<class PMHTT>
class SequentialIRIReferencePolicy : public AbstractIRIReferencePolicy<PMHTT> {

public:

    static const bool IS_PARALLEL = false;

    static const size_t BUCKET_SIZE = 6;

    always_inline SequentialIRIReferencePolicy(MemoryManager& memoryManager, DataPool& dataPool) : AbstractIRIReferencePolicy<PMHTT>(memoryManager, dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, typename AbstractIRIReferencePolicy<PMHTT>::BucketContents& bucketContents) {
        bucketContents.m_chunkIndex = getChunkIndex(bucket);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const typename AbstractIRIReferencePolicy<PMHTT>::BucketContents& bucketContents) {
        if (getChunkIndex(bucket) == 0) {
            setChunkIndex(bucket, bucketContents.m_chunkIndex);
            return true;
        }
        else
            return false;
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return true;
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::read6(bucket);
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::write6(bucket, chunkIndex);
    }

};

// ConcurrentIRIReferencePolicy

template<class PMHTT>
class ConcurrentIRIReferencePolicy : public AbstractIRIReferencePolicy<PMHTT> {

public:

    static const bool IS_PARALLEL = true;

    static const size_t BUCKET_SIZE = sizeof(uint64_t);

    static const uint64_t IN_INSERTION_CHUNK_INDEX = static_cast<uint64_t>(-1);

    always_inline ConcurrentIRIReferencePolicy(MemoryManager& memoryManager, DataPool& dataPool) : AbstractIRIReferencePolicy<PMHTT>(memoryManager, dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, typename AbstractIRIReferencePolicy<PMHTT>::BucketContents& bucketContents) {
        do {
            bucketContents.m_chunkIndex = getChunkIndex(bucket);
        } while (bucketContents.m_chunkIndex == IN_INSERTION_CHUNK_INDEX);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const typename AbstractIRIReferencePolicy<PMHTT>::BucketContents& bucketContents) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, bucketContents.m_chunkIndex);
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, IN_INSERTION_CHUNK_INDEX);
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::atomicRead(*reinterpret_cast<const uint64_t*>(bucket));
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::atomicWrite(*reinterpret_cast<uint64_t*>(bucket), chunkIndex);
    }

};

// Declaration of IRIReferenceDatatype

class MemoryManager;

static const std::string s_iriReferenceDatatype("internal:iri-reference");

template<class HTT, class PMHTT>
class IRIReferenceDatatype : public Datatype {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    mutable HashTableType m_hashTable;

    size_t getPrefixLexicalFormLength(const char* lexicalForm, size_t lexicalFormLength) const;

public:

    IRIReferenceDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

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

// Implementation of IRIReferenceDatatype

template<class HTT, class PMHTT>
always_inline size_t IRIReferenceDatatype<HTT, PMHTT>::getPrefixLexicalFormLength(const char* lexicalForm, size_t lexicalFormLength) const {
    const char* scan = lexicalForm + lexicalFormLength;
    while (scan != lexicalForm) {
        char c = *(--scan);
        if (c == '/' || c == '#')
            break;
        --lexicalFormLength;
    }
    return lexicalFormLength;
}

template<class HTT, class PMHTT>
IRIReferenceDatatype<HTT, PMHTT>::IRIReferenceDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, memoryManager, m_dataPool)
{
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::initialize(const size_t initialResourceCapacity) {
    m_hashTable.getPolicy().m_prefixManager.initialize();
    if (!m_hashTable.initialize(::getHashTableSize(static_cast<size_t>(0.6 * initialResourceCapacity))))
        throw RDF_STORE_EXCEPTION("Cannot initialize the hash table in IRIReferenceDatatype.");
    if (resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, OWL_SAME_AS, OWL_SAME_AS_ID) != OWL_SAME_AS_ID)
        throw RDF_STORE_EXCEPTION("Internal error: owl:sameAs is not resolved to resource ID 1; this is probably because datatypes were ordered in an incorrect order.");
    if (resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, OWL_DIFFERENT_FROM, OWL_DIFFERENT_FROM_ID) != OWL_DIFFERENT_FROM_ID)
        throw RDF_STORE_EXCEPTION("Internal error: owl:differentFrom is not resolved to resource ID 2; this is probably because datatypes were ordered in an incorrect order.");
    if (resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, RDF_TYPE, RDF_TYPE_ID) != RDF_TYPE_ID)
        throw RDF_STORE_EXCEPTION("Internal error: rdf:type is not resolved to resource ID 3; this is probably because datatypes were ordered in an incorrect order.");
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_hashTable.getPolicy().m_prefixManager.setNumberOfThreads(numberOfThreads);
    m_hashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    const uint8_t* lexicalFormData = m_dataPool.getDataFor(lexicalFormHandle);
    const PrefixID prefixID = *reinterpret_cast<const PrefixID*>(lexicalFormData + sizeof(ResourceID));
    const char* prefixLexicalForm;
    size_t prefixLexicalFormLength;
    m_hashTable.getPolicy().m_prefixManager.getPrefixIDLexicalForm(prefixID, prefixLexicalForm, prefixLexicalFormLength);
    const char* localNameLexicalForm = reinterpret_cast<const char*>(lexicalFormData + sizeof(ResourceID) + sizeof(PrefixID));
    const size_t localNameLexicalFormLength = ::strlen(localNameLexicalForm);
    resourceValue.setString(datatypeID, prefixLexicalForm, prefixLexicalFormLength, localNameLexicalForm, localNameLexicalFormLength);
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    const uint8_t* lexicalFormData = m_dataPool.getDataFor(lexicalFormHandle);
    const PrefixID prefixID = *reinterpret_cast<const PrefixID*>(lexicalFormData + sizeof(ResourceID));
    const char* prefixLexicalForm;
    size_t prefixLexicalFormLength;
    m_hashTable.getPolicy().m_prefixManager.getPrefixIDLexicalForm(prefixID, prefixLexicalForm, prefixLexicalFormLength);
    lexicalForm.assign(prefixLexicalForm, prefixLexicalFormLength);
    lexicalForm.append(reinterpret_cast<const char*>(lexicalFormData + sizeof(ResourceID) + sizeof(PrefixID)));
    datatypeIRI = s_iriReferenceDatatype;
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    const uint8_t* lexicalFormData = m_dataPool.getDataFor(lexicalFormHandle);
    const PrefixID prefixID = *reinterpret_cast<const PrefixID*>(lexicalFormData + sizeof(ResourceID));
    const char* prefixLexicalForm;
    size_t prefixLexicalFormLength;
    m_hashTable.getPolicy().m_prefixManager.getPrefixIDLexicalForm(prefixID, prefixLexicalForm, prefixLexicalFormLength);
    lexicalForm.assign(prefixLexicalForm, prefixLexicalFormLength);
    lexicalForm.append(reinterpret_cast<const char*>(lexicalFormData + sizeof(ResourceID) + sizeof(PrefixID)));
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm) const {
    const char* lexicalFormData = lexicalForm.c_str();
    const size_t prefixLexicalFormLength = getPrefixLexicalFormLength(lexicalFormData, lexicalForm.length());
    size_t prefixHashCode;
    const PrefixID prefixID = m_hashTable.getPolicy().m_prefixManager.getPrefixID(threadContext, lexicalFormData, prefixLexicalFormLength, prefixHashCode);
    ResourceID resourceID;
    if (prefixID == INVALID_PREFIX_ID)
        resourceID = INVALID_RESOURCE_ID;
    else {
        const char* suffixLexicalForm = lexicalFormData + prefixLexicalFormLength;
        typename HTT::BucketDescriptor bucketDescriptor;
        m_hashTable.acquireBucket(threadContext, bucketDescriptor, prefixID, prefixHashCode, suffixLexicalForm);
        if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, prefixID, prefixHashCode, suffixLexicalForm) == BUCKET_CONTAINS)
            resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
        else
            resourceID = INVALID_RESOURCE_ID;
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    }
    return resourceID;
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    return tryResolveResource(threadContext, resourceValue.getString());
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    return tryResolveResource(threadContext, lexicalForm);
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    return tryResolveResource(threadContext, lexicalForm);
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const ResourceID preferredResourceID) {
    const char* lexicalFormData = lexicalForm.c_str();
    const size_t prefixLexicalFormLength = getPrefixLexicalFormLength(lexicalFormData, lexicalForm.length());
    size_t prefixHashCode;
    const PrefixID prefixID = m_hashTable.getPolicy().m_prefixManager.resolvePrefix(threadContext, dictionaryUsageContext, lexicalFormData, prefixLexicalFormLength, prefixHashCode);
    const char* suffixLexicalForm = lexicalFormData + prefixLexicalFormLength;
    ResourceID resourceID;
    typename HTT::BucketDescriptor bucketDescriptor;
    BucketStatus bucketStatus;
    m_hashTable.acquireBucket(threadContext, bucketDescriptor, prefixID, prefixHashCode, suffixLexicalForm);
    while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, prefixID, prefixHashCode, suffixLexicalForm)) == BUCKET_EMPTY) {
        if (m_hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    if (bucketStatus == BUCKET_EMPTY) {
        resourceID = nextResourceID<HTT::PolicyType::IS_PARALLEL>(dictionaryUsageContext, preferredResourceID);
        const uint64_t chunkIndex = m_dataPool.newDataChunk<HTT::PolicyType::IS_PARALLEL, ResourceID>(dictionaryUsageContext, sizeof(ResourceID) + sizeof(PrefixID) + lexicalForm.length() - prefixLexicalFormLength + 1);
        if (chunkIndex == DataPool::INVALID_CHUNK_INDEX) {
            m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
            ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_INVALID_DATATYPE_ID);
            throw RDF_STORE_EXCEPTION("The data pool is full.");
        }
        uint8_t* iriReferenceData = m_dataPool.getDataFor(chunkIndex);
        *reinterpret_cast<ResourceID*>(iriReferenceData) = resourceID;
        iriReferenceData += sizeof(ResourceID);
        *reinterpret_cast<PrefixID*>(iriReferenceData) = prefixID;
        iriReferenceData += sizeof(PrefixID);
        uint8_t value;
        do {
            value = *(suffixLexicalForm++);
            *(iriReferenceData++) = value;
        } while (value != 0);
        ::atomicWrite(m_lexicalFormHandlesByResourceID[resourceID], chunkIndex);
        ::atomicWrite(m_datatypeIDsByResourceID[resourceID], D_IRI_REFERENCE);
        m_hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, chunkIndex);
        m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else
        throw RDF_STORE_EXCEPTION("The hash table for strings is full.");
    m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, resourceValue.getString(), preferredResourceID);
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, lexicalForm, preferredResourceID);
}

template<class HTT, class PMHTT>
ResourceID IRIReferenceDatatype<HTT, PMHTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, dictionaryUsageContext, lexicalForm, preferredResourceID);
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("IRIReferenceDatatype");
    m_hashTable.getPolicy().m_prefixManager.save(outputStream);
    m_hashTable.save(outputStream);
}

template<class HTT, class PMHTT>
void IRIReferenceDatatype<HTT, PMHTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("IRIReferenceDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load StringDatatype.");
    m_hashTable.getPolicy().m_prefixManager.load(inputStream);
    m_hashTable.load(inputStream);
}

template<class HTT, class PMHTT>
std::unique_ptr<ComponentStatistics> IRIReferenceDatatype<HTT, PMHTT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("IRIReferenceDatatype"));
    const size_t size = m_hashTable.getNumberOfBuckets() * m_hashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Total number of buckets", m_hashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets", m_hashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%)", (m_hashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_hashTable.getNumberOfBuckets()));
    result->addIntegerItem("Aggregate size", size);
    return result;
}

// IRIReferenceDatatypeFactory

typedef ParallelHashTable<ConcurrentPrefixManagerPolicy> ConcurrentPrefixManagerHashTable;
typedef SequentialHashTable<SequentialPrefixManagerPolicy> SequentialPrefixManagerHashTable;
typedef IRIReferenceDatatype<ParallelHashTable<ConcurrentIRIReferencePolicy<ConcurrentPrefixManagerHashTable> >, ConcurrentPrefixManagerHashTable> ConcurrentIRIReferenceDatatype;
typedef IRIReferenceDatatype<SequentialHashTable<SequentialIRIReferencePolicy<SequentialPrefixManagerHashTable> >, SequentialPrefixManagerHashTable> SequentialIRIReferenceDatatype;

DATATYPE_FACTORY(IRIReferenceDatatypeFactory, ConcurrentIRIReferenceDatatype, SequentialIRIReferenceDatatype);

IRIReferenceDatatypeFactory::IRIReferenceDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{ { D_IRI_REFERENCE, s_iriReferenceDatatype } }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{ { s_iriReferenceDatatype, D_IRI_REFERENCE } })
{
}

void IRIReferenceDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    resourceValue.setString(D_IRI_REFERENCE, lexicalForm);
}

void IRIReferenceDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    resourceValue.setString(D_IRI_REFERENCE, lexicalForm);
}

void IRIReferenceDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const char*>(data);
}

void IRIReferenceDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    prefixes.encodeIRI(reinterpret_cast<const char*>(data), literalText);
}

EffectiveBooleanValue IRIReferenceDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return EBV_ERROR;
}
