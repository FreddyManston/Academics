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
#include "StringPolicies.h"

// Declaration StringDatatype

class MemoryManager;

static const std::string s_xsdString(XSD_STRING);
static const std::string s_rdfPlainLiteral(RDF_PLAIN_LITERAL);

template<class HTT>
class StringDatatype : public Datatype {

public:

    typedef HTT HashTableType;
    typedef typename HTT::PolicyType PolicyType;

protected:

    mutable HashTableType m_xsdStringHashTable;
    mutable HashTableType m_rdfPlainLiteralHashTable;

public:

    StringDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

    void initialize(const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const;

    ResourceID tryResolveResource(HashTableType& hashTable, ThreadContext& threadContext, const std::string& lexicalForm) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const;

    ResourceID resolveResource(HashTableType& hashTable, ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// Implementation StringDatatype

template<class HTT>
StringDatatype<HTT>::StringDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool),
    m_xsdStringHashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, m_dataPool),
    m_rdfPlainLiteralHashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, m_dataPool)
{
}

template<class HTT>
void StringDatatype<HTT>::initialize(const size_t initialResourceCapacity) {
    if (!m_xsdStringHashTable.initialize(::getHashTableSize(static_cast<size_t>(0.4 * initialResourceCapacity))))
        throw RDF_STORE_EXCEPTION("Cannot initialize the xsd:string hash table in StringDatatype.");
    if (!m_rdfPlainLiteralHashTable.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize the rdf:PlainLiteral hash table in StringDatatype.");
}

template<class HTT>
void StringDatatype<HTT>::setNumberOfThreads(const size_t numberOfThreads) {
    m_xsdStringHashTable.setNumberOfThreads(numberOfThreads);
    m_rdfPlainLiteralHashTable.setNumberOfThreads(numberOfThreads);
}

template<class HTT>
void StringDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    const char* const lexicalFormData = reinterpret_cast<const char*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID));
    resourceValue.setString(datatypeID, lexicalFormData);
}

template<class HTT>
void StringDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    const char* const lexicalFormData = reinterpret_cast<const char*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID));
    lexicalForm = lexicalFormData;
    if (datatypeID == D_XSD_STRING)
        datatypeIRI = s_xsdString;
    else
        datatypeIRI = s_rdfPlainLiteral;
}

template<class HTT>
void StringDatatype<HTT>::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    const char* const lexicalFormData = reinterpret_cast<const char*>(m_dataPool.getDataFor(lexicalFormHandle) + sizeof(ResourceID));
    lexicalForm = lexicalFormData;
}

template<class HTT>
ResourceID StringDatatype<HTT>::tryResolveResource(typename StringDatatype<HTT>::HashTableType& hashTable, ThreadContext& threadContext, const std::string& lexicalForm) const {
    const char* const lexicalFormData = lexicalForm.c_str();
    typename HTT::BucketDescriptor bucketDescriptor;
    ResourceID resourceID;
    hashTable.acquireBucket(threadContext, bucketDescriptor, lexicalFormData);
    if (hashTable.continueBucketSearch(threadContext, bucketDescriptor, lexicalFormData) == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else
        resourceID = INVALID_RESOURCE_ID;
    hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID StringDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    const std::string& lexicalForm = resourceValue.getString();
    ResourceID resourceID;
    if (resourceValue.getDatatypeID() == D_XSD_STRING)
        resourceID = tryResolveResource(m_xsdStringHashTable, threadContext, lexicalForm);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1) {
            const std::string changedLexicalForm(lexicalForm, 0, length - 1);
            resourceID = tryResolveResource(m_xsdStringHashTable, threadContext, changedLexicalForm);
        }
        else
            resourceID = tryResolveResource(m_rdfPlainLiteralHashTable, threadContext, lexicalForm);
    }
    return resourceID;
}

template<class HTT>
ResourceID StringDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    if (datatypeIRI == s_xsdString)
        return tryResolveResource(m_xsdStringHashTable, threadContext, lexicalForm);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1) {
            const std::string changedLexicalForm(lexicalForm, 0, length - 1);
            return tryResolveResource(m_xsdStringHashTable, threadContext, changedLexicalForm);
        }
        else
            return tryResolveResource(m_rdfPlainLiteralHashTable, threadContext, lexicalForm);
    }
}

template<class HTT>
ResourceID StringDatatype<HTT>::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    if (datatypeID == D_XSD_STRING)
        return tryResolveResource(m_xsdStringHashTable, threadContext, lexicalForm);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1) {
            const std::string changedLexicalForm(lexicalForm, 0, length - 1);
            return tryResolveResource(m_xsdStringHashTable, threadContext, changedLexicalForm);
        }
        else
            return tryResolveResource(m_rdfPlainLiteralHashTable, threadContext, lexicalForm);
    }
}

template<class HTT>
ResourceID StringDatatype<HTT>::resolveResource(HashTableType& hashTable, ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    const char* lexicalFormData = lexicalForm.c_str();
    typename HTT::BucketDescriptor bucketDescriptor;
    BucketStatus bucketStatus;
    hashTable.acquireBucket(threadContext, bucketDescriptor, lexicalFormData);
    while ((bucketStatus = hashTable.continueBucketSearch(threadContext, bucketDescriptor, lexicalFormData)) == BUCKET_EMPTY) {
        if (hashTable.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    ResourceID resourceID;
    if (bucketStatus == BUCKET_EMPTY) {
        resourceID = nextResourceID<HTT::PolicyType::IS_PARALLEL>(dictionaryUsageContext, preferredResourceID);
        const uint64_t chunkIndex = m_dataPool.newDataChunk<HTT::PolicyType::IS_PARALLEL, ResourceID>(dictionaryUsageContext, sizeof(ResourceID) + lexicalForm.length() + 1);
        if (chunkIndex == DataPool::INVALID_CHUNK_INDEX) {
            hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, DataPool::INVALID_CHUNK_INDEX);
            hashTable.releaseBucket(threadContext, bucketDescriptor);
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
        ::atomicWrite(m_datatypeIDsByResourceID[resourceID], datatypeID);
        hashTable.getPolicy().setChunkIndex(bucketDescriptor.m_bucket, chunkIndex);
        hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    else if (bucketStatus == BUCKET_CONTAINS)
        resourceID = *reinterpret_cast<const ResourceID*>(m_dataPool.getDataFor(bucketDescriptor.m_bucketContents.m_chunkIndex));
    else {
        hashTable.releaseBucket(threadContext, bucketDescriptor);
        throw RDF_STORE_EXCEPTION("The hash table for strings is full.");
    }
    hashTable.releaseBucket(threadContext, bucketDescriptor);
    return resourceID;
}

template<class HTT>
ResourceID StringDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    const std::string& lexicalForm = resourceValue.getString();
    ResourceID resourceID;
    if (resourceValue.getDatatypeID() == D_XSD_STRING)
        resourceID = resolveResource(m_xsdStringHashTable, threadContext, dictionaryUsageContext, lexicalForm, D_XSD_STRING, preferredResourceID);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1) {
            const std::string changedLexicalForm(lexicalForm, 0, length - 1);
            resourceID = resolveResource(m_xsdStringHashTable, threadContext, dictionaryUsageContext, changedLexicalForm, D_XSD_STRING, preferredResourceID);
        }
        else
            resourceID = resolveResource(m_rdfPlainLiteralHashTable, threadContext, dictionaryUsageContext, lexicalForm, D_RDF_PLAIN_LITERAL, preferredResourceID);
    }
    return resourceID;
}

template<class HTT>
ResourceID StringDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    if (datatypeIRI == s_xsdString)
        return resolveResource(m_xsdStringHashTable, threadContext, dictionaryUsageContext, lexicalForm, D_XSD_STRING, preferredResourceID);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1) {
            const std::string changedLexicalForm(lexicalForm, 0, length - 1);
            return resolveResource(m_xsdStringHashTable, threadContext, dictionaryUsageContext, changedLexicalForm, D_XSD_STRING, preferredResourceID);
        }
        else
            return resolveResource(m_rdfPlainLiteralHashTable, threadContext, dictionaryUsageContext, lexicalForm, D_RDF_PLAIN_LITERAL, preferredResourceID);
    }
}

template<class HTT>
ResourceID StringDatatype<HTT>::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    if (datatypeID == D_XSD_STRING)
        return resolveResource(m_xsdStringHashTable, threadContext, dictionaryUsageContext, lexicalForm, D_XSD_STRING, preferredResourceID);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1) {
            const std::string changedLexicalForm(lexicalForm, 0, length - 1);
            return resolveResource(m_xsdStringHashTable, threadContext, dictionaryUsageContext, changedLexicalForm, D_XSD_STRING, preferredResourceID);
        }
        else
            return resolveResource(m_rdfPlainLiteralHashTable, threadContext, dictionaryUsageContext, lexicalForm, D_RDF_PLAIN_LITERAL, preferredResourceID);
    }
}

template<class HTT>
void StringDatatype<HTT>::save(OutputStream& outputStream) const {
    outputStream.writeString("StringDatatype");
    m_xsdStringHashTable.save(outputStream);
    m_rdfPlainLiteralHashTable.save(outputStream);
}

template<class HTT>
void StringDatatype<HTT>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("StringDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load StringDatatype.");
    m_xsdStringHashTable.load(inputStream);
    m_rdfPlainLiteralHashTable.load(inputStream);
}

template<class HTT>
std::unique_ptr<ComponentStatistics> StringDatatype<HTT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("StringDatatype"));
    const size_t xsdStringSize = m_xsdStringHashTable.getNumberOfBuckets() * m_xsdStringHashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size (xsd:string)", xsdStringSize);
    result->addIntegerItem("Total number of buckets (xsd:string)", m_xsdStringHashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets (xsd:string)", m_xsdStringHashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%, xsd:string)", (m_xsdStringHashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_xsdStringHashTable.getNumberOfBuckets()));
    const size_t rdfPlainLiteralSize = m_rdfPlainLiteralHashTable.getNumberOfBuckets() * m_rdfPlainLiteralHashTable.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size (rdf:PlainLiteral)", rdfPlainLiteralSize);
    result->addIntegerItem("Total number of buckets (rdf:PlainLiteral)", m_rdfPlainLiteralHashTable.getNumberOfBuckets());
    result->addIntegerItem("Number of used buckets (rdf:PlainLiteral)", m_rdfPlainLiteralHashTable.getNumberOfUsedBuckets());
    result->addFloatingPointItem("Load factor (%, rdf:PlainLiteral)", (m_rdfPlainLiteralHashTable.getNumberOfUsedBuckets() * 100.0) / static_cast<double>(m_rdfPlainLiteralHashTable.getNumberOfBuckets()));
    result->addIntegerItem("Aggregate size", xsdStringSize + rdfPlainLiteralSize);
    return result;
}

// StringDatatypeFactory

always_inline static bool isAlpha(const char c) {
    return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
}

always_inline static bool isAlphaNum(const char c) {
    return ::isAlpha(c) || ('0' <= c && c <= '9');
}

always_inline static bool isLanguageTagWellFormed(const std::string& lexicalForm, const size_t atPosition) {
    const size_t length = lexicalForm.length();
    size_t position = atPosition + 1;
    if (position >= length || !::isAlpha(lexicalForm[position]))
        return false;
    while (position < length && ::isAlpha(lexicalForm[position]))
        ++position;
    while (position < length && lexicalForm[position] == '-') {
        ++position;
        if (position < length && !::isAlphaNum(lexicalForm[position]))
            return false;
        while (position < length && ::isAlphaNum(lexicalForm[position]))
            ++position;
    }
    return position == length;
}

DATATYPE_FACTORY(StringDatatypeFactory, StringDatatype<ParallelHashTable<ConcurrentStringPolicy> >, StringDatatype<SequentialHashTable<SequentialStringPolicy> >);

StringDatatypeFactory::StringDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{
        { D_XSD_STRING, s_xsdString },
        { D_RDF_PLAIN_LITERAL, s_rdfPlainLiteral }
    }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{
        { s_xsdString, D_XSD_STRING },
        { s_rdfPlainLiteral, D_RDF_PLAIN_LITERAL }
    })
{
}

void StringDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    if (datatypeIRI == s_xsdString)
        resourceValue.setString(D_XSD_STRING, lexicalForm);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1)
            resourceValue.setString(D_XSD_STRING, lexicalForm, 0, length - 1);
        else if (isLanguageTagWellFormed(lexicalForm, atPosition))
            resourceValue.setString(D_RDF_PLAIN_LITERAL, lexicalForm);
        else {
            std::ostringstream message;
            message << "Literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it its language tag is malformed.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
    }
}

void StringDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    if (datatypeID == D_XSD_STRING)
        resourceValue.setString(D_XSD_STRING, lexicalForm);
    else {
        const size_t atPosition = lexicalForm.find_last_of('@');
        if (atPosition == std::string::npos) {
            std::ostringstream message;
            message << "The lexical form of literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it does not contain the '@' character.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const size_t length = lexicalForm.length();
        if (atPosition == length - 1)
            resourceValue.setString(D_XSD_STRING, lexicalForm, 0, length - 1);
        else if (isLanguageTagWellFormed(lexicalForm, atPosition))
            resourceValue.setString(D_RDF_PLAIN_LITERAL, lexicalForm);
        else {
            std::ostringstream message;
            message << "Literal '" << lexicalForm << "' of type rdf:PlainLiteral is incorrect because it its language tag is malformed.";
            throw RDF_STORE_EXCEPTION(message.str());
        }

    }
}

void StringDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    lexicalForm = reinterpret_cast<const char*>(data);
}

void StringDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    const char* current = reinterpret_cast<const char*>(data);
    const char* languageTag;
    if (datatypeID == D_XSD_STRING)
        languageTag = current + (dataSize - 1);
    else
        languageTag = ::strrchr(current, '@');
    literalText.clear();
    literalText.push_back('"');
    while (current != languageTag) {
        switch (*current) {
        case '\t':
            literalText.append("\\t");
            break;
        case '\b':
            literalText.append("\\b");
            break;
        case '\n':
            literalText.append("\\n");
            break;
        case '\r':
            literalText.append("\\r");
            break;
        case '\f':
            literalText.append("\\f");
            break;
        case '\"':
            literalText.push_back('\\');
            // deliberate fall-through!
        default:
            literalText.push_back(*current);
            break;
        }
        ++current;
    }
    literalText.push_back('"');
    if (languageTag == nullptr)
        literalText.append(prefixes.encodeIRI(s_rdfPlainLiteral));
    else {
        while (*languageTag) {
            literalText.push_back(*languageTag);
            ++languageTag;
        }
    }
}

EffectiveBooleanValue StringDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return resourceValue.getStringLength() != 0 ? EBV_TRUE : EBV_FALSE;
}
