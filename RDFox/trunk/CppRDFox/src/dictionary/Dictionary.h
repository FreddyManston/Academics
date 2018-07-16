// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DICTIONARY_H_
#define DICTIONARY_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/MemoryRegion.h"
#include "../util/ThreadContext.h"
#include "DictionaryUsageContext.h"
#include "DataPool.h"

class MemoryManager;
class ComponentStatistics;
class InputStream;
class OutputStream;
class Datatype;
class DatatypeFactory;

class Dictionary : private Unmovable {

protected:

    friend class FrequencyCounter;

    typedef Datatype* DatatypePtr;
    typedef const DatatypeFactory* DatatypeFactoryPtr;

    static std::vector<const DatatypeFactory*>& getRegisteredDatatypeFactories();
    static DatatypeFactoryPtr* getDatatypeFactoriesByID();
    static std::string* getDatatypeIRIsByID();
    static std::map<std::string, DatatypeID>& getDatatypeIDsByIRI();

    ResourceID m_nextResourceID;
    MemoryRegion<LexicalFormHandle> m_lexicalFormHandlesByResourceID;
    MemoryRegion<DatatypeID> m_datatypeIDsByResourceID;
    DataPool m_dataPool;
    unique_ptr_vector<Datatype> m_datatypes;
    std::unordered_map<std::string, DatatypePtr> m_datatypesByIRIs;
    DatatypePtr m_datatypesByIDs[256];

    Datatype& getDatatype(const std::string& datatypeIRI) const;

    Datatype& getDatatype(const DatatypeID datatypeID) const;

    void reassignResourceID(const LexicalFormHandle lexicalFormHandle, const ResourceID newResourceID);

public:

    static void registerDatatypeFactory(const DatatypeFactory& datatypeFactory);

    static void unregisterDatatypeFactory(const DatatypeFactory& datatypeFactory);

    static always_inline const std::string& getDatatypeIRI(const DatatypeID datatypeID) {
        return getDatatypeIRIsByID()[datatypeID];
    }

    static always_inline DatatypeID getDatatypeID(const std::string& datatypeIRI) {
        std::map<std::string, DatatypeID>& datatypeIDsByIRI = getDatatypeIDsByIRI();
        std::map<std::string, DatatypeID>::const_iterator iterator = datatypeIDsByIRI.find(datatypeIRI);
        if (iterator == datatypeIDsByIRI.end())
            return D_INVALID_DATATYPE_ID;
        else
            return iterator->second;
    }

    static always_inline DatatypeID getDatatypeIDSafe(const std::string& datatypeIRI) {
        std::map<std::string, DatatypeID>& datatypeIDsByIRI = getDatatypeIDsByIRI();
        std::map<std::string, DatatypeID>::const_iterator iterator = datatypeIDsByIRI.find(datatypeIRI);
        if (iterator == datatypeIDsByIRI.end()) {
            std::ostringstream buffer;
            buffer << "Datatype IRI '" << datatypeIRI << "' is unknown.";
            throw RDF_STORE_EXCEPTION(buffer.str());
        }
        else
            return iterator->second;
    }

    static always_inline void parseResourceValue(ResourceValue& resourceValue, const ResourceText& resourceText) {
        parseResourceValue(resourceValue, resourceText.m_resourceType, resourceText.m_lexicalForm, resourceText.m_datatypeIRI);
    }

    static void parseResourceValue(ResourceValue& resourceValue, const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI);

    static void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID);

    static void toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm);

    static always_inline void toLexicalForm(const ResourceValue& resourceValue, std::string& lexicalForm) {
        toLexicalForm(resourceValue.getDatatypeID(), resourceValue.getDataRaw(), resourceValue.getDataSize(), lexicalForm);
    }

    static void toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText);

    static always_inline void toTurtleLiteral(const ResourceValue& resourceValue, const Prefixes& prefixes, std::string& literalText) {
        toTurtleLiteral(resourceValue.getDatatypeID(), resourceValue.getDataRaw(), resourceValue.getDataSize(), prefixes, literalText);
    }

    static EffectiveBooleanValue getEffectiveBooleanValue(const ResourceValue& resourceValue);

    Dictionary(MemoryManager& memoryManager, const bool shouldBeConcurrent);

    ~Dictionary();

    always_inline bool isValid(const ResourceID resourceID) const {
        return resourceID < ::atomicRead(m_nextResourceID);
    }

    always_inline ResourceID getNextResourceID() const {
        return ::atomicRead(m_nextResourceID);
    }

    always_inline ResourceID getMaxResourceID() const {
        return ::atomicRead(m_nextResourceID) - 1;
    }

    void initialize(const size_t initialResourceCapacity = 0);

    void setNumberOfThreads(const size_t numberOfThreads);

    always_inline DatatypeID getDatatypeID(const ResourceID resourceID) const {
        return m_datatypeIDsByResourceID[resourceID];
    }

    bool getResource(const ResourceID resourceID, ResourceValue& resourceValue) const;

    always_inline bool getResource(const ResourceID resourceID, ResourceText& resourceText) const {
        return getResource(resourceID, resourceText.m_resourceType, resourceText.m_lexicalForm, resourceText.m_datatypeIRI);
    }

    bool getResource(const ResourceID resourceID, ResourceType& resourceType, std::string& lexicalForm, std::string& datatypeIRI) const;

    bool getResource(const ResourceID resourceID, std::string& lexicalForm, DatatypeID& datatypeID) const;

    bool toTurtleLiteral(const ResourceID resourceID, const Prefixes& prefixes, std::string& literalText) const {
        ResourceValue resourceValue;
        if (getResource(resourceID, resourceValue)) {
            toTurtleLiteral(resourceValue, prefixes, literalText);
            return true;
        }
        else
            return false;
    }

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    always_inline ResourceID tryResolveResource(const ResourceValue& resourceValue) const {
        return tryResolveResource(ThreadContext::getCurrentThreadContext(), resourceValue);
    }

    always_inline ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceText& resourceText) const {
        return tryResolveResource(threadContext, resourceText.m_resourceType, resourceText.m_lexicalForm, resourceText.m_datatypeIRI);
    }

    always_inline ResourceID tryResolveResource(const ResourceText& resourceText) const {
        return tryResolveResource(ThreadContext::getCurrentThreadContext(), resourceText);
    }

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI) const;

    always_inline ResourceID tryResolveResource(const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI) const {
        return tryResolveResource(ThreadContext::getCurrentThreadContext(), resourceType, lexicalForm, datatypeIRI);
    }

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const;

    always_inline ResourceID tryResolveResource(const std::string& lexicalForm, const DatatypeID datatypeID) const {
        return tryResolveResource(ThreadContext::getCurrentThreadContext(), lexicalForm, datatypeID);
    }

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID = INVALID_RESOURCE_ID);


    always_inline ResourceID resolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID = INVALID_RESOURCE_ID) {
        return resolveResource(threadContext, nullptr, resourceValue, preferredResourceID);
    }
    always_inline ResourceID resolveResource(const ResourceValue& resourceValue, const ResourceID preferredResourceID = INVALID_RESOURCE_ID) {
        return resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, resourceValue, preferredResourceID);
    }

    always_inline ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceText& resourceText, const ResourceID preferredResourceID = INVALID_RESOURCE_ID) {
        return resolveResource(threadContext, dictionaryUsageContext, resourceText.m_resourceType, resourceText.m_lexicalForm, resourceText.m_datatypeIRI, preferredResourceID);
    }

    always_inline ResourceID resolveResource(const ResourceText& resourceText, const ResourceID preferredResourceID = INVALID_RESOURCE_ID) {
        return resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, resourceText, preferredResourceID);
    }

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID = INVALID_RESOURCE_ID);

    always_inline ResourceID resolveResource(const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID = INVALID_RESOURCE_ID) {
        return resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, resourceType, lexicalForm, datatypeIRI, preferredResourceID);
    }

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID = INVALID_RESOURCE_ID);

    always_inline ResourceID resolveResource(const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID = INVALID_RESOURCE_ID) {
        return resolveResource(ThreadContext::getCurrentThreadContext(), nullptr, lexicalForm, datatypeID, preferredResourceID);
    }

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

    __ALIGNED(Dictionary)

};


#endif /* DICTIONARY_H_ */
