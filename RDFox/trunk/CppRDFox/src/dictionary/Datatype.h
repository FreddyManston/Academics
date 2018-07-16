// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATATYPE_H_
#define DATATYPE_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/MemoryRegion.h"
#include "DictionaryUsageContext.h"

class MemoryManager;
class ComponentStatistics;
class DataPool;
class InputStream;
class OutputStream;
class ThreadContext;

const DatatypeID D_LOCK_ID = static_cast<DatatypeID>(-1);

// Datatype

class Datatype : private Unmovable {

protected:

    ResourceID& m_nextResourceID;
    MemoryRegion<LexicalFormHandle>& m_lexicalFormHandlesByResourceID;
    MemoryRegion<DatatypeID>& m_datatypeIDsByResourceID;
    DataPool& m_dataPool;

    template<bool is_parallel>
    ResourceID nextResourceID(DictionaryUsageContext* dictionaryUsageContext, const ResourceID preferredResourceID);

    static const uint32_t RESOURCE_ID_WINDOW_SIZE = 1024;

public:

    Datatype(ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

    virtual ~Datatype();

    virtual void initialize(const size_t initialResourceCapacity) = 0;

    virtual void setNumberOfThreads(const size_t numberOfThreads) = 0;

    virtual void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const = 0;

    virtual void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const = 0;

    virtual void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const = 0;

    virtual ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const = 0;

    virtual ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const = 0;

    virtual ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const = 0;

    virtual ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) = 0;

    virtual ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) = 0;

    virtual ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) = 0;

    virtual void save(OutputStream& outputStream) const = 0;

    virtual void load(InputStream& inputStream) = 0;

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const = 0;

};

template<>
always_inline ResourceID Datatype::nextResourceID<true>(DictionaryUsageContext* dictionaryUsageContext, const ResourceID preferredResourceID) {
    if (preferredResourceID != INVALID_RESOURCE_ID) {
        if (!m_lexicalFormHandlesByResourceID.ensureEndAtLeast(preferredResourceID, 1) || !m_datatypeIDsByResourceID.ensureEndAtLeast(preferredResourceID, 1))
            throw RDF_STORE_EXCEPTION("The arrays for resolving resource IDs to lexical handles and datatypes are full.");
        if (::atomicConditionalSet(m_datatypeIDsByResourceID[preferredResourceID], D_INVALID_DATATYPE_ID, D_LOCK_ID)) {
            ResourceID nextResourceID;
            do {
                nextResourceID = ::atomicRead(m_nextResourceID);
            } while (nextResourceID <= preferredResourceID && !::atomicConditionalSet(m_nextResourceID, nextResourceID, preferredResourceID + 1));
            return preferredResourceID;
        }
    }
    ResourceID resourceID;
    do {
        if (dictionaryUsageContext == nullptr)
            resourceID = ::atomicIncrement(m_nextResourceID) - 1;
        else {
            if (dictionaryUsageContext->m_nextFreeResourceID >= dictionaryUsageContext->m_afterLastFreeResourceID) {
                ResourceID nextResourceID;
                ResourceID newNextResourceID;
                do {
                    nextResourceID = ::atomicRead(m_nextResourceID);
                    newNextResourceID = nextResourceID + RESOURCE_ID_WINDOW_SIZE;
                    if (newNextResourceID >= static_cast<ResourceID>(-1) - RESOURCE_ID_WINDOW_SIZE)
                        throw RDF_STORE_EXCEPTION("The capacity of RDFox for the number of resources has been exceeded.");
                } while(!::atomicConditionalSet(m_nextResourceID, nextResourceID, newNextResourceID));
                dictionaryUsageContext->m_nextFreeResourceID = nextResourceID;
                dictionaryUsageContext->m_afterLastFreeResourceID = newNextResourceID;
            }
            resourceID = dictionaryUsageContext->m_nextFreeResourceID++;
        }
        if (!m_lexicalFormHandlesByResourceID.ensureEndAtLeast(resourceID, 1) || !m_datatypeIDsByResourceID.ensureEndAtLeast(resourceID, 1))
            throw RDF_STORE_EXCEPTION("The arrays for resolving resource IDs to lexical handles and datatypes are full.");
    } while (!::atomicConditionalSet(m_datatypeIDsByResourceID[resourceID], D_INVALID_DATATYPE_ID, D_LOCK_ID));
    return resourceID;
}

template<>
always_inline ResourceID Datatype::nextResourceID<false>(DictionaryUsageContext* dictionaryUsageContext, const ResourceID preferredResourceID) {
    if (preferredResourceID != INVALID_RESOURCE_ID) {
        if (!m_lexicalFormHandlesByResourceID.ensureEndAtLeast(preferredResourceID, 1) || !m_datatypeIDsByResourceID.ensureEndAtLeast(preferredResourceID, 1))
            throw RDF_STORE_EXCEPTION("The arrays for resolving resource IDs to lexical handles and datatypes are full.");
        if (m_datatypeIDsByResourceID[preferredResourceID] == D_INVALID_DATATYPE_ID) {
            if (m_nextResourceID <= preferredResourceID)
                m_nextResourceID = preferredResourceID + 1;
            return preferredResourceID;
        }
    }
    ResourceID resourceID;
    do {
        resourceID = m_nextResourceID++;
        if (!m_lexicalFormHandlesByResourceID.ensureEndAtLeast(resourceID, 1) || !m_datatypeIDsByResourceID.ensureEndAtLeast(resourceID, 1))
            throw RDF_STORE_EXCEPTION("The arrays for resolving resource IDs to lexical handles and datatypes are full.");
    } while (m_datatypeIDsByResourceID[resourceID] != D_INVALID_DATATYPE_ID);
    return resourceID;
}

// DatatypeFactory

class DatatypeFactory {

public:

    DatatypeFactory();

    virtual ~DatatypeFactory();

    void registerFactory();

    void unregisterFactory();

    virtual const std::unordered_map<DatatypeID, std::string>& getDatatypeIRIsByID() const = 0;

    virtual const std::unordered_map<std::string, DatatypeID>& getDatatypeIDsByAllIRIs() const = 0;

    virtual std::unique_ptr<Datatype> createDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool, const bool shouldBeConcurrent) const = 0;

    virtual void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const = 0;

    virtual void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const = 0;

    virtual void toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const = 0;

    virtual void toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const = 0;

    virtual EffectiveBooleanValue getEffectiveBooleanValue(const ResourceValue& resourceValue) const = 0;

};

// DatatypeFactoryInitializer

template<class DFT>
class DatatypeFactoryInitializer {

protected:

    DFT m_datatypeFactory;

public:

    DatatypeFactoryInitializer() : m_datatypeFactory() {
        m_datatypeFactory.registerFactory();
    }

};

// Datatype factory macro

#define DATATYPE_FACTORY(N, CDT, SDT) \
    class N : public DatatypeFactory { \
    \
    protected: \
    \
        const std::unordered_map<DatatypeID, std::string> m_datatypeIRIsByID; \
        const std::unordered_map<std::string, DatatypeID> m_datatypeIDsByAllIRIs; \
    \
    public: \
    \
        N(); \
    \
        virtual const std::unordered_map<DatatypeID, std::string>& getDatatypeIRIsByID() const; \
    \
        virtual const std::unordered_map<std::string, DatatypeID>& getDatatypeIDsByAllIRIs() const; \
    \
        virtual std::unique_ptr<Datatype> createDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool, const bool shouldBeConcurrent) const; \
    \
        virtual void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const; \
    \
        virtual void parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const; \
    \
        virtual void toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const; \
    \
        virtual void toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const; \
    \
        virtual EffectiveBooleanValue getEffectiveBooleanValue(const ResourceValue& resourceValue) const; \
    \
    }; \
    \
    const std::unordered_map<DatatypeID, std::string>& N::getDatatypeIRIsByID() const { \
        return m_datatypeIRIsByID; \
    } \
    \
    const std::unordered_map<std::string, DatatypeID>& N::getDatatypeIDsByAllIRIs() const { \
        return m_datatypeIDsByAllIRIs; \
    } \
    \
    std::unique_ptr<Datatype> N::createDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool, const bool shouldBeConcurrent) const { \
        if (shouldBeConcurrent) \
            return std::unique_ptr<Datatype>(new CDT(memoryManager, nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool)); \
        else \
            return std::unique_ptr<Datatype>(new SDT(memoryManager, nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool)); \
    } \
    \
    static DatatypeFactoryInitializer<N> s_datatypeFactoryInitializer

#endif /* DATATYPE_H_ */
