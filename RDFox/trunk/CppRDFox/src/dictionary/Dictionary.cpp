// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "DataPoolImpl.h"
#include "Dictionary.h"
#include "Datatype.h"

always_inline Datatype& Dictionary::getDatatype(const std::string& datatypeIRI) const {
    std::unordered_map<std::string, DatatypePtr>::const_iterator iterator = m_datatypesByIRIs.find(datatypeIRI);
    if (iterator == m_datatypesByIRIs.end()) {
        std::ostringstream buffer;
        buffer << "Datatype IRI '" << datatypeIRI << "' is unknown.";
        throw RDF_STORE_EXCEPTION(buffer.str());
    }
    else
        return *iterator->second;
}

always_inline Datatype& Dictionary::getDatatype(const DatatypeID datatypeID) const {
    if (datatypeID == D_INVALID_DATATYPE_ID) {
        std::ostringstream buffer;
        buffer << "Datatype ID '" << static_cast<int32_t>(datatypeID) << "' is unknown.";
        throw RDF_STORE_EXCEPTION(buffer.str());
    }
    else
        return *m_datatypesByIDs[datatypeID];
}

std::vector<const DatatypeFactory*>& Dictionary::getRegisteredDatatypeFactories() {
    static std::vector<const DatatypeFactory*> s_registeredDatatypeFactories;
    return s_registeredDatatypeFactories;
}

Dictionary::DatatypeFactoryPtr* Dictionary::getDatatypeFactoriesByID() {
    static DatatypeFactoryPtr s_datatypeFactoriesByID[256];
    return s_datatypeFactoriesByID;
}

std::string* Dictionary::getDatatypeIRIsByID() {
    static std::string s_datatypeIRIsByID[256];
    return s_datatypeIRIsByID;
}

std::map<std::string, DatatypeID>& Dictionary::getDatatypeIDsByIRI() {
    static std::map<std::string, DatatypeID> s_datatypeIDsByIRI;
    return s_datatypeIDsByIRI;
}

void Dictionary::registerDatatypeFactory(const DatatypeFactory& datatypeFactory) {
    getRegisteredDatatypeFactories().push_back(&datatypeFactory);
    const std::unordered_map<DatatypeID, std::string>& datatypeIRIsByIDs = datatypeFactory.getDatatypeIRIsByID();
    for (auto iterator = datatypeIRIsByIDs.begin(); iterator != datatypeIRIsByIDs.end(); ++iterator) {
        getDatatypeIRIsByID()[iterator->first] = iterator->second;
        getDatatypeFactoriesByID()[iterator->first] = &datatypeFactory;
    }
    const std::unordered_map<std::string, DatatypeID>& datatypeIDsByAllIRIs = datatypeFactory.getDatatypeIDsByAllIRIs();
    for (auto iterator = datatypeIDsByAllIRIs.begin(); iterator != datatypeIDsByAllIRIs.end(); ++iterator)
        getDatatypeIDsByIRI()[iterator->first] = iterator->second;
}

void Dictionary::unregisterDatatypeFactory(const DatatypeFactory& datatypeFactory) {
    std::vector<const DatatypeFactory*>& registeredDatatypeFactories = Dictionary::getRegisteredDatatypeFactories();
    for (std::vector<const DatatypeFactory*>::iterator iterator = registeredDatatypeFactories.begin(); iterator != registeredDatatypeFactories.end(); ++iterator) {
        if (*iterator == &datatypeFactory) {
            registeredDatatypeFactories.erase(iterator);
            const std::unordered_map<DatatypeID, std::string>& datatypeIRIsByIDs = datatypeFactory.getDatatypeIRIsByID();
            for (auto iterator = datatypeIRIsByIDs.begin(); iterator != datatypeIRIsByIDs.end(); ++iterator) {
                getDatatypeIRIsByID()[iterator->first].clear();
                getDatatypeFactoriesByID()[iterator->first] = nullptr;
            }
            const std::unordered_map<std::string, DatatypeID>& datatypeIDsByAllIRIs = datatypeFactory.getDatatypeIDsByAllIRIs();
            for (auto iterator = datatypeIDsByAllIRIs.begin(); iterator != datatypeIDsByAllIRIs.end(); ++iterator)
                getDatatypeIDsByIRI().erase(iterator->first);
            break;
        }
    }
}

void Dictionary::parseResourceValue(ResourceValue& resourceValue, const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI) {
    switch (resourceType) {
    case UNDEFINED_RESOURCE:
        resourceValue.setUndefined();
        break;
    case IRI_REFERENCE:
        getDatatypeFactoriesByID()[D_IRI_REFERENCE]->parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
        break;
    case BLANK_NODE:
        getDatatypeFactoriesByID()[D_BLANK_NODE]->parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
        break;
    case LITERAL:
        getDatatypeFactoriesByID()[getDatatypeIDSafe(datatypeIRI)]->parseResourceValue(resourceValue, lexicalForm, datatypeIRI);
        break;
    default:
        throw RDF_STORE_EXCEPTION("Unknown resource type.");
    }
}

void Dictionary::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) {
    if (datatypeID == D_INVALID_DATATYPE_ID)
        resourceValue.setUndefined();
    else
        getDatatypeFactoriesByID()[datatypeID]->parseResourceValue(resourceValue, lexicalForm, datatypeID);
}

void Dictionary::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) {
    if (datatypeID == D_INVALID_DATATYPE_ID)
        lexicalForm = "UNDEF";
    else
        getDatatypeFactoriesByID()[datatypeID]->toLexicalForm(datatypeID, data, dataSize, lexicalForm);
}

void Dictionary::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) {
    if (datatypeID == D_INVALID_DATATYPE_ID)
        literalText = "UNDEF";
    else
        getDatatypeFactoriesByID()[datatypeID]->toTurtleLiteral(datatypeID, data, dataSize, prefixes, literalText);
}

EffectiveBooleanValue Dictionary::getEffectiveBooleanValue(const ResourceValue& resourceValue) {
    if (resourceValue.getDatatypeID() == D_INVALID_DATATYPE_ID)
        return EBV_ERROR;
    else
        return getDatatypeFactoriesByID()[resourceValue.getDatatypeID()]->getEffectiveBooleanValue(resourceValue);
}

Dictionary::Dictionary(MemoryManager& memoryManager, const bool shouldBeConcurrent) :
    m_nextResourceID(1),
    m_lexicalFormHandlesByResourceID(memoryManager),
    m_datatypeIDsByResourceID(memoryManager),
    m_dataPool(memoryManager),
    m_datatypes(),
    m_datatypesByIRIs()
{
    for (size_t datatypeID = 0; datatypeID <= 255; ++datatypeID)
        m_datatypesByIDs[datatypeID] = 0;
    std::vector<const DatatypeFactory*>& registeredDatatypeFactories = Dictionary::getRegisteredDatatypeFactories();
    std::unordered_map<Datatype*, DatatypeID> leastDatatypeIDByDatatype;
    for (std::vector<const DatatypeFactory*>::iterator factoryIterator = registeredDatatypeFactories.begin(); factoryIterator != registeredDatatypeFactories.end(); ++factoryIterator) {
        const DatatypeFactory* const datatypeFactory = *factoryIterator;
        std::unique_ptr<Datatype> datatype = datatypeFactory->createDatatype(memoryManager, m_nextResourceID, m_lexicalFormHandlesByResourceID, m_datatypeIDsByResourceID, m_dataPool, shouldBeConcurrent);
        const std::unordered_map<std::string, DatatypeID>& datatypeIDsByAllIRIs = datatypeFactory->getDatatypeIDsByAllIRIs();
        for (std::unordered_map<std::string, DatatypeID>::const_iterator iriIterator = datatypeIDsByAllIRIs.begin(); iriIterator != datatypeIDsByAllIRIs.end(); ++iriIterator)
            m_datatypesByIRIs[iriIterator->first] = datatype.get();
        const std::unordered_map<DatatypeID, std::string>& datatypeIRIsbyIDs = datatypeFactory->getDatatypeIRIsByID();
        DatatypeID leastDatatypeID = static_cast<DatatypeID>(-1);
        for (auto idIterator = datatypeIRIsbyIDs.begin(); idIterator != datatypeIRIsbyIDs.end(); ++idIterator) {
            m_datatypesByIDs[idIterator->first] = datatype.get();
            if (idIterator->first < leastDatatypeID)
                leastDatatypeID = idIterator->first;
        }
        leastDatatypeIDByDatatype[datatype.get()] = leastDatatypeID;
        bool added = false;
        for (unique_ptr_vector<Datatype>::iterator current = m_datatypes.begin(); current != m_datatypes.end(); ++current) {
            const DatatypeID currentLeastDatatypeID =leastDatatypeIDByDatatype[current->get()];
            if (leastDatatypeID < currentLeastDatatypeID) {
                m_datatypes.insert(current, std::move(datatype));
                added = true;
                break;
            }
        }
        if (!added)
            m_datatypes.push_back(std::move(datatype));
    }
}

Dictionary::~Dictionary() {
}

void Dictionary::initialize(const size_t initialResourceCapacity) {
    m_nextResourceID = 1;
    if (!m_lexicalFormHandlesByResourceID.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the lexical form handles array in the dictionary.");
    if (!m_datatypeIDsByResourceID.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the datatype IDs array in the dictionary.");
    if (initialResourceCapacity != 0) {
        if (!m_lexicalFormHandlesByResourceID.ensureEndAtLeast(initialResourceCapacity, 1) || !m_datatypeIDsByResourceID.ensureEndAtLeast(initialResourceCapacity, 1))
            throw RDF_STORE_EXCEPTION("Cannot reserve a sufficient amount of memory for the resources in the dictionary.");
    }
    m_dataPool.initialize();
    for (unique_ptr_vector<Datatype>::iterator iterator = m_datatypes.begin(); iterator != m_datatypes.end(); ++iterator)
        (*iterator)->initialize(initialResourceCapacity);
}

void Dictionary::setNumberOfThreads(const size_t numberOfThreads) {
    for (unique_ptr_vector<Datatype>::iterator iterator = m_datatypes.begin(); iterator != m_datatypes.end(); ++iterator)
        (*iterator)->setNumberOfThreads(numberOfThreads);
}

bool Dictionary::getResource(const ResourceID resourceID, ResourceValue& resourceValue) const {
    if (resourceID == INVALID_RESOURCE_ID) {
        resourceValue.setUndefined();
        return true;
    }
    else if (resourceID < ::atomicRead(m_nextResourceID)) {
        // First read datatypeID since a resource is considered entered into the dictionary when this value becomes different from zero.
        // The value can also be D_LOCK_ID, which means that this resource ID is currently being assigned.
        const DatatypeID datatypeID = ::atomicRead(m_datatypeIDsByResourceID[resourceID]);
        if (datatypeID == D_INVALID_DATATYPE_ID || datatypeID == D_LOCK_ID)
            return false;
        const LexicalFormHandle lexicalFormHandle = ::atomicRead(m_lexicalFormHandlesByResourceID[resourceID]);
        getDatatype(datatypeID).getResource(lexicalFormHandle, datatypeID, resourceValue);
        return true;
    }
    else
        return false;
}

bool Dictionary::getResource(const ResourceID resourceID, ResourceType& resourceType, std::string& lexicalForm, std::string& datatypeIRI) const {
    if (resourceID == INVALID_RESOURCE_ID) {
        resourceType = UNDEFINED_RESOURCE;
        lexicalForm.clear();
        datatypeIRI.clear();
        return true;
    }
    else if (resourceID < ::atomicRead(m_nextResourceID)) {
        // First read datatypeID since a resource is considered entered into the dictionary when this value becomes different from zero.
        // The value can also be D_LOCK_ID, which means that this resource ID is currently being assigned.
        const DatatypeID datatypeID = ::atomicRead(m_datatypeIDsByResourceID[resourceID]);
        if (datatypeID == D_INVALID_DATATYPE_ID || datatypeID == D_LOCK_ID)
            return false;
        const LexicalFormHandle lexicalFormHandle = ::atomicRead(m_lexicalFormHandlesByResourceID[resourceID]);
        switch (datatypeID) {
        case D_IRI_REFERENCE:
            getDatatype(datatypeID).getResource(lexicalFormHandle, datatypeID, lexicalForm);
            resourceType = IRI_REFERENCE;
            datatypeIRI.clear();
            break;
        case D_BLANK_NODE:
            getDatatype(datatypeID).getResource(lexicalFormHandle, datatypeID, lexicalForm);
            resourceType = BLANK_NODE;
            datatypeIRI.clear();
            break;
        default:
            getDatatype(datatypeID).getResource(lexicalFormHandle, datatypeID, lexicalForm, datatypeIRI);
            resourceType = LITERAL;
            break;
        }
        return true;
    }
    else
        return false;
}

bool Dictionary::getResource(const ResourceID resourceID, std::string& lexicalForm, DatatypeID& datatypeID) const {
    if (resourceID == INVALID_RESOURCE_ID) {
        lexicalForm.clear();
        datatypeID = D_INVALID_DATATYPE_ID;
        return true;
    }
    else if (resourceID < ::atomicRead(m_nextResourceID)) {
        const LexicalFormHandle lexicalFormHandle = ::atomicRead(m_lexicalFormHandlesByResourceID[resourceID]);
        datatypeID = ::atomicRead(m_datatypeIDsByResourceID[resourceID]);
        getDatatype(datatypeID).getResource(lexicalFormHandle, datatypeID, lexicalForm);
        return true;
    }
    else
        return false;
}

ResourceID Dictionary::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    if (resourceValue.getDatatypeID() == D_INVALID_DATATYPE_ID)
        return INVALID_RESOURCE_ID;
    else
        return getDatatype(resourceValue.getDatatypeID()).tryResolveResource(threadContext, resourceValue);
}

ResourceID Dictionary::tryResolveResource(ThreadContext& threadContext, const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    switch (resourceType) {
    case UNDEFINED_RESOURCE:
        return INVALID_RESOURCE_ID;
    case IRI_REFERENCE:
        return getDatatype(D_IRI_REFERENCE).tryResolveResource(threadContext, lexicalForm, D_IRI_REFERENCE);
    case BLANK_NODE:
        return getDatatype(D_BLANK_NODE).tryResolveResource(threadContext, lexicalForm, D_BLANK_NODE);
    case LITERAL:
        return getDatatype(datatypeIRI).tryResolveResource(threadContext, lexicalForm, datatypeIRI);
    default:
        throw RDF_STORE_EXCEPTION("Unknown resource type.");
    }
}

ResourceID Dictionary::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    if (datatypeID == D_INVALID_DATATYPE_ID)
        return INVALID_RESOURCE_ID;
    else
        return getDatatype(datatypeID).tryResolveResource(threadContext, lexicalForm, datatypeID);
}

ResourceID Dictionary::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    if (resourceValue.getDatatypeID() == D_INVALID_DATATYPE_ID)
        return INVALID_RESOURCE_ID;
    else
        return getDatatype(resourceValue.getDatatypeID()).resolveResource(threadContext, dictionaryUsageContext, resourceValue, preferredResourceID);
}

ResourceID Dictionary::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceType resourceType, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    switch (resourceType) {
    case UNDEFINED_RESOURCE:
        return INVALID_RESOURCE_ID;
    case IRI_REFERENCE:
        return getDatatype(D_IRI_REFERENCE).resolveResource(threadContext, dictionaryUsageContext, lexicalForm, D_IRI_REFERENCE, preferredResourceID);
    case BLANK_NODE:
        return getDatatype(D_BLANK_NODE).resolveResource(threadContext, dictionaryUsageContext, lexicalForm, D_BLANK_NODE, preferredResourceID);
    case LITERAL:
        return getDatatype(datatypeIRI).resolveResource(threadContext, dictionaryUsageContext, lexicalForm, datatypeIRI, preferredResourceID);
    default:
        throw RDF_STORE_EXCEPTION("Unknown resource type.");
    }
}

ResourceID Dictionary::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID  datatypeID, const ResourceID preferredResourceID) {
    if (datatypeID == D_INVALID_DATATYPE_ID)
        return INVALID_RESOURCE_ID;
    else
        return getDatatype(datatypeID).resolveResource(threadContext, dictionaryUsageContext, lexicalForm, datatypeID, preferredResourceID);
}

void Dictionary::save(OutputStream& outputStream) const {
    outputStream.writeString("Dictionary");
    outputStream.writeMemoryRegion(m_lexicalFormHandlesByResourceID);
    outputStream.writeMemoryRegion(m_datatypeIDsByResourceID);
    outputStream.write<ResourceID>(m_nextResourceID);
    m_dataPool.save(outputStream);
    for (unique_ptr_vector<Datatype>::const_iterator iterator = m_datatypes.begin(); iterator != m_datatypes.end(); ++iterator)
        (*iterator)->save(outputStream);
}

void Dictionary::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("Dictionary"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load Dictionary.");
    inputStream.readMemoryRegion(m_lexicalFormHandlesByResourceID);
    inputStream.readMemoryRegion(m_datatypeIDsByResourceID);
    m_nextResourceID = inputStream.read<ResourceID>();
    m_dataPool.load(inputStream);
    for (unique_ptr_vector<Datatype>::const_iterator iterator = m_datatypes.begin(); iterator != m_datatypes.end(); ++iterator)
        (*iterator)->load(inputStream);
}

std::unique_ptr<ComponentStatistics> Dictionary::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("Dictionary"));
    std::unique_ptr<ComponentStatistics> dataPoolStatistics = m_dataPool.getComponentStatistics();
    const uint64_t lexicalFormHandleTableSize = sizeof(LexicalFormHandle)* m_nextResourceID;
    uint64_t aggregateSize = lexicalFormHandleTableSize + dataPoolStatistics->getItemIntegerValue("Size");
    result->addSubcomponent(std::move(dataPoolStatistics));
    for (unique_ptr_vector<Datatype>::const_iterator iterator = m_datatypes.begin(); iterator != m_datatypes.end(); ++iterator) {
        std::unique_ptr<ComponentStatistics> datatypeStatistics = (*iterator)->getComponentStatistics();
        aggregateSize += datatypeStatistics->getItemIntegerValue("Aggregate size");
        result->addSubcomponent(std::move(datatypeStatistics));
    }
    result->addIntegerItem("Resource mapping size", lexicalFormHandleTableSize);
    result->addIntegerItem("Aggregate size", aggregateSize);
    result->addIntegerItem("Number of resources", m_nextResourceID - 1);
    return result;
}
