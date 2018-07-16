// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Datatype.h"
#include "Dictionary.h"

Datatype::Datatype(ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    m_nextResourceID(nextResourceID),
    m_lexicalFormHandlesByResourceID(lexicalFormHandlesByResourceID),
    m_datatypeIDsByResourceID(datatypeIDsByResourceID),
    m_dataPool(dataPool)
{
}

Datatype::~Datatype() {
}

DatatypeFactory::DatatypeFactory() {
}

DatatypeFactory::~DatatypeFactory() {
}

void DatatypeFactory::registerFactory() {
    Dictionary::registerDatatypeFactory(*this);
}

void DatatypeFactory::unregisterFactory() {
    Dictionary::unregisterDatatypeFactory(*this);
}
