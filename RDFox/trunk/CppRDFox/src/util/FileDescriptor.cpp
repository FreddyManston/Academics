#include "../all.h"
#include "../RDFStoreException.h"
#include "FileDescriptor.h"

#ifdef WIN32

static NORETURN always_inline void doReportError() {
    DWORD error = ::GetLastError();
    std::ostringstream message;
    message << "I/O error " << error;
    throw RDF_STORE_EXCEPTION(message.str());
}

#else

static NORETURN always_inline void doReportError() {
    int error = errno;
    std::ostringstream message;
    message << "I/O error " << error;
    throw RDF_STORE_EXCEPTION(message.str());
}

#endif

// FileDescriptor

FileDescriptor::~FileDescriptor() {
    close();
}

// FileDescriptorInputStream

void FileDescriptorInputStream::reportError() {
    ::doReportError();
}

// FileDescriptorOutputStream

void FileDescriptorOutputStream::reportError() {
    ::doReportError();
}
