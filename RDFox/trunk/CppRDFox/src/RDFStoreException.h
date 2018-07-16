// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RDFSTOREEXCEPTION_H_
#define RDFSTOREEXCEPTION_H_

#include "all.h"

class RDFStoreException : public std::exception {

protected:

    const std::string m_fileName;
    const long m_lineNumber;
    const std::string m_message;
    const std::vector<std::exception_ptr> m_causes;
    const std::string m_what;

public:

    RDFStoreException(const std::string& fileName, const long lineNumber, const std::string& message, const std::vector<std::exception_ptr>& causes = std::vector<std::exception_ptr>());

    RDFStoreException(const RDFStoreException& other);

    virtual ~RDFStoreException();

    virtual const char* what() const noexcept;

    always_inline const std::string& getFileName() const {
        return m_fileName;
    }

    always_inline long getLineNumber() const {
        return m_lineNumber;
    }

    always_inline const std::string& getMessage() const {
        return m_message;
    }

    always_inline const std::vector<std::exception_ptr>& getCauses() const {
        return m_causes;
    }

};

#define RDF_STORE_EXCEPTION_WITH_CAUSES(message, causes)  \
    RDFStoreException(__FILE__, __LINE__, message, causes)

#define RDF_STORE_EXCEPTION(message)  \
    RDFStoreException(__FILE__, __LINE__, message)

#endif // RDFSTOREEXCEPTION_H_
