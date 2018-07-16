// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "RDFStoreException.h"

static void printException(std::ostream& output, const RDFStoreException& e, const size_t indent) {
    for (size_t index = 0; index < indent; ++index)
        output << " ";
    output << "RDFStoreException[file: " << e.getFileName() << "; line: " << e.getLineNumber() << "]: " << e.getMessage();
    const std::vector<std::exception_ptr>& causes = e.getCauses();
    for (auto iterator = causes.begin(); iterator != causes.end(); ++iterator) {
        output << std::endl;
        try {
            std::rethrow_exception(*iterator);
        }
        catch (const RDFStoreException& cause) {
            printException(output, cause, indent + 4);
        }
        catch (const std::exception& cause) {
            for (size_t index = 0; index < indent + 4; ++index)
                output << " ";
            output << "std::exception[" << typeid(cause).name() << "]: " << cause.what();
        }
        catch (...) {
            for (size_t index = 0; index < indent + 4; ++index)
                output << " ";
            output << "<unknown exception type>";
        }
    }
}

RDFStoreException::RDFStoreException(const std::string& fileName, const long lineNumber, const std::string& message, const std::vector<std::exception_ptr>& causes) :
    std::exception(),
    m_fileName(fileName),
    m_lineNumber(lineNumber),
    m_message(message),
    m_causes(causes),
    m_what()
{
    std::ostringstream output;
    printException(output, *this, 0);
    const_cast<std::string&>(m_what) = output.str();
}

RDFStoreException::RDFStoreException(const RDFStoreException& other) :
    std::exception(),
    m_fileName(other.m_fileName),
    m_lineNumber(other.m_lineNumber),
    m_message(other.m_message),
    m_causes(other.m_causes),
    m_what(other.m_what)
{
}

RDFStoreException::~RDFStoreException() {
}

const char* RDFStoreException::what() const noexcept {
    return m_what.c_str();
}
