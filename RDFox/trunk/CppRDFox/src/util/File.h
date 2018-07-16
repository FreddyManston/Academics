// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef FILE_H_
#define FILE_H_

#include "FileDescriptor.h"

class File : public FileDescriptor {

    friend class MemoryMappedFileView;

protected:

    bool m_readable;
    bool m_writable;
    bool m_sequentialAccess;

    void reportError();

public:

    enum OpenMode { CREATE_NEW_FILE_IF_NOT_EXISTS, CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, OPEN_EXISTING_FILE, OPEN_OR_CREATE_NEW_FILE, TRUNCATE_EXISTING_FILE };

#ifdef WIN32

    always_inline void rewind() {
        LARGE_INTEGER zero;
        zero.LowPart = 0;
        zero.HighPart = 0;
        if (!::SetFilePointerEx(m_fileDescriptor, zero, NULL, FILE_BEGIN))
            reportError();
    }

    always_inline size_t getSize() {
        LARGE_INTEGER fileSize;
		if (!::GetFileSizeEx(m_fileDescriptor, &fileSize))
            reportError();
        return fileSize.QuadPart;
    }

    always_inline bool isRegularFile() {
		const DWORD fileType = ::GetFileType(m_fileDescriptor);
        if (fileType == FILE_TYPE_UNKNOWN && ::GetLastError() != NO_ERROR)
            reportError();
        return (fileType == FILE_TYPE_DISK);
    }

#else

    always_inline void rewind() {
        if (::lseek(m_fileDescriptor, 0, SEEK_SET) == -1)
            reportError();
    }

    size_t getSize() {
        struct stat buffer;
        if (::fstat(m_fileDescriptor, &buffer) == -1)
            reportError();
        return buffer.st_size;
    }

    bool isRegularFile() {
        struct stat buffer;
        if (::fstat(m_fileDescriptor, &buffer) == -1)
            reportError();
        return S_ISREG(buffer.st_mode);
    }

#endif

    void open(const char* const fileType, const char* const fileName, const OpenMode openMode, const bool read, const bool write, const bool sequentialAccess, const bool deleteOnClose = false);

    void open(const std::string& fileName, const OpenMode openMode, const bool read, const bool write, const bool sequentialAccess, const bool deleteOnClose = false);

    static bool isKnownFileType(const char* const fileType);

};

#endif /* FILE_H_ */
