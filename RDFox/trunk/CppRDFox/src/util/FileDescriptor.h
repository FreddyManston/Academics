// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef FILEDESCRIPTOR_H_
#define FILEDESCRIPTOR_H_

#include "../all.h"
#include "InputStream.h"
#include "OutputStream.h"

class FileDescriptorInputStream;
class FileDescriptorOutputStream;

#ifdef WIN32

    typedef HANDLE FileDescriptorType;
    static const FileDescriptorType INVALID_FILE_DESCRIPTOR = INVALID_HANDLE_VALUE;

#else

    typedef int FileDescriptorType;
    static const FileDescriptorType INVALID_FILE_DESCRIPTOR = -1;

#endif


// FileDescriptor;

class FileDescriptor : private Unmovable {

    friend class FileDescriptorInputStream;
    friend class FileDescriptorOutputStream;

public:

    typedef FileDescriptorInputStream InputStreamType;
    typedef FileDescriptorOutputStream OutputStreamType;

protected:

    FileDescriptorType m_fileDescriptor;

public:

    always_inline FileDescriptor() : m_fileDescriptor(INVALID_FILE_DESCRIPTOR) {
    }

    always_inline bool isOpen() const {
        return m_fileDescriptor != INVALID_FILE_DESCRIPTOR;
    }

    always_inline void close() {
        if (m_fileDescriptor != INVALID_FILE_DESCRIPTOR) {
#ifdef WIN32
            ::CloseHandle(m_fileDescriptor);
#else
            ::close(m_fileDescriptor);
#endif
            m_fileDescriptor = INVALID_FILE_DESCRIPTOR;
        }
    }

    virtual ~FileDescriptor() = 0;

};

// FileDescriptorInputStream

class FileDescriptorInputStream : public InputStream {

protected:

    FileDescriptorType m_fileDescriptor;

    NORETURN void reportError();

public:

    always_inline FileDescriptorInputStream() : m_fileDescriptor(INVALID_FILE_DESCRIPTOR) {
    }

    always_inline FileDescriptorInputStream(FileDescriptor& fileDescriptor) : m_fileDescriptor(fileDescriptor.m_fileDescriptor) {
    }

#ifdef WIN32

    always_inline FileDescriptorInputStream(const bool useInputStream) : m_fileDescriptor(useInputStream ? ::GetStdHandle(STD_INPUT_HANDLE) : INVALID_FILE_DESCRIPTOR) {
    }

    always_inline virtual void rewind() {
        LARGE_INTEGER zero;
        zero.LowPart = 0;
        zero.HighPart = 0;
        if (!::SetFilePointerEx(m_fileDescriptor, zero, NULL, FILE_BEGIN))
            reportError();
    }

    always_inline virtual size_t read(void* const data, const size_t numberOfBytesToRead) {
        DWORD bytesRead;
        if (!::ReadFile(m_fileDescriptor, data, numberOfBytesToRead < 0xffffffffULL ? static_cast<DWORD>(numberOfBytesToRead) : 0xffffffffUL, &bytesRead, 0))
            reportError();
        else
            return static_cast<size_t>(bytesRead);
    }

#else

    always_inline FileDescriptorInputStream(const bool useInputStream) : m_fileDescriptor(useInputStream ? STDIN_FILENO : INVALID_FILE_DESCRIPTOR) {
    }

    always_inline virtual void rewind() {
        if (::lseek(m_fileDescriptor, 0, SEEK_SET) == -1)
            reportError();
    }

    always_inline virtual size_t read(void* data, const size_t numberOfBytesToRead) {
        const ssize_t bytesRead = ::read(m_fileDescriptor, data, numberOfBytesToRead);
        if (bytesRead == -1)
            reportError();
        else
            return static_cast<size_t>(bytesRead);
    }

#endif

};

// FileDescriptorOutputStream

class FileDescriptorOutputStream : public OutputStream {

protected:

    FileDescriptorType m_fileDescriptor;

    NORETURN void reportError();

public:

    always_inline FileDescriptorOutputStream() : m_fileDescriptor(INVALID_FILE_DESCRIPTOR) {
    }

    always_inline FileDescriptorOutputStream(FileDescriptor& fileDescriptor) : m_fileDescriptor(fileDescriptor.m_fileDescriptor) {
    }

#ifdef WIN32

    always_inline FileDescriptorOutputStream(const bool useOutputStream) : m_fileDescriptor(useOutputStream ? ::GetStdHandle(STD_OUTPUT_HANDLE) : INVALID_FILE_DESCRIPTOR) {
    }

    always_inline virtual void flush() {
        if (!::FlushFileBuffers(m_fileDescriptor))
            reportError();
    }

    always_inline virtual size_t write(const void* const data, const size_t numberOfBytesToWrite) {
        DWORD bytesWritten;
        if (!::WriteFile(m_fileDescriptor, data, numberOfBytesToWrite < 0xffffffffULL ? static_cast<DWORD>(numberOfBytesToWrite) : 0xffffffffUL, &bytesWritten, 0))
            reportError();
        else
            return static_cast<size_t>(bytesWritten);
    }

#else

    always_inline FileDescriptorOutputStream(const bool useOutputStream) : m_fileDescriptor(useOutputStream ? STDOUT_FILENO : INVALID_FILE_DESCRIPTOR) {
    }

    always_inline virtual void flush() {
        if (::fsync(m_fileDescriptor) == -1)
            reportError();
    }

    always_inline virtual size_t write(const void* const data, const size_t numberOfBytesToWrite) {
        const ssize_t bytesWritten = ::write(m_fileDescriptor, data, numberOfBytesToWrite);
        if (bytesWritten == -1)
            reportError();
        else
            return static_cast<size_t>(bytesWritten);
    }

#endif

};

#endif /* FILEDESCRIPTOR_H_ */
