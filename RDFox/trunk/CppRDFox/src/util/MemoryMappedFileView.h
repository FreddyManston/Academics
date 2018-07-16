// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MEMORYMAPPEDFILEVIEW_H_
#define MEMORYMAPPEDFILEVIEW_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "File.h"

class MemoryMappedFileView : private Unmovable {

protected:

    File* m_file;
    size_t m_fileSize;
#ifdef WIN32
    HANDLE m_fileMapping;
    DWORD m_protection;
#else
    int m_protection;
#endif
    void* m_mappedData;
    void* m_afterMappedData;
    size_t m_mappedDataSize;

public:

#ifdef WIN32

    always_inline MemoryMappedFileView() : m_file(0), m_fileMapping(INVALID_HANDLE_VALUE), m_protection(0), m_mappedData(0), m_afterMappedData(0), m_mappedDataSize(0) {
    }

    void open(File& file) {
        close();
        m_file = &file;
        m_fileSize = m_file->getSize();
        DWORD flProtect = 0;
        if (m_file->m_readable & m_file->m_writable)
            flProtect = PAGE_READWRITE;
        else if (m_file->m_readable)
            flProtect = PAGE_READONLY;
        else if (m_file->m_writable)
            throw RDF_STORE_EXCEPTION("Windows does not support write-only memory mapped files.");
         m_protection = (m_file->m_readable ? FILE_MAP_READ : 0) | (m_file->m_writable ? FILE_MAP_WRITE : 0);
        if (m_file->getSize() > 0) {
			m_fileMapping = ::CreateFileMapping(m_file->m_fileDescriptor, NULL, flProtect, 0, 0, NULL);
            if (m_fileMapping == INVALID_HANDLE_VALUE) {
                DWORD error = ::GetLastError();
                close();
                std::ostringstream message;
                message << "Cannot create file mapping; error " << error;
                throw RDF_STORE_EXCEPTION(message.str());
            }
        }
    }

    void close() {
        unmapView();
        if (m_fileMapping != INVALID_HANDLE_VALUE) {
            ::CloseHandle(m_fileMapping);
            m_fileMapping = INVALID_HANDLE_VALUE;
        }
    }

    void mapView(const size_t offset, const size_t length) {
        assert(offset + length <= m_fileSize);
        assert(length > 0);
        unmapView();
        m_mappedData = ::MapViewOfFileEx(m_fileMapping, m_protection, static_cast<DWORD>(offset >> 32), static_cast<DWORD>(offset), length, NULL);
        if (m_mappedData == 0) {
            DWORD error = ::GetLastError();
            std::ostringstream message;
            message << "Cannot map view of a file; error " << error;
            throw RDF_STORE_EXCEPTION(message.str());
        }
        m_afterMappedData = reinterpret_cast<uint8_t*>(m_mappedData) + length;
        m_mappedDataSize = length;
    }

    void unmapView() {
        if (m_mappedData != 0) {
            ::UnmapViewOfFile(m_mappedData);
            m_mappedData = 0;
            m_afterMappedData = 0;
            m_mappedDataSize = 0;
        }
    }

#else

    always_inline MemoryMappedFileView() : m_file(0), m_protection(0), m_mappedData(0), m_afterMappedData(0), m_mappedDataSize(0) {
    }

    void open(File& file) {
        close();
        m_file = &file;
        m_fileSize = m_file->getSize();
        m_protection = (m_file->m_readable ? PROT_READ : 0) | (m_file->m_writable ? PROT_WRITE : 0);
    }

    void close() {
        unmapView();
    }

    void mapView(const size_t offset, const size_t length) {
        assert(offset + length <= m_fileSize);
        assert(length > 0);
        unmapView();
        m_mappedData = ::mmap(0, length, m_protection, MAP_SHARED, m_file->m_fileDescriptor, offset);
        if (m_mappedData == MAP_FAILED) {
            m_mappedData = 0;
            int error = errno;
            std::ostringstream message;
            message << "Cannot map view of a file; error " << error;
            throw RDF_STORE_EXCEPTION(message.str());
        }
        m_afterMappedData = reinterpret_cast<uint8_t*>(m_mappedData) + length;
        m_mappedDataSize = length;
        if (m_file->m_sequentialAccess && ::madvise(reinterpret_cast<caddr_t>(m_mappedData), m_mappedDataSize, MADV_SEQUENTIAL) != 0) {
            int error = errno;
            std::ostringstream message;
            message << "Cannot set up sequential access for a file; error " << error;
            throw RDF_STORE_EXCEPTION(message.str());
        }
    }

    void unmapView() {
        if (m_mappedData != 0) {
            ::munmap(m_mappedData, m_mappedDataSize);
            m_mappedData = 0;
            m_afterMappedData = 0;
            m_mappedDataSize = 0;
        }
    }

#endif

    ~MemoryMappedFileView() {
        close();
    }

    always_inline File& getFile() const {
        return *m_file;
    }

    always_inline bool isMapped() const {
        return m_mappedData != 0;
    }

    always_inline void* getMappedData() const {
        return m_mappedData;
    }

    always_inline void* getAfterMappedData() const {
        return m_afterMappedData;
    }

    always_inline size_t getMappedDataSize() const {
        return m_mappedDataSize;
    }

};

#endif /* MEMORYMAPPEDFILEVIEW_H_ */
