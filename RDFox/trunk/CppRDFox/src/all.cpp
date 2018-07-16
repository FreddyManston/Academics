// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include <new>

#include "all.h"
#include "RDFStoreException.h"
#include "util/CodePoint.h"

#ifdef WIN32

    const std::string DIRECTORY_SEPARATOR("\\");

    LARGE_INTEGER s_queryPerformanceFrequency;
    static BOOL dummy = ::QueryPerformanceFrequency(&s_queryPerformanceFrequency);

    size_t getVMPageSize() {
        SYSTEM_INFO systemInfo;
        ::GetSystemInfo(&systemInfo);
        return systemInfo.dwPageSize;
    }

    size_t getAllocationGranularity() {
        SYSTEM_INFO systemInfo;
        ::GetSystemInfo(&systemInfo);
        return systemInfo.dwAllocationGranularity;
    }
    
    size_t getTotalPhysicalMemorySize() {
        MEMORYSTATUSEX memoryStatusEx;
        memoryStatusEx.dwLength = sizeof(MEMORYSTATUSEX);
        ::GlobalMemoryStatusEx(&memoryStatusEx);
        return memoryStatusEx.ullTotalPhys;
    }

    void getDirectoryFiles(const std::string& directoryName, std::vector<std::string>& listOfFiles) {
        listOfFiles.clear();
        std::string pattern(directoryName);
        if (directoryName[directoryName.size() - 1] != '\\' && directoryName[directoryName.size() - 1] != '/')
            pattern.push_back('/');
        pattern.push_back('*');
        WIN32_FIND_DATA foundFileData;
        HANDLE hFind = ::FindFirstFile(std::wstring(pattern.begin(), pattern.end()).c_str(), &foundFileData);
        if (hFind) {
            do {
                std::string directoryNameComplete(pattern.begin(), pattern.begin() + (pattern.size() - 1));
                char ch[260];
                char defChar = ' ';
                ::WideCharToMultiByte(CP_ACP, 0, foundFileData.cFileName, -1, ch, 260, &defChar, NULL);
                std::string fileName(ch);
                if (ch[0] != '.' && !(foundFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
                    listOfFiles.push_back(directoryNameComplete.append(fileName));
            }
            while (::FindNextFile(hFind, &foundFileData));
            ::FindClose(hFind);
        }
    }

    always_inline static std::wstring toAPI(const char* const text, const size_t length) {
        std::wstring result;
        const uint8_t* current = reinterpret_cast<const uint8_t*>(text);
        const uint8_t* afterLast = current + length;
        while (current < afterLast) {
            const CodePoint codePoint = ::getNextCodePoint(current, afterLast);
            if (codePoint != INVALID_CODE_POINT) {
                if (codePoint <= 0xFFFFu)
                    result.push_back(static_cast<wchar_t>(codePoint));
                else {
                    const CodePoint subtractedCodePoint = (codePoint - 0x010000u);
                    result.push_back(static_cast<wchar_t>((subtractedCodePoint >> 10) + 0xD800));
                    result.push_back(static_cast<wchar_t>((subtractedCodePoint && 0x3FFu) + 0xDC00));
                }
            }
        }
        return result;
    }

    std::wstring toAPI(const std::string& text) {
        return toAPI(text.c_str(), text.length());
    }

    std::wstring toAPI(const char* const text) {
        return toAPI(text, ::strlen(text));
    }

#else

    const std::string DIRECTORY_SEPARATOR("/");

    #include <dirent.h>

    size_t getVMPageSize() {
        return static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    }

    size_t getAllocationGranularity() {
        return static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    }

    #ifdef __APPLE__

        #include <sys/sysctl.h>

        size_t getTotalPhysicalMemorySize() {
            int mib[2];
            mib[0] = CTL_HW;
            mib[1] = HW_MEMSIZE;
            int64_t memorySize;
            size_t length = sizeof(memorySize);
            ::sysctl(mib, 2, &memorySize, &length, 0, 0);
            return static_cast<size_t>(memorySize);
        }

    #else

        size_t getTotalPhysicalMemorySize() {
            return static_cast<size_t>(::sysconf(_SC_PHYS_PAGES)) * static_cast<size_t>(::sysconf(_SC_PAGESIZE));
        }


        bool s_overcommitSupportEstablished = false;

        void doEnsureOvercommitSupported() {
            int file = ::open("/proc/sys/vm/overcommit_memory", O_RDONLY);
            if (file != -1) {
                uint8_t buffer[4096];
                size_t bytesRead = 0;
                ssize_t lastReadSize = 0;
                while (bytesRead < sizeof(buffer) && (lastReadSize = ::read(file, buffer + bytesRead, sizeof(buffer) - bytesRead)) > 0)
                    bytesRead += lastReadSize;
                ::close(file);
                if (lastReadSize == 0 && bytesRead >= 1 && buffer[0] != '0') {
                    std::ostringstream message;
                    message <<
                        "RDFox uses overcommit, which seems to be turned off on your system:\nthe value of the '/proc/sys/vm/overcommit_memory' kernel variable is '" << static_cast<char>(buffer[0]) << "'.\n" <<
                        "Please change the value of this this variable to '0'.\nYou will most likely require root access to do this.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
            }
            // If we fail at any point, we assume that overcommit is supported and hope for the best.
            ::atomicWrite(s_overcommitSupportEstablished, true);
        }


    #endif

    void getDirectoryFiles(const std::string& directoryName, std::vector<std::string>& listOfFiles) {
        listOfFiles.clear();
        DIR *directory = ::opendir(directoryName.c_str());
        if (directory) {
            struct dirent *entry;
            while ((entry = ::readdir(directory)) != 0) {
                if (entry->d_name[0] != '.') {
                    struct stat fileStatus;
                    ::stat(entry->d_name, &fileStatus);
                    if ((fileStatus.st_mode & S_IFMT) == S_IFREG) {
                        std::string fileName(directoryName);
                        listOfFiles.push_back(fileName.append(entry->d_name));
                    }
                }
            }
            ::closedir(directory);
        }
    }

#endif

bool isAbsoluteFileName(const std::string& fileName) {
    if (fileName.length() > 0 && (fileName[0] == '/' || fileName[0] == '\\'))
        return true;
    if (fileName.length() > 2 && ::isalpha(fileName[0]) && fileName[1] == ':')
        return true;
    return false;
}

std::string getCurrentDateTime() {
    time_t now;
    ::time(&now);
    struct tm time;
#ifdef WIN32
    ::localtime_s(&time, &now);
#else
    ::localtime_r(&now, &time);
#endif
    char buffer[80];
    ::strftime(buffer, sizeof(buffer), "%d-%b-%Y %X%p", &time);
    return std::string(buffer);
}

void* alignedNewThrowing(size_t size, size_t alignment) throw(std::bad_alloc) {
    if (size == 0)
        size = 1;
    while (true) {
        void* memoryBlock = ::_aligned_malloc(size, alignment);
        if (memoryBlock != 0)
            return memoryBlock;
        std::new_handler globalHandler = std::set_new_handler(0);
        std::set_new_handler(globalHandler);
        if (globalHandler)
            (*globalHandler)();
        else
            throw std::bad_alloc();
    }
}

void* alignedNewNonthrowing(size_t size, size_t alignment) throw() {
    if (size == 0)
        size = 1;
    return ::_aligned_malloc(size, alignment);
}

const char* getRDFoxVersion() {
#ifndef SVN_REVISION
    #define SVN_REVISION 0000
#endif
    return TO_STRING(SVN_REVISION);
}
