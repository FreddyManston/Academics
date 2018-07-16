// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "File.h"

#ifdef WIN32

    void File::reportError() {
        DWORD error = ::GetLastError();
        std::ostringstream message;
        message << "I/O error " << error;
        throw RDF_STORE_EXCEPTION(message.str());
    }

    static const char* s_commandsForExtensions[][3] = {
        { nullptr, nullptr, nullptr }
    };

    static HANDLE openRegularFile(const char* const fileName, const File::OpenMode openMode, const bool read, const bool write, const bool sequentialAccess, const bool deleteOnClose) {
        DWORD dwDesiredAccess = 0;
        if (read)
            dwDesiredAccess |= GENERIC_READ;
        if (write)
            dwDesiredAccess |= GENERIC_WRITE;
        if (dwDesiredAccess == 0)
            throw RDF_STORE_EXCEPTION("Either read or write mode should be specified.");
        // The OpenMode enumeration is deliberately by one off from the corresponding codes in Windows
        HANDLE fileHandle = ::CreateFile(::toAPI(fileName).c_str(), dwDesiredAccess, FILE_SHARE_READ, NULL, openMode + 1, (sequentialAccess ? FILE_FLAG_SEQUENTIAL_SCAN : FILE_ATTRIBUTE_NORMAL) | (deleteOnClose ? FILE_FLAG_DELETE_ON_CLOSE : 0), NULL);
        if (fileHandle == INVALID_HANDLE_VALUE) {
            DWORD error = ::GetLastError();
            std::ostringstream message;
            message << "Cannot open file '" << fileName << "'; error " << error;
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return fileHandle;
    }

    static HANDLE openProcessPipedFile(const bool read, char* const processArguments[], const char* const outputFileName) {
        throw RDF_STORE_EXCEPTION("Not supported yet.");
    }

#else

    #include "Thread.h"

    void File::reportError() {
        int error = errno;
        std::ostringstream message;
        message << "I/O error " << error;
        throw RDF_STORE_EXCEPTION(message.str());
    }

    static int openRegularFile(const char* const fileName, const File::OpenMode openMode, const bool read, const bool write, const bool sequentialAccess, const bool deleteOnClose) {
        int flags = O_CLOEXEC;
        if (read && write)
            flags |= O_RDWR;
        else if (read)
            flags |= O_RDONLY;
        else if (write)
            flags |= O_WRONLY;
        else if (!read && !write)
            throw RDF_STORE_EXCEPTION("Either read or write mode should be specified.");
        switch (openMode) {
        case File::CREATE_NEW_FILE_IF_NOT_EXISTS:
            flags |= O_CREAT | O_EXCL;
            break;
        case File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE:
            flags |= O_CREAT | O_TRUNC;
            break;
        case File::OPEN_EXISTING_FILE:
            break;
        case File::OPEN_OR_CREATE_NEW_FILE:
            flags |= O_CREAT;
            break;
        case File::TRUNCATE_EXISTING_FILE:
            flags |= O_TRUNC;
            break;
        }
        const int fileDescriptor = ::open(fileName, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
        if (fileDescriptor == -1) {
            int error = errno;
            std::ostringstream message;
            message << "Cannot open file '" << fileName << "'; error " << error;
            throw RDF_STORE_EXCEPTION(message.str());
        }
        if (deleteOnClose) {
            if (::unlink(fileName) == -1) {
                int error = errno;
                ::close(fileDescriptor);
                std::ostringstream message;
                message << "Cannot ensure that file '" << fileName << "' is deleted once it is closed; error " << error;
                throw RDF_STORE_EXCEPTION(message.str());
            }
        }
        return fileDescriptor;
    }

    class ChildProcessReaper : public Thread {

    protected:

        const pid_t m_childProcessID;

    public:

        ChildProcessReaper(const pid_t childProcessID) : Thread(true), m_childProcessID(childProcessID) {
        }

        virtual void run() {
            int statusIgnored;
            ::waitpid(m_childProcessID, &statusIgnored, 0);
        }

    };

    static int openProcessPipedFile(const bool read, char* const processArguments[], const char* const fileName) {
        int fileDescriptors[2];
        if (::pipe(fileDescriptors) != 0)
            throw RDF_STORE_EXCEPTION("Cannot create a pipe.");
        posix_spawn_file_actions_t fileActions;
        ::posix_spawn_file_actions_init(&fileActions);
        if (read) {
            ::posix_spawn_file_actions_addclose(&fileActions, STDOUT_FILENO);
            ::posix_spawn_file_actions_adddup2(&fileActions, fileDescriptors[1], STDOUT_FILENO);
            ::posix_spawn_file_actions_addclose(&fileActions, fileDescriptors[0]);
            // Open the file if needed and redirect it to input
            if (fileName != nullptr) {
                ::posix_spawn_file_actions_addclose(&fileActions, STDIN_FILENO);
                ::posix_spawn_file_actions_addopen(&fileActions, STDIN_FILENO, fileName, O_RDONLY, 0);
            }
        }
        else {
            ::posix_spawn_file_actions_addclose(&fileActions, STDIN_FILENO);
            ::posix_spawn_file_actions_adddup2(&fileActions, fileDescriptors[0], STDIN_FILENO);
            ::posix_spawn_file_actions_addclose(&fileActions, fileDescriptors[1]);
            // Open the file if needed and redirect output to it
            if (fileName != nullptr) {
                ::posix_spawn_file_actions_addclose(&fileActions, STDOUT_FILENO);
                ::posix_spawn_file_actions_addopen(&fileActions, STDOUT_FILENO, fileName, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
            }
        }
        pid_t childProcessID;
        const int result = ::posix_spawnp(&childProcessID, processArguments[0], &fileActions, nullptr,processArguments, nullptr);
        ::posix_spawn_file_actions_destroy(&fileActions);
        if (result != 0) {
            ::close(fileDescriptors[0]);
            ::close(fileDescriptors[1]);
            std::ostringstream message;
            message << "An error occurred while spawining a process: " << result;
            throw RDF_STORE_EXCEPTION(message.str());
        }
        // The thread object is autoCleanup, so it will be deleted when the thread finishes.
        (new ChildProcessReaper(childProcessID))->start();
        ::close(fileDescriptors[read ? 1 : 0]);
        return fileDescriptors[read ? 0 : 1];
    }

    static const char* s_commandsForExtensions[][3] = {
        { "zip", "unzip -p $", "zip -q $ -" },
        { "gz", "gunzip", "gzip" },
        { "bz2", "pbunzip2 -m2000", "pbzip2 -m2000" },
        { "bz2", "bunzip2", "bzip2" },
        { "Z", "uncompress", "compress" },
        { nullptr, nullptr, nullptr }
    };

#endif

static size_t getIndexForExtension(const char* const extension) {
    size_t extensionIndex = 0;
    while (s_commandsForExtensions[extensionIndex][0] != nullptr && ::strcmp(extension, s_commandsForExtensions[extensionIndex][0]) != 0)
        ++extensionIndex;
    return extensionIndex;
}

void File::open(const char* const fileType, const char* const fileName, const OpenMode openMode, const bool read, const bool write, const bool sequentialAccess, const bool deleteOnClose) {
    close();
    if (fileType == nullptr)
        m_fileDescriptor = openRegularFile(fileName, openMode, read, write, sequentialAccess, deleteOnClose);
    else if (deleteOnClose)
        throw RDF_STORE_EXCEPTION("Piped files cannot be opened with 'deleteOnClose' set.");
    else {
        switch (openMode) {
        case CREATE_NEW_FILE_IF_NOT_EXISTS:
            if (read)
                throw RDF_STORE_EXCEPTION("Piped files cannot be opened with truncation for reading.");
            if (::fileExists(fileName))
                throw RDF_STORE_EXCEPTION("Piped file already exists.");
            break;
        case CREATE_NEW_OR_TRUNCATE_EXISTING_FILE:
            if (read)
                throw RDF_STORE_EXCEPTION("Piped files cannot be opened with truncation for reading.");
			::deleteFile(fileName);
            break;
        case OPEN_EXISTING_FILE:
            if (write)
                throw RDF_STORE_EXCEPTION("Piped files cannot be opened for wiritng without truncating them.");
            break;
        case OPEN_OR_CREATE_NEW_FILE:
            throw RDF_STORE_EXCEPTION("Open or create new mode is not supproted for piped files.");
        case TRUNCATE_EXISTING_FILE:
            if (read)
                throw RDF_STORE_EXCEPTION("Piped files cannot be opened with truncation for reading.");
            if (!::fileExists(fileName))
                throw RDF_STORE_EXCEPTION("Piped file does not exist.");
            ::deleteFile(fileName);
            break;
        }
        size_t extensionIndex = 0;
        bool atLeastOneTypeHandlerFound = false;
        std::vector<std::exception_ptr> causes;
        while (m_fileDescriptor == INVALID_FILE_DESCRIPTOR && s_commandsForExtensions[extensionIndex][0] != nullptr) {
            if (::strcmp(fileType, s_commandsForExtensions[extensionIndex][0]) == 0) {
                atLeastOneTypeHandlerFound = true;
                const char* command = s_commandsForExtensions[extensionIndex][read ? 1 : 2];
                const char* tokenStart = command;
                std::vector<std::string> arguments;
                bool fileNameInCommand = false;
                while (true) {
                    if (*command == ' ' || *command == 0) {
                        if (command > tokenStart) {
                            if (command == tokenStart + 1 && *tokenStart == '$') {
                                arguments.push_back(fileName);
                                fileNameInCommand = true;
                            }
                            else
                                arguments.emplace_back(tokenStart, command - tokenStart);
                        }
                        while (*command == ' ')
                            ++command;
                        if (*command == 0)
                            break;
                        tokenStart = command;
                    }
                    else
                        ++command;
                }
                std::unique_ptr<char*[]> argumentsArray(new char*[arguments.size() + 1]);
                for (size_t index = 0; index < arguments.size(); ++index)
                    argumentsArray[index] = const_cast<char*>(arguments[index].c_str());
                argumentsArray[arguments.size()] = nullptr;
                try {
                    m_fileDescriptor = openProcessPipedFile(read, argumentsArray.get(), fileNameInCommand ? nullptr : fileName);
                }
                catch (const RDFStoreException&) {
                    causes.push_back(std::current_exception());
                }
            }
            ++extensionIndex;
        }
        if (m_fileDescriptor == INVALID_FILE_DESCRIPTOR) {
            if (!atLeastOneTypeHandlerFound) {
                std::ostringstream message;
                message << "File type '" << fileType << "' is unknown.";
                throw RDF_STORE_EXCEPTION(message.str());
            }
            else {
                std::ostringstream message;
                message << "None of the handlers could open a file of type '" << fileType << "'.";
                throw RDF_STORE_EXCEPTION_WITH_CAUSES(message.str(), causes);
            }
        }
    }
    m_readable = read;
    m_writable = write;
    m_sequentialAccess = sequentialAccess;
}

static const char* getFileNameExtension(const char* const fileName) {
    for (const char* extension = fileName + ::strlen(fileName); extension >= fileName; --extension) {
        if (*extension == '/')
            return nullptr;
        else if (*extension == '.')
            return extension + 1;
    }
    return nullptr;
}

void File::open(const std::string& fileName, const OpenMode openMode, const bool read, const bool write, const bool sequentialAccess, const bool deleteOnClose) {
    const char* fileType = nullptr;
    if (read != write) {
        const char* const extension = getFileNameExtension(fileName.c_str());
        if (extension != nullptr) {
            const size_t extensionIndex = getIndexForExtension(extension);
            fileType = s_commandsForExtensions[extensionIndex][0];
        }
    }
    open(fileType, fileName.c_str(), openMode, read, write, sequentialAccess, deleteOnClose);
}

bool File::isKnownFileType(const char* const fileType) {
    const size_t extensionIndex = getIndexForExtension(fileType);
    return s_commandsForExtensions[extensionIndex][0] != nullptr;
}
