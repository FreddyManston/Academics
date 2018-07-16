// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SOCKET_H_
#define SOCKET_H_

#include "../all.h"
#include "../RDFStoreException.h"

class SocketSelector;

#ifdef WIN32

	typedef SOCKET SocketType;
	static const SocketType INVALID_SOCKET_HANDLE = INVALID_SOCKET;

#else

	typedef int SocketType;
	static const SocketType INVALID_SOCKET_HANDLE = -1;

#endif

// SocketAddress

class SocketAddress {

    friend class Socket;

protected:

    addrinfo* m_firstAddress;
    addrinfo* m_currentAddress;

public:

    enum ProtocolType { IP_V6_ONLY, IP_V4_ONLY, IP_ANY, IP_V6_TRY_FIRST };

    SocketAddress() : m_firstAddress(nullptr), m_currentAddress(nullptr) {
    }

    SocketAddress(const SocketAddress& other) = delete;

    SocketAddress& operator=(const SocketAddress& other) = delete;

    SocketAddress(SocketAddress&& other) : m_firstAddress(other.m_firstAddress), m_currentAddress(other.m_currentAddress) {
        other.m_firstAddress = nullptr;
        other.m_currentAddress = nullptr;
    }

    SocketAddress& operator=(SocketAddress&& other) {
        if (this != &other) {
            if (m_firstAddress != nullptr)
                ::freeaddrinfo(m_firstAddress);
            m_firstAddress = other.m_firstAddress;
            m_currentAddress = other.m_currentAddress;
            other.m_firstAddress = nullptr;
            other.m_currentAddress = nullptr;
        }
        return *this;
    }

    always_inline ~SocketAddress() {
        close();
    }

    void open(const ProtocolType protocolType, const bool passive, const char* const nodeName, const char* const serviceName);

    void close();

    always_inline bool isValid() const {
        return m_currentAddress != nullptr;
    }

    always_inline bool isIPv6() const {
        return m_currentAddress->ai_family == AF_INET6;
    }

    always_inline void nextAddress() {
        m_currentAddress = m_currentAddress->ai_next;
    }

    always_inline void resetToFirstAddress() {
        m_currentAddress = m_firstAddress;
    }

    always_inline uint32_t getNumberOfAddresses() const {
        uint32_t numberOfAddresses = 0;
        for (addrinfo* current = m_firstAddress; current != nullptr; current = current->ai_next)
            ++numberOfAddresses;
        return numberOfAddresses;
    }

};

// Socket

class Socket {

    friend class SocketSelector;

protected:

    SocketType m_socket;

    NORETURN void reportError(const char* const message);

public:

    always_inline Socket() : m_socket(INVALID_SOCKET_HANDLE) {
    }

    ~Socket();

    Socket(const Socket& other) = delete;

    always_inline Socket(Socket&& other) : m_socket(other.m_socket) {
        other.m_socket = INVALID_SOCKET_HANDLE;
    }

    Socket& operator=(const Socket& other) = delete;

    always_inline Socket& operator=(Socket&& other) {
        if (this != &other) {
            close();
            m_socket = other.m_socket;
            other.m_socket = INVALID_SOCKET_HANDLE;
        }
        return *this;
    }

    void create(const SocketAddress& socketAddress);

    bool connect(const SocketAddress& socketAddress);

    void connectLocally(Socket& other);

    void listen(const SocketAddress& socketAddress, const size_t backlog);

    void accept(Socket& listeningSocket);

#ifdef WIN32

    enum ShutdownType { READ = SD_RECEIVE, WRITE = SD_SEND, READWRITE = SD_BOTH };

    always_inline virtual size_t read(void* const data, const size_t numberOfBytesToRead) {
        const int bytesRead = ::recv(m_socket, reinterpret_cast<char*>(data), static_cast<int>(numberOfBytesToRead), 0);
        if (bytesRead == -1)
            reportError("Error reading from a socket");
        return static_cast<size_t>(bytesRead);
    }

    always_inline virtual size_t read(void* const data, const size_t numberOfBytesToRead, bool& wouldBlock) {
        const int bytesRead = ::recv(m_socket, reinterpret_cast<char*>(data), static_cast<int>(numberOfBytesToRead), 0);
        if (bytesRead == -1) {
            wouldBlock = (::WSAGetLastError() == WSAEWOULDBLOCK);
            if (wouldBlock)
                return 0;
            else
                reportError("Error reading from a socket");
        }
        else {
            wouldBlock = false;
            return static_cast<size_t>(bytesRead);
        }
    }

    always_inline virtual size_t write(const void* const data, const size_t numberOfBytesToWrite) {
        const int bytesWritten = ::send(m_socket, reinterpret_cast<const char*>(data), static_cast<int>(numberOfBytesToWrite), 0);
        if (bytesWritten == -1)
            reportError("Error writing to a socket");
        return static_cast<size_t>(bytesWritten);
    }

    always_inline virtual size_t write(const void* const data, const size_t numberOfBytesToWrite, bool& wouldBlock) {
        const int bytesWritten = ::send(m_socket, reinterpret_cast<const char*>(data), static_cast<int>(numberOfBytesToWrite), 0);
        if (bytesWritten == -1) {
            wouldBlock = (::WSAGetLastError() == WSAEWOULDBLOCK);
            if (wouldBlock)
                return 0;
            else
                reportError("Error writing to a socket");
        }
        else {
            wouldBlock = false;
            return static_cast<size_t>(bytesWritten);
        }
    }

    always_inline void makeNonBlocking(const bool value) {
        u_long mode = (value ? 0 : 1);
        if (::ioctlsocket(m_socket, FIONBIO, &mode) != 0)
            reportError("Cannot set nonblocking option on a socket");
    }

    always_inline void close() {
        if (m_socket != INVALID_SOCKET_HANDLE) {
            ::closesocket(m_socket);
            m_socket = INVALID_SOCKET_HANDLE;
        }
    }

#else

    enum ShutdownType { READ = SHUT_RD, WRITE = SHUT_WR, READWRITE = SHUT_RDWR };

    always_inline virtual size_t read(void* const data, const size_t numberOfBytesToRead) {
        const ssize_t bytesRead = ::recv(m_socket, data, numberOfBytesToRead, 0);
        if (bytesRead == -1)
            reportError("Error reading from a socket");
        return static_cast<size_t>(bytesRead);
    }

    always_inline virtual size_t read(void* const data, const size_t numberOfBytesToRead, bool& wouldBlock) {
        const ssize_t bytesRead = ::recv(m_socket, data, numberOfBytesToRead, 0);
        if (bytesRead == -1) {
            wouldBlock = (errno == EWOULDBLOCK);
            if (wouldBlock)
                return 0;
            else
                reportError("Error reading from a socket");
        }
        else {
            wouldBlock = false;
            return static_cast<size_t>(bytesRead);
        }
    }

    always_inline virtual size_t write(const void* const data, const size_t numberOfBytesToWrite) {
        const ssize_t bytesWritten = ::send(m_socket, data, numberOfBytesToWrite, MSG_NOSIGNAL);
        if (bytesWritten == -1)
            reportError("Error writing to a socket");
        return static_cast<size_t>(bytesWritten);
    }

    always_inline virtual size_t write(const void* const data, const size_t numberOfBytesToWrite, bool& wouldBlock) {
        const ssize_t bytesWritten = ::send(m_socket, data, numberOfBytesToWrite, MSG_NOSIGNAL);
        if (bytesWritten == -1) {
            wouldBlock = (errno == EWOULDBLOCK);
            if (wouldBlock)
                return 0;
            else
                reportError("Error writing to a socket");
        }
        else {
            wouldBlock = false;
            return static_cast<size_t>(bytesWritten);
        }
    }

    always_inline void makeNonBlocking(const bool value) {
        int flags = ::fcntl(m_socket, F_GETFL, 0);
        if (flags == -1 || ::fcntl(m_socket, F_SETFL, flags | O_NONBLOCK) == -1)
            reportError("Cannot set nonblocking option on a socket");
    }

    always_inline void close() {
        if (m_socket != INVALID_SOCKET_HANDLE) {
            ::close(m_socket);
            m_socket = INVALID_SOCKET_HANDLE;
        }
    }

#endif

    always_inline void ensureNoError() {
        int value;
#ifdef WIN32
        int valueLength = sizeof(int);
#else
        socklen_t valueLength = sizeof(int);
#endif
        if (::getsockopt(m_socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&value), &valueLength) != 0)
            reportError("Cannot read socket error");
        if (value != 0) {
            std::ostringstream message;
            message << "Socket error: " << value;
            throw RDF_STORE_EXCEPTION(message.str());
        }
    }

    always_inline void setSendBufferSize(const uint16_t sendBufferSize) {
        int optionValue = static_cast<int>(sendBufferSize);
        if (::setsockopt(m_socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&optionValue), sizeof(optionValue)) != 0)
            reportError("Cannot change the size of the send buffer");
    }

    always_inline void setReceiveBufferSize(const uint16_t receiveBufferSize) {
        int optionValue = static_cast<int>(receiveBufferSize);
        if (::setsockopt(m_socket, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&optionValue), sizeof(optionValue)) != 0)
            reportError("Cannot change the size of the receive buffer");
    }

    always_inline void setReuseAddress(const bool value) {
        int optionValue = static_cast<int>(value);
        if (::setsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&optionValue), sizeof(optionValue)) != 0)
            reportError("Cannot change reuse-address option");
    }

    always_inline void setDualStack(const bool value) {
        int optionValue = static_cast<int>(!value);
        if (::setsockopt(m_socket, IPPROTO_IPV6, IPV6_V6ONLY, reinterpret_cast<char*>(&optionValue), sizeof(optionValue)) != 0)
            reportError("Cannot change the IPv6-only option");
    }

    always_inline void setKeepAlive(const bool value) {
        int optionValue = static_cast<int>(value);
        if (::setsockopt(m_socket, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<char*>(&optionValue), sizeof(optionValue)) != 0)
            reportError("Cannot change keep-alive option");
    }

    always_inline void setTCPNoDelay(const bool value) {
        int optionValue = static_cast<int>(value);
        if (::setsockopt(m_socket, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&optionValue), sizeof(optionValue)) != 0)
            reportError("Cannot change no-delay option");
    }

    always_inline bool isValid() const {
        return m_socket != INVALID_SOCKET_HANDLE;
    }

    always_inline void shutdown(const ShutdownType shutdownType) {
        if (::shutdown(m_socket, static_cast<int>(shutdownType)) != 0)
            reportError("Cannot shutdown the scoket");
    }

};

// SocketSelector

class SocketSelector : private Unmovable {

protected:

    SocketType m_interrupt[2];
    fd_set m_readSet;
    fd_set m_writeSet;
    fd_set m_exceptionSet;
#ifndef WIN32
    int m_maxSocket;
#endif

    NORETURN void reportError(const char* const message);

public:

    enum SelectResult { SOCKET_READY, INTERRUPTED, TIMEOUT };

    always_inline SocketSelector() {
        m_interrupt[0] = m_interrupt[1] = INVALID_SOCKET_HANDLE;
        clear();
    }

    ~SocketSelector();

    void enableInterrupt();

    void disableInterrupt();

    always_inline bool isInterruptEnabled() const {
        return m_interrupt[0] != INVALID_SOCKET_HANDLE && m_interrupt[1] != INVALID_SOCKET_HANDLE;
    }

    always_inline void clear() {
        FD_ZERO(&m_readSet);
        FD_ZERO(&m_writeSet);
        FD_ZERO(&m_exceptionSet);
#ifdef WIN32
        if (m_interrupt[0] != INVALID_SOCKET_HANDLE) {
            FD_SET(m_interrupt[0], &m_readSet);
        }
#else
        if (m_interrupt[0] == INVALID_SOCKET_HANDLE)
            m_maxSocket = 0;
        else {
            FD_SET(m_interrupt[0], &m_readSet);
            m_maxSocket = m_interrupt[0];
        }
#endif
    }

    always_inline void monitor(const Socket& socket, const bool read, const bool write, const bool exception) {
        if (read) {
            FD_SET(socket.m_socket, &m_readSet);
        }
        if (write) {
            FD_SET(socket.m_socket, &m_writeSet);
        }
        if (exception) {
            FD_SET(socket.m_socket, &m_exceptionSet);
        }
#ifndef WIN32
        if ((read || write || exception) && socket.m_socket > m_maxSocket)
            m_maxSocket = socket.m_socket;
#endif
    }

    always_inline bool canRead(const Socket& socket) const {
        return FD_ISSET(socket.m_socket, &m_readSet) != 0;
    }

    always_inline bool canWrite(const Socket& socket) const {
        return FD_ISSET(socket.m_socket, &m_writeSet) != 0;
    }

    always_inline bool hasException(const Socket& socket) const {
        return FD_ISSET(socket.m_socket, &m_exceptionSet) != 0;
    }

    SelectResult select();

    SelectResult select(const Duration waitMilliseconds);

    void interrupt();

};

#endif /* SOCKET_H_ */
