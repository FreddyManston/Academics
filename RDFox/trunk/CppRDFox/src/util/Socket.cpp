// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Socket.h"

#ifdef WIN32

    #define socklen_t int

    static always_inline int getErrorCode() {
        return ::WSAGetLastError();
    }

    static always_inline void formatError(std::ostringstream& message, int errorCode) {
        LPSTR errorString = 0;
        DWORD size = ::FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, 0, static_cast<DWORD>(errorCode), 0, reinterpret_cast<LPSTR>(&errorString), 0, 0);
        message << errorString;
        std::cout << message.str() << std::endl;
        ::LocalFree(errorString);
    }

    struct WinsockInitializer {
        bool m_initializationSuccessful;

        WinsockInitializer() {
            WSADATA wsaData;
            m_initializationSuccessful = (::WSAStartup(MAKEWORD(1, 0), &wsaData) != 0);
        }

        ~WinsockInitializer() {
            if (m_initializationSuccessful)
                ::WSACleanup();
        }
    };

    static WinsockInitializer s_winsockInitializer;

#else

    #define closesocket close
    #define ioctlsocket ioctl

    static always_inline int getErrorCode() {
        return errno;
    }

    static always_inline void formatError(std::ostringstream& message, int errorCode) {
        message << ::gai_strerror(errorCode);
    }

#endif

static NORETURN always_inline void doReportError(const char* const message, int errorCode) {
    std::ostringstream buffer;
    buffer << message << ": " << errorCode;
    throw RDF_STORE_EXCEPTION(buffer.str());
}

static NORETURN always_inline void doReportError(const char* const message) {
    doReportError(message, ::getErrorCode());
}

static void doConnectLocally(SocketType& socket1, SocketType& socket2) {
    if (socket1 != INVALID_SOCKET_HANDLE)
        ::closesocket(socket1);
    if (socket2 != INVALID_SOCKET_HANDLE)
        ::closesocket(socket2);
    sockaddr_in inaddr;
    ::memset(&inaddr, 0, sizeof(inaddr));
    inaddr.sin_family = AF_INET;
    inaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    inaddr.sin_port = 0;
    SocketType listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    int yes = 1;
    if (::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&yes), sizeof(yes)) != 0)
        ::doReportError("Error in connecting sockets: setsockopt");
    if (::bind(listener, reinterpret_cast<sockaddr *>(&inaddr), sizeof(inaddr)) != 0)
        ::doReportError("Error in connecting sockets: bind");
    if (::listen(listener,1) != 0)
        ::doReportError("Error in connecting sockets: listen");
    sockaddr addr;
    ::memset(&addr, 0, sizeof(addr));
    socklen_t length = sizeof(inaddr);
    if (::getsockname(listener, &addr, &length) != 0) {
        ::closesocket(listener);
        ::doReportError("Error in connecting sockets: getsockname");
    }
    socket1 = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socket1 == INVALID_SOCKET_HANDLE) {
        ::closesocket(listener);
        ::doReportError("Error in connecting sockets: socket");
    }
    if (::connect(socket1, &addr, length) != 0) {
        ::closesocket(listener);
        ::closesocket(socket1);
        ::doReportError("Error in connecting sockets: connect");
    }
    socket2 = ::accept(listener, 0, 0);
    if (socket2 == INVALID_SOCKET_HANDLE) {
        ::closesocket(listener);
        ::closesocket(socket1);
        ::doReportError("Error in connecting sockets:  accept");
    }
    ::closesocket(listener);
}

// SocketAddress

void SocketAddress::open(const ProtocolType protocolType, const bool passive, const char* const nodeName, const char* const serviceName) {
    close();
    addrinfo hints;
    ::memset(&hints, 0, sizeof(addrinfo));
    switch (protocolType) {
    case IP_V6_ONLY:
    case IP_V6_TRY_FIRST:
        hints.ai_family = AF_INET6;
        break;
    case IP_V4_ONLY:
        hints.ai_family = AF_INET;
        break;
    case IP_ANY:
        hints.ai_family = AF_UNSPEC;
        break;
    }
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = (passive ? AI_PASSIVE : 0);
    hints.ai_protocol = 0;
    int result = ::getaddrinfo(nodeName, serviceName, &hints, &m_firstAddress);
    if (result != 0 && protocolType == IP_V6_TRY_FIRST) {
        hints.ai_family = AF_UNSPEC;
        result = ::getaddrinfo(nodeName, serviceName, &hints, &m_firstAddress);
    }
    if (result != 0) {
        std::ostringstream message;
        message << "Could not get the ";
        if (passive)
            message << "passive ";
        message << "address of ";
        if (serviceName != nullptr)
            message << " service '" << serviceName << "' on";
        if (nodeName == nullptr)
            message << " local node:";
        else
            message << " node '" << nodeName << "': ";
        formatError(message, result);
        throw RDF_STORE_EXCEPTION(message.str());
    }
    m_currentAddress = m_firstAddress;
}

void SocketAddress::close() {
    if (m_firstAddress != nullptr) {
        ::freeaddrinfo(m_firstAddress);
        m_firstAddress = m_currentAddress = nullptr;
    }
}

// Socket

void Socket::reportError(const char* const message) {
    ::doReportError(message);
}

Socket::~Socket() {
    close();
}

void Socket::create(const SocketAddress& socketAddress) {
    close();
    m_socket = ::socket(socketAddress.m_currentAddress->ai_family, socketAddress.m_currentAddress->ai_socktype, socketAddress.m_currentAddress->ai_protocol);
    if (m_socket == INVALID_SOCKET_HANDLE)
        reportError("Cannot create socket");
#ifndef WIN32
    if (::fcntl(m_socket, F_SETFD, FD_CLOEXEC) != 0) {
        const int errorCode = ::getErrorCode();
        close();
        doReportError("Cannot change socket to not be close-on-exec", errorCode);
    }
#endif
#ifdef __APPLE__
    int noSIGPIPE = 1;
    if (::setsockopt(m_socket, SOL_SOCKET, SO_NOSIGPIPE, &noSIGPIPE, sizeof(noSIGPIPE)) != 0) {
        const int errorCode = ::getErrorCode();
        close();
        doReportError("Cannot change socket to not send SIGPIPE", errorCode);
    }
#endif
}

bool Socket::connect(const SocketAddress& socketAddress) {
    // The cast to int is to make Windows happy.
    if (::connect(m_socket, socketAddress.m_currentAddress->ai_addr, static_cast<int>(socketAddress.m_currentAddress->ai_addrlen)) != 0) {
        const int errorCode = ::getErrorCode();
#ifdef WIN32
        if (errorCode == WSAEWOULDBLOCK)
#else
        if (errorCode == EINPROGRESS)
#endif
            return false;
        close();
        doReportError("Cannot connect socket", errorCode);
    }
    else
        return true;
}

void Socket::connectLocally(Socket& other) {
    ::doConnectLocally(m_socket, other.m_socket);
}

void Socket::listen(const SocketAddress& socketAddress, const size_t backlog) {
#ifdef WIN32
    if (::bind(m_socket, socketAddress.m_currentAddress->ai_addr, static_cast<int>(socketAddress.m_currentAddress->ai_addrlen)) == 0)
#else
    if (::bind(m_socket, socketAddress.m_currentAddress->ai_addr, socketAddress.m_currentAddress->ai_addrlen) == 0)
#endif
        if (::listen(m_socket, static_cast<int>(backlog)) == 0)
            return;
    close();
    doReportError("Cannot start listening");
}

void Socket::accept(Socket& listeningSocket) {
    close();
    m_socket = ::accept(listeningSocket.m_socket, nullptr, nullptr);
    if (m_socket == INVALID_SOCKET_HANDLE)
        doReportError("Cannot accept the connection from a socket");
#ifndef WIN32
    int flags = ::fcntl(m_socket, F_GETFL, 1);
    if (flags == -1 || ::fcntl(m_socket, F_SETFD, flags | FD_CLOEXEC) != 0) {
        const int errorCode = ::getErrorCode();
        close();
        doReportError("Cannot change socket to not be close-on-exec.", errorCode);
    }
#endif
#ifdef __APPLE__
    int noSIGPIPE = 1;
    if (::setsockopt(m_socket, SOL_SOCKET, SO_NOSIGPIPE, &noSIGPIPE, sizeof(noSIGPIPE)) != 0) {
        const int errorCode = ::getErrorCode();
        close();
        doReportError("Cannot change socket to not send SIGPIPE", errorCode);
    }
#endif
}

// SocketSelector

void SocketSelector::reportError(const char* const message) {
    ::doReportError(message);
}

#ifdef WIN32

SocketSelector::~SocketSelector() {
    if (m_interrupt[0] != INVALID_SOCKET_HANDLE)
        ::closesocket(m_interrupt[0]);
    if (m_interrupt[1] != INVALID_SOCKET_HANDLE)
        ::closesocket(m_interrupt[1]);
}

void SocketSelector::enableInterrupt() {
    disableInterrupt();
    ::doConnectLocally(m_interrupt[0],  m_interrupt[1]);
    u_long mode = 1;
    if (::ioctlsocket(m_interrupt[0], FIONBIO, &mode) != 0)
        reportError("Error opening the interrupt pipe: ioctlsocket");
    FD_SET(m_interrupt[0], &m_readSet);
}

void SocketSelector::disableInterrupt() {
    if (m_interrupt[0] != INVALID_SOCKET_HANDLE) {
        ::closesocket(m_interrupt[0]);
        m_interrupt[0] = INVALID_SOCKET_HANDLE;
    }
    if (m_interrupt[1] != INVALID_SOCKET_HANDLE) {
        ::closesocket(m_interrupt[1]);
        m_interrupt[1] = INVALID_SOCKET_HANDLE;
    }
}


always_inline SocketSelector::SelectResult processSelectResult(const int result, const SocketType interruptRead, fd_set& readSet) {
    if (result < 0)
        doReportError("Error while selecing sockets");
    else if (result == 0)
        return SocketSelector::TIMEOUT;
    else if (interruptRead != INVALID_SOCKET_HANDLE && (FD_ISSET(interruptRead, &readSet) != 0)) {
        uint8_t buffer;
        const int bytesRead = ::recv(interruptRead, reinterpret_cast<char*>(&buffer), 1, 0);
        if (bytesRead == -1) {
            if (::WSAGetLastError() != WSAEWOULDBLOCK)
                doReportError("Error while reading from the interrupt pipe");
        }
        else if (bytesRead == 1)
            return SocketSelector::INTERRUPTED;
    }
    return SocketSelector::SOCKET_READY;
}

SocketSelector::SelectResult SocketSelector::select() {
    const int result = ::select(0, &m_readSet, &m_writeSet, &m_exceptionSet, nullptr);
    return processSelectResult(result, m_interrupt[0], m_readSet);
}

SocketSelector::SelectResult SocketSelector::select(const Duration waitMilliseconds) {
    timeval timeout;
    timeout.tv_sec = static_cast<long>(waitMilliseconds / 1000);
    timeout.tv_usec = static_cast<long>((waitMilliseconds % 1000) * 1000);
    const int result = ::select(0, &m_readSet, &m_writeSet, &m_exceptionSet, &timeout);
    return processSelectResult(result, m_interrupt[0], m_readSet);
}

void SocketSelector::interrupt() {
    if (m_interrupt[1] == INVALID_SOCKET_HANDLE)
        throw RDF_STORE_EXCEPTION("Interrupts were not enabled on this selector.");
    uint8_t buffer = 0;
    int written;
    while ((written = ::send(m_interrupt[1], reinterpret_cast<char*>(&buffer), 1, 0)) != 1) {
        if (written == -1)
            reportError("Error while interrupting select");
    }
}

#else

SocketSelector::~SocketSelector() {
    if (m_interrupt[0] != INVALID_SOCKET_HANDLE)
        ::close(m_interrupt[0]);
    if (m_interrupt[1] != INVALID_SOCKET_HANDLE)
        ::close(m_interrupt[1]);
}

void SocketSelector::enableInterrupt() {
    disableInterrupt();
    if (::pipe(m_interrupt) != 0)
        reportError("Cannot create the pipe for interrupting select");
    int flags = ::fcntl(m_interrupt[0], F_GETFL, 0);
    if (flags == -1 || ::fcntl(m_interrupt[0], F_SETFL, flags | O_NONBLOCK | O_CLOEXEC) == -1)
        reportError("Cannot set close-on-exec and close-on-exec option on the read portion of the pipe");
    flags = ::fcntl(m_interrupt[1], F_GETFL, 1);
    if (flags == -1 || ::fcntl(m_interrupt[1], F_SETFL, flags | O_CLOEXEC) == -1)
        reportError("Cannot set close-on-exec on the write option on a pipe");
    FD_SET(m_interrupt[0], &m_readSet);
    m_maxSocket = std::max(m_maxSocket, m_interrupt[0]);
}

void SocketSelector::disableInterrupt() {
    if (m_interrupt[0] != INVALID_SOCKET_HANDLE) {
        ::close(m_interrupt[0]);
        m_interrupt[0] = INVALID_SOCKET_HANDLE;
    }
    if (m_interrupt[1] != INVALID_SOCKET_HANDLE) {
        ::close(m_interrupt[1]);
        m_interrupt[1] = INVALID_SOCKET_HANDLE;
    }
}

always_inline SocketSelector::SelectResult processSelectResult(const int result, const SocketType interruptRead, fd_set& readSet) {
    if (result < 0)
        doReportError("Error while selecing sockets");
    else if (result == 0)
        return SocketSelector::TIMEOUT;
    else if (interruptRead != INVALID_SOCKET_HANDLE && (FD_ISSET(interruptRead, &readSet) != 0)) {
        uint8_t buffer;
        const ssize_t bytesRead = ::read(interruptRead, &buffer, 1);
        if (bytesRead == -1) {
            if (errno != EWOULDBLOCK)
                doReportError("Error while reading from the interrupt pipe");
        }
        else if (bytesRead == 1)
            return SocketSelector::INTERRUPTED;
    }
    return SocketSelector::SOCKET_READY;
}

SocketSelector::SelectResult SocketSelector::select() {
    const int result = ::select(m_maxSocket + 1, &m_readSet, &m_writeSet, &m_exceptionSet, nullptr);
    return processSelectResult(result, m_interrupt[0], m_readSet);
}

SocketSelector::SelectResult SocketSelector::select(const Duration waitMilliseconds) {
    timeval timeout;
    timeout.tv_sec = static_cast<time_t>(waitMilliseconds / 1000);
    timeout.tv_usec = static_cast<suseconds_t>((waitMilliseconds % 1000) * 1000);
    const int result = ::select(m_maxSocket + 1, &m_readSet, &m_writeSet, &m_exceptionSet, &timeout);
    return processSelectResult(result, m_interrupt[0], m_readSet);
}

void SocketSelector::interrupt() {
    if (m_interrupt[1] == INVALID_SOCKET_HANDLE)
        throw RDF_STORE_EXCEPTION("Interrupts were not enabled on this selector.");
    uint8_t buffer = 0;
    ssize_t written;
    while ((written = ::write(m_interrupt[1], reinterpret_cast<char*>(&buffer), 1)) != 1) {
        if (written == -1)
            reportError("Error while interrupting select");
    }
}

#endif
