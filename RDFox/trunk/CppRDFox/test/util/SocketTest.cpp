// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME      SocketTest

#include <CppTest/AutoTest.h>

#include "../../src/util/Thread.h"
#include "../../src/util/Condition.h"
#include "../../src/util/Mutex.h"
#include "../../src/util/Socket.h"

static const char* const s_sendToServer = "Message sent to the server";
static const char* const s_sendToClient = "Another message sent to the client";

always_inline void writeExactly(Socket& socket, const void* const buffer, size_t size) {
    const uint8_t* current = reinterpret_cast<const uint8_t*>(buffer);
    while (size > 0) {
        const size_t written = socket.write(current, size);
        current += written;
        size -= written;
    }
}

always_inline void readExactly(Socket& socket, void* const buffer, size_t size) {
    uint8_t* current = reinterpret_cast<uint8_t*>(buffer);
    while (size > 0) {
        const size_t read = socket.read(current, size);
        current += read;
        size -= read;
    }
}

class ServerThread : public Thread {

public:

    Mutex m_mutex;
    Condition m_condition;
    bool m_ready;
    std::string m_serviceName;
    char m_serverBuffer[1024];

    ServerThread(const std::string& serviceName) : m_mutex(), m_condition(), m_ready(false), m_serviceName(serviceName) {
    }

    void waitForReady() {
        m_mutex.lock();
        while (!m_ready)
            m_condition.wait(m_mutex);
        m_mutex.unlock();
    }

    virtual void run() {
        SocketAddress listeningAddress;
        listeningAddress.open(SocketAddress::IP_V6_TRY_FIRST, true, nullptr, m_serviceName.c_str());
        Socket listener;
        listener.create(listeningAddress);
        listener.setReuseAddress(true);
        listener.listen(listeningAddress, 1);
        m_mutex.lock();
        m_ready = true;
        m_condition.signalAll();
        m_mutex.unlock();
        Socket server;
        server.accept(listener);
        server.setTCPNoDelay(true);
        writeExactly(server, s_sendToClient, ::strlen(s_sendToClient) + 1);
        server.shutdown(Socket::WRITE);
        readExactly(server, m_serverBuffer, ::strlen(s_sendToServer) + 1);
        ASSERT_TRUE(server.read(m_serverBuffer, 1) == 0);
        server.close();
        listener.close();
    }

};

TEST(testBasic) {
    ServerThread serverThread("12000");
    serverThread.start();
    serverThread.waitForReady();
    char clientBuffer[1024];
    Socket client;
    SocketAddress serverAddress;
    serverAddress.open(SocketAddress::IP_V6_TRY_FIRST, false, nullptr, "12000");
    client.create(serverAddress);
    client.connect(serverAddress);
    client.setTCPNoDelay(true);
    writeExactly(client, s_sendToServer, ::strlen(s_sendToServer) + 1);
    client.shutdown(Socket::WRITE);
    readExactly(client, clientBuffer, ::strlen(s_sendToClient) + 1);
    ASSERT_TRUE(client.read(clientBuffer, 1) == 0);
    serverThread.join();
    client.close();
    ASSERT_TRUE(::strcmp(clientBuffer, s_sendToClient) == 0);
    ASSERT_TRUE(::strcmp(serverThread.m_serverBuffer, s_sendToServer) == 0);
}

always_inline void doRead(bool& read, bool& finish, const SocketSelector& selector, Socket& client, char* const buffer) {
    if (selector.canRead(client)) {
        if (read) {
            readExactly(client, buffer, ::strlen(s_sendToClient) + 1);
            ASSERT_TRUE(::strcmp(buffer, s_sendToClient) == 0);
            read = false;
        }
        else if (finish) {
            ASSERT_TRUE(client.read(buffer, 1) == 0);
            finish = false;
        }
    }
}

always_inline void doWrite(bool& write, const SocketSelector& selector, Socket& client) {
    if (write && selector.canWrite(client)) {
        writeExactly(client, s_sendToServer, ::strlen(s_sendToServer) + 1);
        client.shutdown(Socket::WRITE);
        write = false;
    }
}

TEST(testSelect) {
    ServerThread serverThread1("12001");
    ServerThread serverThread2("12002");
    serverThread1.start();
    serverThread1.waitForReady();
    serverThread2.start();
    serverThread2.waitForReady();
    char buffer1[1024];
    char buffer2[1024];
    bool read1 = true;
    bool read2 = true;
    bool write1 = true;
    bool write2 = true;
    bool finish1 = true;
    bool finish2 = true;
    SocketAddress serverAddress1;
    SocketAddress serverAddress2;
    Socket client1;
    Socket client2;
    serverAddress1.open(SocketAddress::IP_V6_TRY_FIRST, false, nullptr, "12001");
    serverAddress2.open(SocketAddress::IP_V6_TRY_FIRST, false, nullptr, "12002");
    client1.create(serverAddress1);
    client1.connect(serverAddress1);
    client1.setTCPNoDelay(true);
    client2.create(serverAddress2);
    client2.connect(serverAddress2);
    client2.setTCPNoDelay(true);
    SocketSelector selector;
    while (read1 || read2 || write1 || write2 || finish1 || finish2) {
        selector.clear();
        selector.monitor(client1, read1 || finish1, write1, false);
        selector.monitor(client2, read2 || finish2, write2, false);
        if (selector.select() == SocketSelector::SOCKET_READY) {
            doRead(read1, finish1, selector, client1, buffer1);
            doRead(read2, finish2, selector, client2, buffer2);
            doWrite(write1, selector, client1);
            doWrite(write2, selector, client2);
        }
    }
    serverThread1.join();
    serverThread2.join();
    ASSERT_TRUE(::strcmp(serverThread1.m_serverBuffer, s_sendToServer) == 0);
    ASSERT_TRUE(::strcmp(serverThread2.m_serverBuffer, s_sendToServer) == 0);
}

class InterruptTread : public Thread {

public:

    SocketSelector& m_selector;

    InterruptTread(SocketSelector& selector) : m_selector(selector) {
    }

    void run() {
        ::sleepMS(1000);
        m_selector.interrupt();
    }
    
};

TEST(testInterrupt) {
    SocketSelector socketSelector;
    socketSelector.enableInterrupt();
    InterruptTread interruptThread(socketSelector);
    interruptThread.start();
    ASSERT_TRUE(socketSelector.select() == SocketSelector::INTERRUPTED);
    interruptThread.join();
}

#endif
