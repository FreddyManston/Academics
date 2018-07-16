// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#pragma once

#if defined(DEBUG) && !defined(WIN32)
    #define _GLIBCXX_DEBUG
#endif

#ifdef WIN32
    #define NOMINMAX
    #include <WinSock2.h>
    #include <ws2tcpip.h>
    #include <Windows.h>
    #include <Shlwapi.h>
    #include <malloc.h>
#else
    #include <sys/mman.h>
    #include <sys/stat.h>
    #include <sys/time.h>
    #include <sys/wait.h>
    #include <sys/socket.h>
    #include <netinet/tcp.h>
    #include <fcntl.h>
    #include <netdb.h>
    #include <unistd.h>
    #include <pthread.h>
    #include <errno.h>
    #include <spawn.h>
#endif

#include <assert.h>
#include <stdarg.h>
#include <stdint.h>

#include <exception>
#include <typeinfo>
#include <cstring>
#include <cmath>
#include <ctime>

#include <initializer_list>
#include <limits>
#include <memory>
#include <algorithm>
#include <string>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <vector>
#include <stack>
#include <set>
#include <unordered_set>
#include <map>
#include <unordered_map>
#include <queue>

typedef long long TimePoint;
typedef long long Duration;

#define STRINGIFY(x) #x
#define TO_STRING(x) STRINGIFY(x)

#define CACHE_LINE_SIZE 64

#ifdef WIN32

    #define always_inline __forceinline

    #pragma warning(disable : 4290)

    #define NORETURN __declspec(noreturn)

    #ifdef DEBUG
        #define UNREACHABLE        assert(false)
    #else
        #define UNREACHABLE     __assume(0)
    #endif

    #define __ALIGN(V)  _declspec(align(V))
    #define __ALIGNOF(T)  __alignof(T)
    #define ALIGN_TO_CACHE_LINE __ALIGN(CACHE_LINE_SIZE)
    #define ALIGN_PTR __ALIGN(8)
    #define ALIGN_64_T __ALIGN(8)
    #define ALIGN_32_T __ALIGN(4)
    #define ALIGN_16_T __ALIGN(2)

    typedef ALIGN_64_T double   aligned_double;
    typedef ALIGN_64_T size_t   aligned_size_t;
    typedef ALIGN_64_T uint64_t aligned_uint64_t;
    typedef ALIGN_64_T int64_t  aligned_int64_t;
    typedef ALIGN_32_T float    aligned_float;
    typedef ALIGN_32_T uint32_t aligned_uint32_t;
    typedef ALIGN_32_T int32_t  aligned_int32_t;
    typedef ALIGN_16_T uint16_t aligned_uint16_t;
    typedef ALIGN_16_T int16_t  aligned_int16_t;

    always_inline bool isNaN(const float value) {
        return _isnan(value) != 0;
    }

    always_inline bool isNaN(const double value) {
        return _isnan(value) != 0;
    }

    always_inline float roundf(float value) {
        return value < 0.0f ? ::ceilf(value - 0.5f) : ::floorf(value + 0.5f);
    }

    always_inline double round(double value) {
        return value < 0.0 ? ::ceil(value - 0.5) : ::floor(value + 0.5);
    }

    extern LARGE_INTEGER s_queryPerformanceFrequency;

    always_inline TimePoint getTimePoint() {
        LARGE_INTEGER now;
        ::QueryPerformanceCounter(&now);
        return (1000LL * now.QuadPart) / s_queryPerformanceFrequency.QuadPart;
    }

    always_inline uint8_t getMostSignificantBitIndex(uint32_t value) {
        unsigned long firstBitIndex;
        ::_BitScanReverse(&firstBitIndex, value);
        return static_cast<uint8_t>(firstBitIndex);
    }

    always_inline uint8_t getMostSignificantBitIndex64(uint64_t value) {
        unsigned long firstBitIndex;
        ::_BitScanReverse64(&firstBitIndex, value);
        return static_cast<uint8_t>(firstBitIndex);
    }

    always_inline uint8_t getRequiredByteSize(int64_t value) {
        uint64_t positive = (static_cast<uint64_t>(value >> 63) & 0xffffffffffffffffULL) ^ value;
        unsigned long firstBitIndex;
        if (::_BitScanReverse64(&firstBitIndex, positive))
            return (static_cast<uint8_t>(firstBitIndex) + 1) >> 3;
        else
            return 0;
    }

    always_inline uint8_t getNumberOfOneBits(const uint64_t value) {
        return static_cast<uint8_t>(__popcnt64(value));
    }

    always_inline int64_t getAbsoluteValue(int64_t value) {
        return ::_abs64(value);
    }

    typedef int64_t ssize_t;

    always_inline void memoryFence() {
        ::MemoryBarrier();
    }

    always_inline int32_t atomicAdd(volatile aligned_int32_t& variable, const int32_t amount) {
        return ::_InterlockedAdd(reinterpret_cast<volatile LONG*>(&variable), amount);
    }

    always_inline int64_t atomicAdd(volatile aligned_int64_t& variable, const int64_t amount) {
        return ::_InterlockedAdd64(reinterpret_cast<volatile int64_t*>(&variable), amount);
    }

    always_inline int32_t atomicSubtract(volatile aligned_int32_t& variable, const int32_t amount) {
        return ::_InterlockedAdd(reinterpret_cast<volatile LONG*>(&variable), -amount);
    }

    always_inline int64_t atomicSubtract(volatile aligned_int64_t& variable, const int64_t amount) {
        return ::_InterlockedAdd64(reinterpret_cast<volatile int64_t*>(&variable), -amount);
    }

    always_inline int32_t atomicIncrement(volatile aligned_int32_t& variable) {
        return ::_InterlockedIncrement(reinterpret_cast<volatile LONG*>(&variable));
    }

    always_inline int64_t atomicIncrement(volatile aligned_int64_t& variable) {
        return ::_InterlockedIncrement64(reinterpret_cast<volatile int64_t*>(&variable));
    }

    always_inline int32_t atomicDecrement(volatile aligned_int32_t& variable) {
        return ::_InterlockedDecrement(reinterpret_cast<volatile LONG*>(&variable));
    }

    always_inline int64_t atomicDecrement(volatile aligned_int64_t& variable) {
        return ::_InterlockedDecrement64(reinterpret_cast<volatile int64_t*>(&variable));
    }

    always_inline bool atomicConditionalSet(volatile int8_t& variable, const int8_t expectedValue, const int8_t newValue) {
        return ::_InterlockedCompareExchange8(reinterpret_cast<volatile char*>(&variable), newValue, expectedValue) == expectedValue;
    }

    always_inline bool atomicConditionalSet(volatile aligned_int16_t& variable, const int16_t expectedValue, const int16_t newValue) {
        return ::_InterlockedCompareExchange16(reinterpret_cast<volatile short*>(&variable), newValue, expectedValue) == expectedValue;
    }

    always_inline bool atomicConditionalSet(volatile aligned_int32_t& variable, const int32_t expectedValue, const int32_t newValue) {
        return ::_InterlockedCompareExchange(reinterpret_cast<volatile long*>(&variable), newValue, expectedValue) == expectedValue;
    }

    always_inline bool atomicConditionalSet(volatile aligned_int64_t& variable, const int64_t expectedValue, const int64_t newValue) {
        return ::_InterlockedCompareExchange64(reinterpret_cast<volatile int64_t*>(&variable), newValue, expectedValue) == expectedValue;
    }

    template<typename T>
    always_inline bool atomicConditionalSet(T* volatile & variable, T* const expectedValue, T* const newValue) {
        return ::_InterlockedCompareExchangePointer(reinterpret_cast<void* volatile*>(&variable), static_cast<void*>(newValue), static_cast<void*>(expectedValue)) == static_cast<void*>(expectedValue);
    }

    always_inline int8_t atomicConditionalSetEx(volatile int8_t& variable, const int8_t expectedValue, const int8_t newValue) {
        return ::_InterlockedCompareExchange8(reinterpret_cast<volatile char*>(&variable), newValue, expectedValue);
    }

    always_inline int16_t atomicConditionalSetEx(volatile aligned_int16_t& variable, const int16_t expectedValue, const int16_t newValue) {
        return ::_InterlockedCompareExchange16(reinterpret_cast<volatile short*>(&variable), newValue, expectedValue);
    }

    always_inline int32_t atomicConditionalSetEx(volatile aligned_int32_t& variable, const int32_t expectedValue, const int32_t newValue) {
        return ::_InterlockedCompareExchange(reinterpret_cast<volatile long*>(&variable), newValue, expectedValue);
    }

    always_inline int64_t atomicConditionalSetEx(volatile aligned_int64_t& variable, const int64_t expectedValue, const int64_t newValue) {
        return ::_InterlockedCompareExchange64(reinterpret_cast<volatile int64_t*>(&variable), newValue, expectedValue);
    }

    template<typename T>
    always_inline T* atomicConditionalSetEx(T* volatile & variable, T* const expectedValue, T* const newValue) {
        return static_cast<T*>(::_InterlockedCompareExchangePointer(reinterpret_cast<void* volatile*>(&variable), static_cast<void*>(newValue), static_cast<void*>(expectedValue)));
    }

    always_inline int8_t atomicExchange(volatile int8_t& variable, const int8_t value) {
        return ::_InterlockedExchange8(reinterpret_cast<volatile char*>(&variable), value);
    }

    always_inline int16_t atomicExchange(volatile aligned_int16_t& variable, const int16_t value) {
        return ::_InterlockedExchange16(reinterpret_cast<volatile SHORT*>(&variable), value);
    }

    always_inline int32_t atomicExchange(volatile aligned_int32_t& variable, const int32_t value) {
        return ::_InterlockedExchange(reinterpret_cast<volatile LONG*>(&variable), value);
    }

    always_inline int64_t atomicExchange(volatile aligned_int64_t& variable, const int64_t value) {
        return ::_InterlockedExchange64(reinterpret_cast<volatile LONGLONG*>(&variable), value);
    }

    template<typename T>
    always_inline T* atomicExchange(T* volatile & variable, T* const value) {
        return static_cast<T*>(::_InterlockedExchangePointer(reinterpret_cast<void* volatile*>(&variable), static_cast<void*>(value)));
    }

    // Overcommit support

    always_inline void ensureOvercommitSupported() {
    }

    // Thread-local variables

    #define THREAD_LOCAL_DESTRUCTOR WINAPI

    typedef DWORD ThreadLocalKey;

    typedef PFLS_CALLBACK_FUNCTION ThreadLocalDestructor;

    always_inline ThreadLocalKey allocateThreadLocalKey(ThreadLocalDestructor destructor) {
        return ::FlsAlloc(destructor);
    }

    always_inline void freeThreadLocalKey(const ThreadLocalKey threadLocalKey) {
        ::FlsFree(threadLocalKey);
    }

    always_inline void* getThreadLocalKeyValue(const ThreadLocalKey threadLocalKey) {
        return ::FlsGetValue(threadLocalKey);
    }

    always_inline void setThreadLocalKeyValue(const ThreadLocalKey threadLocalKey, void* value) {
        ::FlsSetValue(threadLocalKey, value);
    }

    // Processor configuration

    always_inline size_t getNumberOfLogicalProcessors() {
        SYSTEM_INFO systemInfo;
        ::GetSystemInfo(&systemInfo);
        return static_cast<size_t>(systemInfo.dwNumberOfProcessors);
    }

    // Sleeping

    always_inline void sleepMS(const Duration duration) {
        ::Sleep(static_cast<DWORD>(duration));
    }

    // Unicode conversion

    extern std::wstring toAPI(const std::string& text);

    extern std::wstring toAPI(const char* const text);

    // File system utilities

    always_inline bool createDirectory(const char* const pathName) {
        return ::CreateDirectory(::toAPI(pathName).c_str(), NULL) != FALSE;
    }

    always_inline bool fileExists(const char* const pathName) {
        return ::PathFileExists(::toAPI(pathName).c_str()) != FALSE;
    }

    always_inline bool deleteFile(const char* const fileName) {
        return ::DeleteFile(::toAPI(fileName).c_str()) != FALSE;
    }

#else

    #define always_inline inline __attribute__ ((always_inline))

    #define NORETURN __attribute__((noreturn))

    __attribute__((noreturn)) always_inline void __unreachable_code() { ::exit(0); }

    #define UNREACHABLE     __unreachable_code()

    #define __ALIGN(V)  __attribute__ ((aligned (V)))
    #define __ALIGNOF(T)  __alignof__(T)
    #define ALIGN_TO_CACHE_LINE __ALIGN(CACHE_LINE_SIZE)
    #define ALIGN_PTR __ALIGN(8)
    #define ALIGN_64_T __ALIGN(8)
    #define ALIGN_32_T __ALIGN(4)
    #define ALIGN_16_T __ALIGN(2)

    typedef ALIGN_64_T double   aligned_double;
    typedef ALIGN_64_T size_t   aligned_size_t;
    typedef ALIGN_64_T uint64_t aligned_uint64_t;
    typedef ALIGN_64_T int64_t  aligned_int64_t;
    typedef ALIGN_32_T float    aligned_float;
    typedef ALIGN_32_T uint32_t aligned_uint32_t;
    typedef ALIGN_32_T int32_t  aligned_int32_t;
    typedef ALIGN_16_T uint16_t aligned_uint16_t;
    typedef ALIGN_16_T int16_t  aligned_int16_t;

    always_inline void* _aligned_malloc(size_t size, size_t alignment) {
        void* memoryBlock;
        if (::posix_memalign(&memoryBlock, alignment, size))
            memoryBlock = 0;
        return memoryBlock;
    }

    always_inline void _aligned_free(void* memoryBlock) {
        return ::free(memoryBlock);
    }

    #ifndef MAP_NORESERVE
        #define MAP_NORESERVE 0
    #endif

    #ifndef MSG_NOSIGNAL
        #define MSG_NOSIGNAL 0
    #endif

    #ifdef __sun__

        always_inline bool isNaN(const float value) {
            return isnan(value);
        }

    #else

        always_inline bool isNaN(const float value) {
            return std::isnan(value);
        }

    #endif

    #ifdef __APPLE__

        always_inline bool isnan(const double value) {
            return std::isnan(value);
        }

    #endif

    always_inline TimePoint getTimePoint() {
        struct ::timeval tv;
        ::gettimeofday(&tv, 0);
        return tv.tv_sec * 1000LL + tv.tv_usec / 1000LL;
    }

    always_inline uint8_t getMostSignificantBitIndex(uint32_t value) {
        return 31 - static_cast<uint8_t>(__builtin_clz(value));
    }

    always_inline uint8_t getMostSignificantBitIndex64(uint64_t value) {
        return 63 - static_cast<uint8_t>(__builtin_clzl(value));
    }

    always_inline uint8_t getRequiredByteSize(int64_t value) {
        uint64_t positive = static_cast<uint64_t>(value >> 63) ^ static_cast<uint64_t>(value);
        if (positive == 0)
            return 0;
        else
            return (64 - __builtin_clzl(positive)) >> 3;
    }

    always_inline uint8_t getNumberOfOneBits(const uint64_t value) {
        return static_cast<uint8_t>(__builtin_popcountl(value));
    }

    always_inline int64_t getAbsoluteValue(int64_t value) {
        return ::llabs(value);
    }

    always_inline void memoryFence() {
        asm volatile ("lock orl $0,(%%rsp)" ::: "memory");
    }

    always_inline int32_t atomicAdd(volatile aligned_int32_t& variable, const int32_t amount) {
        return ::__sync_add_and_fetch(&variable, amount);
    }

    always_inline int64_t atomicAdd(volatile aligned_int64_t& variable, const int64_t amount) {
        return ::__sync_add_and_fetch(&variable, amount);
    }

    always_inline int32_t atomicSubtract(volatile aligned_int32_t& variable, const int32_t amount) {
        return ::__sync_add_and_fetch(&variable, -amount);
    }

    always_inline int64_t atomicSubtract(volatile aligned_int64_t& variable, const int64_t amount) {
        return ::__sync_add_and_fetch(&variable, -amount);
    }

    always_inline int32_t atomicIncrement(volatile aligned_int32_t& variable) {
        return ::__sync_add_and_fetch(&variable, 1);
    }

    always_inline int64_t atomicIncrement(volatile aligned_int64_t& variable) {
        return ::__sync_add_and_fetch(&variable, 1);
    }

    always_inline int32_t atomicDecrement(volatile aligned_int32_t& variable) {
        return ::__sync_add_and_fetch(&variable, -1);
    }

    always_inline int64_t atomicDecrement(volatile aligned_int64_t& variable) {
        return ::__sync_add_and_fetch(&variable, -1);
    }

    always_inline bool atomicConditionalSet(volatile int8_t& variable, const int8_t expectedValue, const int8_t newValue) {
        return ::__sync_bool_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline bool atomicConditionalSet(volatile aligned_int16_t& variable, const int16_t expectedValue, const int16_t newValue) {
        return ::__sync_bool_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline bool atomicConditionalSet(volatile aligned_int32_t& variable, const int32_t expectedValue, const int32_t newValue) {
        return ::__sync_bool_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline bool atomicConditionalSet(volatile aligned_int64_t& variable, const int64_t expectedValue, const int64_t newValue) {
        return ::__sync_bool_compare_and_swap(&variable, expectedValue, newValue);
    }

    template<typename T>
    always_inline bool atomicConditionalSet(T* volatile & variable, T* const expectedValue, T* const newValue) {
        return ::__sync_bool_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline int8_t atomicConditionalSetEx(volatile int8_t& variable, const int8_t expectedValue, const int8_t newValue) {
        return ::__sync_val_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline int16_t atomicConditionalSetEx(volatile aligned_int16_t& variable, const int16_t expectedValue, const int16_t newValue) {
        return ::__sync_val_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline int32_t atomicConditionalSetEx(volatile aligned_int32_t& variable, const int32_t expectedValue, const int32_t newValue) {
        return ::__sync_val_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline int64_t atomicConditionalSetEx(volatile aligned_int64_t& variable, const int64_t expectedValue, const int64_t newValue) {
        return ::__sync_val_compare_and_swap(&variable, expectedValue, newValue);
    }

    template<typename T>
    always_inline T* atomicConditionalSetEx(T* volatile & variable, T* const expectedValue, T* const newValue) {
        return ::__sync_val_compare_and_swap(&variable, expectedValue, newValue);
    }

    always_inline int8_t atomicExchange(volatile int8_t& variable, const int8_t value) {
        return ::__sync_lock_test_and_set(&variable, value);
    }

    always_inline int16_t atomicExchange(volatile aligned_int16_t& variable, const int16_t value) {
        return ::__sync_lock_test_and_set(&variable, value);
    }

    always_inline int32_t atomicExchange(volatile aligned_int32_t& variable, const int32_t value) {
        return ::__sync_lock_test_and_set(&variable, value);
    }

    always_inline int64_t atomicExchange(volatile aligned_int64_t& variable, const int64_t value) {
        return ::__sync_lock_test_and_set(&variable, value);
    }

    template<typename T>
    always_inline T* atomicExchange(T* volatile & variable, T* const value) {
        return ::__sync_lock_test_and_set(&variable, value);
    }

    #ifdef __APPLE__

        always_inline size_t atomicAdd(volatile aligned_size_t& variable, const size_t amount) {
            return static_cast<size_t>(::atomicAdd(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(amount)));
        }

        always_inline size_t atomicSubtract(volatile aligned_size_t& variable, const size_t amount) {
            return static_cast<size_t>(::atomicSubtract(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(amount)));
        }

        always_inline size_t atomicIncrement(volatile aligned_size_t& variable) {
            return static_cast<size_t>(::atomicIncrement(reinterpret_cast<volatile int64_t&>(variable)));
        }

        always_inline size_t atomicDecrement(volatile aligned_size_t& variable) {
            return static_cast<size_t>(::atomicDecrement(reinterpret_cast<volatile int64_t&>(variable)));
        }

        always_inline bool atomicConditionalSet(volatile aligned_size_t& variable, const size_t expectedValue, const size_t newValue) {
            return ::atomicConditionalSet(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(expectedValue), static_cast<int64_t>(newValue));
        }

        always_inline size_t atomicConditionalSetEx(volatile aligned_size_t& variable, const size_t expectedValue, const size_t newValue) {
            return static_cast<size_t>(::atomicConditionalSetEx(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(expectedValue), static_cast<int64_t>(newValue)));
        }

        always_inline size_t atomicRead(volatile const aligned_size_t& operand) {
            return operand;
        }

        always_inline void atomicWrite(volatile aligned_size_t& operand, size_t value) {
            operand = value;
        }

        always_inline size_t atomicExchange(volatile aligned_size_t& variable, const size_t value) {
            return static_cast<size_t>(::atomicExchange(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(value)));
        }

    #endif

    // Overcommit support

    #ifdef __APPLE__

        always_inline void ensureOvercommitSupported() {
        }

    #else

        extern bool s_overcommitSupportEstablished;

        extern void doEnsureOvercommitSupported();

        always_inline void ensureOvercommitSupported() {
            if (!s_overcommitSupportEstablished)
                doEnsureOvercommitSupported();
        }

    #endif

    // Thread-local variables

    #define THREAD_LOCAL_DESTRUCTOR

    typedef pthread_key_t ThreadLocalKey;

    typedef void (*ThreadLocalDestructor)(void*);

    always_inline ThreadLocalKey allocateThreadLocalKey(ThreadLocalDestructor destructor) {
        ThreadLocalKey threadLocalKey;
        ::pthread_key_create(&threadLocalKey, destructor);
        return threadLocalKey;
    }

    always_inline void freeThreadLocalKey(const ThreadLocalKey threadLocalKey) {
        ::pthread_key_delete(threadLocalKey);
    }

    always_inline void* getThreadLocalKeyValue(const ThreadLocalKey threadLocalKey) {
        return ::pthread_getspecific(threadLocalKey);
    }

    always_inline void setThreadLocalKeyValue(const ThreadLocalKey threadLocalKey, void* value) {
        ::pthread_setspecific(threadLocalKey, value);
    }

    // Processor configuration

    always_inline size_t getNumberOfLogicalProcessors() {
        return static_cast<size_t>(::sysconf(_SC_NPROCESSORS_ONLN));
    }

    // Sleeping

    always_inline void sleepMS(const Duration duration) {
        timespec sleepTime;
        sleepTime.tv_sec = static_cast<time_t>(duration / 1000);
        sleepTime.tv_nsec = static_cast<long>((duration % 1000) * 1000000L);
        ::nanosleep(&sleepTime, nullptr);
    }
    // File system utilities

    always_inline bool createDirectory(const char* const pathName) {
        return ::mkdir(pathName, 0777) == 0;
    }

    always_inline bool fileExists(const char* const pathName) {
        return ::access(pathName, F_OK) == 0;
    }

    always_inline bool deleteFile(const char* const fileName) {
        return ::unlink(fileName) == 0;
    }

    // Unicode conversion

    always_inline const char* toAPI(const char* const text) {
        return text;
    }

    always_inline const std::string& toAPI(const std::string& text) {
        return text;
    }

#endif

always_inline uint32_t atomicAdd(volatile aligned_uint32_t& variable, const uint32_t amount) {
    return static_cast<uint32_t>(::atomicAdd(reinterpret_cast<volatile int32_t&>(variable), static_cast<int32_t>(amount)));
}

always_inline uint64_t atomicAdd(volatile aligned_uint64_t& variable, const uint64_t amount) {
    return static_cast<uint64_t>(::atomicAdd(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(amount)));
}

always_inline uint32_t atomicSubtract(volatile aligned_uint32_t& variable, const uint32_t amount) {
    return static_cast<uint32_t>(::atomicSubtract(reinterpret_cast<volatile int32_t&>(variable), static_cast<int32_t>(amount)));
}

always_inline uint64_t atomicSubtract(volatile aligned_uint64_t& variable, const uint64_t amount) {
    return static_cast<uint64_t>(::atomicSubtract(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(amount)));
}

always_inline uint32_t atomicIncrement(volatile aligned_uint32_t& variable) {
    return static_cast<uint32_t>(::atomicIncrement(reinterpret_cast<volatile int32_t&>(variable)));
}

always_inline uint64_t atomicIncrement(volatile aligned_uint64_t& variable) {
    return static_cast<uint64_t>(::atomicIncrement(reinterpret_cast<volatile int64_t&>(variable)));
}

always_inline uint32_t atomicDecrement(volatile aligned_uint32_t& variable) {
    return static_cast<uint32_t>(::atomicDecrement(reinterpret_cast<volatile int32_t&>(variable)));
}

always_inline uint64_t atomicDecrement(volatile aligned_uint64_t& variable) {
    return static_cast<uint64_t>(::atomicDecrement(reinterpret_cast<volatile int64_t&>(variable)));
}

always_inline bool atomicConditionalSet(volatile uint8_t& variable, const uint8_t expectedValue, const uint8_t newValue) {
    return ::atomicConditionalSet(reinterpret_cast<volatile int8_t&>(variable), static_cast<int8_t>(expectedValue), static_cast<int8_t>(newValue));
}

always_inline bool atomicConditionalSet(volatile aligned_uint16_t& variable, const uint16_t expectedValue, const uint16_t newValue) {
    return ::atomicConditionalSet(reinterpret_cast<volatile int16_t&>(variable), static_cast<int16_t>(expectedValue), static_cast<int16_t>(newValue));
}

always_inline bool atomicConditionalSet(volatile aligned_uint32_t& variable, const uint32_t expectedValue, const uint32_t newValue) {
    return ::atomicConditionalSet(reinterpret_cast<volatile int32_t&>(variable), static_cast<int32_t>(expectedValue), static_cast<int32_t>(newValue));
}

always_inline bool atomicConditionalSet(volatile aligned_uint64_t& variable, const uint64_t expectedValue, const uint64_t newValue) {
    return ::atomicConditionalSet(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(expectedValue), static_cast<int64_t>(newValue));
}

always_inline uint8_t atomicConditionalSetEx(volatile uint8_t& variable, const uint8_t expectedValue, const uint8_t newValue) {
    return static_cast<uint8_t>(::atomicConditionalSetEx(reinterpret_cast<volatile int8_t&>(variable), static_cast<int8_t>(expectedValue), static_cast<int8_t>(newValue)));
}

always_inline uint16_t atomicConditionalSetEx(volatile aligned_uint16_t& variable, const uint16_t expectedValue, const uint16_t newValue) {
    return static_cast<uint16_t>(::atomicConditionalSetEx(reinterpret_cast<volatile int16_t&>(variable), static_cast<int16_t>(expectedValue), static_cast<int16_t>(newValue)));
}

always_inline uint32_t atomicConditionalSetEx(volatile aligned_uint32_t& variable, const uint32_t expectedValue, const uint32_t newValue) {
    return static_cast<uint32_t>(::atomicConditionalSetEx(reinterpret_cast<volatile int32_t&>(variable), static_cast<int32_t>(expectedValue), static_cast<int32_t>(newValue)));
}

always_inline uint64_t atomicConditionalSetEx(volatile aligned_uint64_t& variable, const uint64_t expectedValue, const uint64_t newValue) {
    return static_cast<uint64_t>(::atomicConditionalSetEx(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(expectedValue), static_cast<int64_t>(newValue)));
}

always_inline uint8_t atomicExchange(volatile uint8_t& variable, const uint8_t value) {
    return ::atomicExchange(reinterpret_cast<volatile int8_t&>(variable), static_cast<int8_t>(value));
}

always_inline uint16_t atomicExchange(volatile aligned_uint16_t& variable, const uint16_t value) {
    return ::atomicExchange(reinterpret_cast<volatile int16_t&>(variable), static_cast<int16_t>(value));
}

always_inline uint32_t atomicExchange(volatile aligned_uint32_t& variable, const uint32_t value) {
    return ::atomicExchange(reinterpret_cast<volatile int32_t&>(variable), static_cast<int32_t>(value));
}

always_inline uint64_t atomicExchange(volatile aligned_uint64_t& variable, const uint64_t value) {
    return ::atomicExchange(reinterpret_cast<volatile int64_t&>(variable), static_cast<int64_t>(value));
}

always_inline bool atomicRead(volatile const bool& operand) {
    return operand;
}

always_inline int8_t atomicRead(volatile const int8_t& operand) {
    return operand;
}

always_inline uint8_t atomicRead(volatile const uint8_t& operand) {
    return operand;
}

always_inline int16_t atomicRead(volatile const aligned_int16_t& operand) {
    return operand;
}

always_inline uint16_t atomicRead(volatile const aligned_uint16_t& operand) {
    return operand;
}

always_inline int32_t atomicRead(volatile const aligned_int32_t& operand) {
    return operand;
}

always_inline uint32_t atomicRead(volatile const uint32_t& operand) {
    return operand;
}

always_inline int64_t atomicRead(volatile const aligned_int64_t& operand) {
    return operand;
}

always_inline float atomicRead(volatile const float& operand) {
    return operand;
}

always_inline double atomicRead(volatile const double& operand) {
    return operand;
}

always_inline uint64_t atomicRead(volatile const aligned_uint64_t& operand) {
    return operand;
}

template<typename T>
always_inline T* atomicRead(T* volatile & operand) {
    return operand;
}

template<typename T>
always_inline T* const atomicRead(T* const volatile & operand) {
    return operand;
}

always_inline void atomicWrite(volatile bool& operand, bool value) {
    operand = value;
}

always_inline void atomicWrite(volatile int8_t& operand, int8_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile uint8_t& operand, uint8_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_int16_t& operand, int16_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_uint16_t& operand, uint16_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_int32_t& operand, int32_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_uint32_t& operand, uint32_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_int64_t& operand, int64_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_uint64_t& operand, uint64_t value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_float& operand, float value) {
    operand = value;
}

always_inline void atomicWrite(volatile aligned_double& operand, double value) {
    operand = value;
}

template<typename T>
always_inline void atomicWrite(T* volatile & operand, T* value) {
    operand = value;
}

// Alignment

extern void* alignedNewThrowing(size_t size, size_t alignment) throw(std::bad_alloc);
extern void* alignedNewNonthrowing(size_t size, size_t alignment) throw();

#define __ALIGNED(T) \
    void* operator new(size_t size) throw(std::bad_alloc) { \
        return ::alignedNewThrowing(size, __ALIGNOF(T)); \
    } \
    void* operator new(std::size_t size, const std::nothrow_t& value) throw() { \
        return ::alignedNewNonthrowing(size, __ALIGNOF(T)); \
    } \
    void* operator new[](size_t size) throw(std::bad_alloc) { \
        return ::alignedNewThrowing(size, __ALIGNOF(T)); \
    } \
    \
    void* operator new[](std::size_t size, const std::nothrow_t& value) throw() { \
        return ::alignedNewNonthrowing(size, __ALIGNOF(T)); \
    } \
    void operator delete(void* memoryBlock) { \
        ::_aligned_free(memoryBlock); \
    } \
    void operator delete[](void* memoryBlock) { \
        ::_aligned_free(memoryBlock); \
    }

#ifdef __sparc__

    const bool SUPPORTS_UNALIGNED_ACCESS = false;

    template<typename T>
    always_inline uint64_t alignValue(const uint64_t start) {
        return (start + (__ALIGNOF(T) - 1)) & ~ static_cast<uint64_t>(__ALIGNOF(T) - 1);
    }

    always_inline uint32_t read4(const uint8_t* const location) {
        return static_cast<uint32_t>(*reinterpret_cast<const uint16_t*>(location)) | (static_cast<uint32_t>(*reinterpret_cast<const uint16_t*>(location + 2)) << 16);
    }

    always_inline void write4(uint8_t* const location, const uint32_t value) {
        *reinterpret_cast<uint16_t*>(location) = static_cast<uint16_t>(value);
        *reinterpret_cast<uint16_t*>(location + 2) = static_cast<uint16_t>(value >> 16);
    }

    always_inline uint32_t increment4(uint8_t* const location, const uint32_t value) {
        const uint32_t result = ::read4(location) + value;
        ::write4(location, result);
        return result;
    }

    always_inline uint32_t decrement4(uint8_t* const location, const uint32_t value) {
        const uint32_t result = ::read4(location) - value;
        ::write4(location, result);
        return result;
    }

    always_inline uint64_t read6(const uint8_t* const location) {
        return ((*reinterpret_cast<const uint16_t*>(location)) | (static_cast<uint64_t>(*reinterpret_cast<const uint16_t*>(location + 2)) << 16) | (static_cast<uint64_t>(*reinterpret_cast<const uint16_t*>(location + 4)) << 32));
    }

    always_inline void write6(uint8_t* const location, const uint64_t value) {
        *reinterpret_cast<uint16_t*>(location) = static_cast<uint16_t>(value);
        *reinterpret_cast<uint16_t*>(location + 2) = static_cast<uint16_t>(value >> 16);
        *reinterpret_cast<uint16_t*>(location + 4) = static_cast<uint16_t>(value >> 32);
    }

    template<typename T>
    always_inline void write8Aligned4(T* & location, T* const value) {
        uint32_t* const rawLocation = reinterpret_cast<uint32_t*>(&location);
        const uint64_t rawValue = reinterpret_cast<uint64_t>(value);
        *rawLocation = static_cast<uint32_t>(rawValue >> 32);
        *(rawLocation + 1) = static_cast<uint32_t>(rawValue);
    }

    template<typename T>
    always_inline T* read8Aligned4(T* const & location) {
        const uint32_t* const rawLocation = reinterpret_cast<const uint32_t*>(&location);
        return reinterpret_cast<T*>((static_cast<uint64_t>(*rawLocation) << 32) | static_cast<uint64_t>(*(rawLocation + 1)));
    }

    always_inline void write8Aligned4(uint64_t& location, const uint64_t value) {
        uint32_t* const rawLocation = reinterpret_cast<uint32_t*>(&location);
        *rawLocation = static_cast<uint32_t>(value >> 32);
        *(rawLocation + 1) = static_cast<uint32_t>(value);
    }

    always_inline uint64_t read8Aligned4(const uint64_t& location) {
        const uint32_t* const rawLocation = reinterpret_cast<const uint32_t*>(&location);
        return (static_cast<uint64_t>(*rawLocation) << 32) | static_cast<uint64_t>(*(rawLocation + 1));
    }

    always_inline uint64_t increment8Aligned4(uint64_t& location, const uint64_t amount) {
        const uint64_t newValue = ::read8Aligned4(location) + amount;
        ::write8Aligned4(location, newValue);
        return newValue;
    }

    always_inline void atomicWrite8Aligned4(volatile uint64_t& location, const uint64_t value) {
        volatile uint32_t* const rawLocation = reinterpret_cast<volatile uint32_t*>(&location);
        *rawLocation = static_cast<uint32_t>(value >> 32);
        *(rawLocation + 1) = static_cast<uint32_t>(value);
    }

    always_inline uint64_t atomicRead8Aligned4(volatile const uint64_t& location) {
        volatile const uint32_t* const rawLocation = reinterpret_cast<volatile const uint32_t*>(&location);
        return (static_cast<uint64_t>(*rawLocation) << 32) | static_cast<uint64_t>(*(rawLocation + 1));
    }

#else

    const bool SUPPORTS_UNALIGNED_ACCESS = true;

    template<typename T>
    always_inline size_t alignValue(const uint64_t start) {
        return start;
    }

    always_inline uint32_t read4(const uint8_t* const location) {
        return *reinterpret_cast<const uint32_t*>(location);
    }

    always_inline void write4(uint8_t* const location, const uint32_t value) {
        *reinterpret_cast<uint32_t*>(location) = value;
    }

    always_inline uint32_t increment4(uint8_t* const location, const uint32_t value) {
        return *reinterpret_cast<uint32_t*>(location) += value;
    }

    always_inline uint32_t decrement4(uint8_t* const location, const uint32_t value) {
        return *reinterpret_cast<uint32_t*>(location) -= value;
    }

    always_inline uint64_t read6(const uint8_t* const location) {
        return ((*reinterpret_cast<const uint16_t*>(location)) | (static_cast<uint64_t>(*reinterpret_cast<const uint32_t*>(location + 2)) << 16));
    }

    always_inline void write6(uint8_t* const location, const uint64_t value) {
        *reinterpret_cast<uint16_t*>(location) = static_cast<uint16_t>(value);
        *reinterpret_cast<uint32_t*>(location + 2) = static_cast<uint32_t>(value >> 16);
    }

    template<typename T>
    always_inline void write8Aligned4(T* & location, T* const value) {
        location = value;
    }

    template<typename T>
    always_inline T* read8Aligned4(T* const & location) {
        return location;
    }

    always_inline void write8Aligned4(uint64_t& location, const uint64_t value) {
        location = value;
    }

    always_inline uint64_t read8Aligned4(const uint64_t& location) {
        return location;
    }

    always_inline uint64_t increment8Aligned4(uint64_t& location, const uint64_t amount) {
        return location += amount;
    }

    always_inline void atomicWrite8Aligned4(volatile uint64_t& location, const uint64_t value) {
        location = value;
    }

    always_inline uint64_t atomicRead8Aligned4(volatile const uint64_t& location) {
        return location;
    }

#endif

#ifdef __APPLE__

    always_inline void write8Aligned4(size_t& location, const size_t value) {
        ::write8Aligned4(reinterpret_cast<uint64_t&>(location), static_cast<uint64_t>(value));
    }

    always_inline size_t read8Aligned4(const size_t& location) {
        return static_cast<size_t>(::read8Aligned4(reinterpret_cast<const uint64_t&>(location)));
    }

    always_inline size_t increment8Aligned4(size_t& location, const size_t amount) {
        return static_cast<size_t>(::increment8Aligned4(reinterpret_cast<uint64_t&>(location), static_cast<uint64_t>(amount)));
    }

    always_inline void atomicWrite8Aligned4(volatile size_t& location, const size_t value) {
        ::atomicWrite8Aligned4(reinterpret_cast<volatile uint64_t&>(location), static_cast<uint64_t>(value));
    }

    always_inline size_t atomicRead8Aligned4(volatile const size_t& location) {
        return static_cast<size_t>(::atomicRead8Aligned4(reinterpret_cast<volatile const uint64_t&>(location)));
    }

#endif

// Aligning pointers

template<typename T, typename S>
always_inline S* alignPointer(S* start) {
    return reinterpret_cast<S*>((reinterpret_cast<uintptr_t>(start) + (__ALIGNOF(T) - 1)) & ~ static_cast<uintptr_t>(__ALIGNOF(T) - 1));
}

template<typename T, typename S>
always_inline const S* alignPointer(const S* start) {
    return alignPointer<T, S>(const_cast<S*>(start));
}

// File system utilities

extern void getDirectoryFiles(const std::string& directoryName, std::vector<std::string>& listOfFiles);

extern bool isAbsoluteFileName(const std::string& fileName);

// Converting string case

always_inline void toLowerCase(std::string& string) {
    std::transform(string.begin(), string.end(), string.begin(), tolower);
}

always_inline void toUpperCase(std::string& string) {
    std::transform(string.begin(), string.end(), string.begin(), toupper);
}

// Float conversions

always_inline uint32_t floatToBits(const float value) {
    union {
        float m_float;
        uint32_t m_uint32_t;
    } converter;
    converter.m_float = value;
    return converter.m_uint32_t;
}

always_inline float floatFromBits(const uint32_t value) {
    union {
        float m_float;
        uint32_t m_uint32_t;
    } converter;
    converter.m_uint32_t = value;
    return converter.m_float;
}

// Double conversions

always_inline uint64_t doubleToBits(const double value) {
    union {
        double m_double;
        uint64_t m_uint64_t;
    } converter;
    converter.m_double = value;
    return converter.m_uint64_t;
}

always_inline double doubleFromBits(const uint64_t value) {
    union {
        double m_double;
        uint64_t m_uint64_t;
    } converter;
    converter.m_uint64_t = value;
    return converter.m_double;
}

// Computing a mask based on a condition

always_inline uint8_t getConditionalMask(const bool condition, const uint8_t mask) {
    return (-static_cast<uint8_t>(condition)) & mask;
}

// Memory calls

extern size_t getVMPageSize();

extern size_t getAllocationGranularity();

extern size_t getTotalPhysicalMemorySize();

// Current date and time

extern std::string getCurrentDateTime();

// Directory separators

extern const std::string DIRECTORY_SEPARATOR;

// Not copying marker

class Unmovable {

private:

    Unmovable(const Unmovable&) = delete;
    Unmovable(Unmovable&&)  = delete;
    Unmovable& operator=(const Unmovable&)  = delete;
    Unmovable& operator=(Unmovable&&)  = delete;

public:

    Unmovable() {}

};

// static_unique_ptr_cast

template<class T, class U>
always_inline std::unique_ptr<T> static_unique_ptr_cast(std::unique_ptr<U> pointer) {
    return std::unique_ptr<T>(static_cast<T*>(pointer.release()));
}

// Printing exceptions

always_inline std::ostream& operator<<(std::ostream& output, const std::exception& e) {
    output << e.what();
    return output;
}

// unique_ptr_vector

template<typename T>
using unique_ptr_vector  = std::vector<std::unique_ptr<T> >;

// Versioning

extern const char* getRDFoxVersion();

// Hashing strings

always_inline size_t stringHashCode(const char* string) {
    size_t result = static_cast<size_t>(14695981039346656037ULL);
    char value;
    while ((value = *(string++)) != 0) {
        result ^= static_cast<size_t>(value);
        result *= static_cast<size_t>(1099511628211ULL);
    }
    return result;
}
