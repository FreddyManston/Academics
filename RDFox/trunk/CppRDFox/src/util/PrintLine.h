// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PRINTLINE_H_
#define PRINTLINE_H_

#include "Mutex.h"

extern Mutex s_outputMutex;

always_inline void doPrintLine() {
    std::cout << std::endl;
}

template<typename T, typename... Rest>
always_inline void doPrintLine(T&& value, Rest&&... rest) {
    std::cout << value;
    doPrintLine(std::forward<Rest>(rest)...);
}

template<typename... Args>
always_inline void printLine(Args&&... args) {
    MutexHolder mutexHolder(s_outputMutex);
    doPrintLine(std::forward<Args>(args)...);
}

#endif /* PRINTLINE_H_ */
