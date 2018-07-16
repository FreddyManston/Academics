// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NODESERVERLISTENER_H_
#define NODESERVERLISTENER_H_

#include "../Common.h"

class NodeServerListener {

public:

    virtual ~NodeServerListener() {
    }

    virtual void nodeServerStarting() = 0;

    virtual void nodeServerReady() = 0;

    virtual void nodeServerFinishing() = 0;

    virtual void nodeServerError(const std::exception& error) = 0;

};

#endif // NODESERVERLISTENER_H_
