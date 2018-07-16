// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SPARQLENDPOINT_H_
#define SPARQLENDPOINT_H_

#if !defined(WIN32) && !defined(__APPLE__)
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wmisleading-indentation"
#endif

#include "../Common.h"
#include <dlib/server.h>

class DataStore;

class SPARQLEndPoint : public dlib::server_http {

protected:

    virtual DataStore* getDataStore() const = 0;

public:

    SPARQLEndPoint();

    virtual const std::string on_request(const dlib::incoming_things& incoming, dlib::outgoing_things& outgoing);

};

#endif /* SPARQLENDPOINT_H_ */
