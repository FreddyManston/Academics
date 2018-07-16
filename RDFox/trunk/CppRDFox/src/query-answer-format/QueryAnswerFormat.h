// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYANSWERFORMAT_H_
#define QUERYANSWERFORMAT_H_

#include "../Common.h"
#include "../logic/Logic.h"

class Dictionary;
class OutputStream;
class Prefixes;
class ResourceValueCache;;

class QueryAnswerFormat : private Unmovable {

protected:

    OutputStream& m_outputStream;
    const bool m_isAskQuery;
    const std::vector<Term>& m_answerTerms;

public:

    QueryAnswerFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms);

    virtual ~QueryAnswerFormat();

    virtual void printPrologue() = 0;

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) = 0;

    virtual void printEpilogue() = 0;

};

extern std::unique_ptr<QueryAnswerFormat> newQueryAnswerFormat(const std::string& queryAnswerFormatName, OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms, const Prefixes& prefixes);

#endif /* QUERYANSWERFORMAT_H_ */
