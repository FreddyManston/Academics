// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATALOGPROGRAMDECOMPOSER_H_
#define DATALOGPROGRAMDECOMPOSER_H_

#include "../all.h"
#include "../logic/Logic.h"

class ArgumentIndexSet;
class TermArray;
class QueryDecompositionNode;
class QueryDecomposition;

class DatalogProgramDecomposer {

protected:

    size_t m_nextReplacementIndex;

    void getBodyForNode(LogicFactory& factory, TermArray& termArray, const QueryDecompositionNode& node, const QueryDecompositionNode* const parent, std::vector<Literal>& body, DatalogProgram& outputDatalogProgram);

    const QueryDecompositionNode* getRootNode(const QueryDecomposition& decomposition, const ArgumentIndexSet& answerVariables) const;

public:

    DatalogProgramDecomposer();

    void decomposeProgram(const DatalogProgram& inputDatalogProgram, DatalogProgram& outputDatalogProgram);

    void decomposeRule(const Rule& rule, DatalogProgram& outputDatalogProgram);

};

#endif // DATALOGPROGRAMDECOMPOSER_H_
