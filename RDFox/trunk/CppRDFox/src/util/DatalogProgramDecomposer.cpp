// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../querying/TermArray.h"
#include "../querying/QueryDecomposition.h"
#include "../storage/ArgumentIndexSet.h"
#include "../util/Vocabulary.h"
#include "DatalogProgramDecomposer.h"

void DatalogProgramDecomposer::getBodyForNode(LogicFactory& factory, TermArray& termArray, const QueryDecompositionNode& node, const QueryDecompositionNode* const parent, std::vector<Literal>& body, DatalogProgram& outputDatalogProgram) {
    std::vector<Literal> nodeBodyLiterals;
    for (size_t formulaIndex = 0; formulaIndex < node.getNumberOfFormulas(); ++formulaIndex) {
        const Formula& formula = node.getFormula(formulaIndex).getFormula();
        assert(formula->getType() == ATOM_FORMULA || formula->getType() == BIND_FORMULA || formula->getType() == FILTER_FORMULA);
        nodeBodyLiterals.push_back(static_pointer_cast<Literal>(formula));
    }
    for (size_t nodeIndex = 0; nodeIndex < node.getNumberOfAdjacentNodes(); ++nodeIndex) {
        const QueryDecompositionNode& neighbour = node.getAdjacentNode(nodeIndex);
        if (parent != &neighbour)
            getBodyForNode(factory, termArray, neighbour, &node, nodeBodyLiterals, outputDatalogProgram);
    }
    if (parent == 0)
        body.insert(body.end(), nodeBodyLiterals.begin(), nodeBodyLiterals.end());
    else {
        ArgumentIndexSet commonVariables(node.getNodeVariables());
        commonVariables.intersectWith(parent->getNodeVariables());
        const size_t numberOfCommonVariables = commonVariables.size();
        if (numberOfCommonVariables > 2)
            body.insert(body.end(), nodeBodyLiterals.begin(), nodeBodyLiterals.end());
        else {
            std::ostringstream name;
            name << RDFOX_NS << "replace" << m_nextReplacementIndex++;
            ResourceByName nodeResource = factory->getIRIReference(name.str());
            Term V1;
            Term V2;
            bool V1seen = false;
            for (ArgumentIndexSet::iterator iterator = commonVariables.begin(); iterator != commonVariables.end(); ++iterator) {
                if (commonVariables.contains(*iterator)) {
                    if (V1seen)
                        V2 = termArray.getTerm(*iterator);
                    else {
                        V1 = termArray.getTerm(*iterator);
                        V1seen = true;
                    }
                }
            }
            Atom atom;
            switch (numberOfCommonVariables) {
            case 0:
                atom = factory->getRDFAtom(nodeResource, factory->getIRIReference(RDF_TYPE), nodeResource);
                break;
            case 1:
                atom = factory->getRDFAtom(V1, factory->getIRIReference(RDF_TYPE), nodeResource);
                break;
            case 2:
                atom = factory->getRDFAtom(V1, nodeResource, V2);
                break;
            }
            Rule rule = factory->getRule(atom, nodeBodyLiterals);
            outputDatalogProgram.push_back(rule);
            body.push_back(atom);
        }
    }
}

const QueryDecompositionNode* DatalogProgramDecomposer::getRootNode(const QueryDecomposition& decomposition, const ArgumentIndexSet& answerVariables) const {
    for (size_t nodeIndex = 0; nodeIndex < decomposition.getNumberOfNodes(); ++nodeIndex) {
        const QueryDecompositionNode& node = decomposition.getNode(nodeIndex);
        if (node.getNodeVariables().contains(answerVariables))
            return &node;
    }
    return 0;
}

DatalogProgramDecomposer::DatalogProgramDecomposer() : m_nextReplacementIndex(0) {
}

void DatalogProgramDecomposer::decomposeProgram(const DatalogProgram& inputDatalogProgram, DatalogProgram& outputDatalogProgram) {
    for (DatalogProgram::const_iterator iterator = inputDatalogProgram.begin(); iterator != inputDatalogProgram.end(); ++iterator)
        decomposeRule(*iterator, outputDatalogProgram);
}

void DatalogProgramDecomposer::decomposeRule(const Rule& rule, DatalogProgram& outputDatalogProgram) {
    TermArray termArray(rule);
    ArgumentIndexSet answerVariables;
    const std::vector<Atom>& head = rule->getHead();
    for (auto headIterator = head.begin(); headIterator != head.end(); ++headIterator) {
        answerVariables.clear();
        const Atom& head = *headIterator;
        const std::vector<Term>& arguments = head->getArguments();
        for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator) {
            const Term& argument = *argumentIterator;
            if (argument->getType() == VARIABLE)
                answerVariables.add(termArray.getPosition(argument));
        }
        QueryDecomposition decomposition(termArray, answerVariables);
        const size_t numberOfBodyLiterals = rule->getNumberOfBodyLiterals();
        for (size_t index = 0; index < numberOfBodyLiterals; ++index)
            decomposition.addFormula(rule->getBody(index));
        ::initializeUsingPrimalGraph(decomposition, true);
        const QueryDecompositionNode* const root = getRootNode(decomposition, answerVariables);
        if (root == 0)
            throw RDF_STORE_EXCEPTION("A decomposition root for the rule cannot can be identified.");
        std::vector<Literal> bodyLiterals;
        getBodyForNode(rule->getFactory(), termArray, *root, 0, bodyLiterals, outputDatalogProgram);
        Rule rootRule = rule->getFactory()->getRule(head, bodyLiterals);
        outputDatalogProgram.push_back(rootRule);
    }
}
