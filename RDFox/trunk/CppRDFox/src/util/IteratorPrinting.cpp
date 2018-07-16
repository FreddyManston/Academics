// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "../logic/Logic.h"
#include "../querying/TermArray.h"
#include "../storage/TupleIterator.h"
#include "../storage/ArgumentIndexSet.h"
#include "IteratorPrinting.h"

IteratorPrinting::IteratorPrinting(const Prefixes& prefixes, const TermArray& termArray, const Dictionary& dictionary) :
    m_prefixes(prefixes),
    m_termArray(termArray),
    m_dictionary(dictionary)
{
}

void IteratorPrinting::printIterator(const TupleIterator& tupleIterator, std::ostream& output) const {
    output << tupleIterator.getName() << "(";
    const size_t arity = tupleIterator.getArity();
    for (size_t index = 0; index < arity; ++index) {
        if (index != 0)
            output << ", ";
        ArgumentIndex argumentIndex = tupleIterator.getArgumentIndexes()[index];
        if (argumentIndex < m_termArray.getNumberOfTerms()) {
            Term term = m_termArray.getTerm(argumentIndex);
            output << term->toString(m_prefixes);
            if (tupleIterator.getAllInputArguments().contains(argumentIndex)) {
                if (!tupleIterator.getSurelyBoundInputArguments().contains(argumentIndex))
                    output << "+";
            }
            else if (tupleIterator.getAllArguments().contains(argumentIndex)) {
                if (tupleIterator.getSurelyBoundArguments().contains(argumentIndex))
                    output << "*";
                else
                    output << "#";
            }
        }
        else {
            const ResourceID resourceID = tupleIterator.getArgumentsBuffer()[argumentIndex];
            ResourceText resourceText;
            m_dictionary.getResource(resourceID, resourceText);
            output << resourceText.toString(m_prefixes);
        }
    }
    output << ')';
}

void IteratorPrinting::printIteratorSubtree(const TupleIterator& tupleIterator, std::ostream& output) const {
    printIteratorSubtree(tupleIterator, output, 0);
}

void IteratorPrinting::printIteratorSubtree(const TupleIterator& tupleIterator, std::ostream& output, const size_t level) const {
    for (size_t index = 0; index < level * 4; ++index)
        output << ' ';
    printIterator(tupleIterator, output);
    output << std::endl;
    const size_t numberOfChildIterators = tupleIterator.getNumberOfChildIterators();
    for (size_t childIteratorIndex = 0; childIteratorIndex < numberOfChildIterators; ++childIteratorIndex)
        printIteratorSubtree(tupleIterator.getChildIterator(childIteratorIndex), output, level + 1);
}
