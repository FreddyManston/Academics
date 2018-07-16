// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RULEPLANITERATOR_H_
#define RULEPLANITERATOR_H_

#include "../Common.h"
#include "../logic/Logic.h"

class RulePlanIterator : private Unmovable {

public:

    virtual size_t getNumberOfLiterals() const = 0;

    virtual const Literal& getLiteral(const size_t literalIndex) const = 0;

    virtual bool isLiteralAfterPivot(const size_t literalIndex) const = 0;

    virtual size_t getLiteralComponentLevel(const size_t literalIndex) const = 0;

    virtual size_t getNumberOfUnderlyingLiteralsInPivot() const = 0;

    virtual const Literal& getUnderlyingLiteral(const size_t underlyingLiteralIndex) const = 0;

    virtual bool isUnderlyingLiteralfterPivot(const size_t underlyingLiteralIndex) const = 0;

    virtual bool open() = 0;

    virtual bool advance() = 0;

};

#endif /* RULEPLANITERATOR_H_ */
