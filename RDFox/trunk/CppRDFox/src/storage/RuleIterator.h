// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RULEITERATOR_H_
#define RULEITERATOR_H_

#include "../Common.h"
#include "../logic/Logic.h"

class RulePlanIterator;

class RuleIterator : private Unmovable {

public:

    virtual ~RuleIterator() {
    }

    virtual const Rule& getRule() const = 0;

    virtual bool isInternalRule() const = 0;

    virtual bool hasNegation() const = 0;

    virtual bool hasAggregation() const = 0;
    
    virtual bool isActive() const = 0;

    virtual bool isJustAdded() const = 0;

    virtual bool isJustDeleted() const = 0;
    
    virtual bool isRecursive(const size_t headAtomIndex) const = 0;

    virtual size_t getHeadAtomComponentLevel(const size_t headAtomIndex) const = 0;

    virtual size_t getBodyLiteralComponentLevel(const size_t bodyLiteralIndex) const = 0;

    virtual std::unique_ptr<RulePlanIterator> createRulePlanIterator() const = 0;

    virtual bool open() = 0;

    virtual bool advance() = 0;

};

#endif /* RULEITERATOR_H_ */
