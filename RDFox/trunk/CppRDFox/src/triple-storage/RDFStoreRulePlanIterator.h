// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RDFSTORERULEPLANITERATOR_H_
#define RDFSTORERULEPLANITERATOR_H_

#include "../reasoning/RuleIndex.h"
#include "../storage/RulePlanIterator.h"

typedef UnderlyingLiteralInfo<PivotNegationBodyLiteralInfo> UnderlyingNegationLiteralInfo;

class RDFStoreRulePlanIterator : public RulePlanIterator {

protected:

    const RuleInfo& m_ruleInfo;
    std::vector<BodyLiteralInfo*> m_rulePlan;
    std::vector<UnderlyingNegationLiteralInfo*> m_underlyingNegationPlan;
    size_t m_currentPositivePlanIndex;
    size_t m_currentNegationPlanIndex;
    size_t m_currentUnderlyingPlanIndex;

    bool load();

public:

    RDFStoreRulePlanIterator(const RuleInfo& ruleInfo);

    virtual size_t getNumberOfLiterals() const;

    virtual const Literal& getLiteral(const size_t literalIndex) const;

    virtual bool isLiteralAfterPivot(const size_t literalIndex) const;

    virtual size_t getLiteralComponentLevel(const size_t literalIndex) const;

    virtual size_t getNumberOfUnderlyingLiteralsInPivot() const;

    virtual const Literal& getUnderlyingLiteral(const size_t underlyingLiteralIndex) const;

    virtual bool isUnderlyingLiteralfterPivot(const size_t underlyingLiteralIndex) const;

    virtual bool open();

    virtual bool advance();

};

#endif // RDFSTORERULEPLANITERATOR_H_
