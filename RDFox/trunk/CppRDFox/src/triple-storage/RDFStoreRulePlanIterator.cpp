// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "RDFStoreRulePlanIterator.h"

bool RDFStoreRulePlanIterator::load() {
    if (m_currentPositivePlanIndex < m_ruleInfo.getNumberOfPivotPositiveEvaluationPlans()) {
        m_ruleInfo.loadPivotPositiveEvaluationPlan(m_currentPositivePlanIndex, m_rulePlan);
        m_underlyingNegationPlan.clear();
        return true;
    }
    else if (m_currentNegationPlanIndex < m_ruleInfo.getNumberOfPivotNegationEvaluationPlans()) {
        assert(m_currentUnderlyingPlanIndex < m_ruleInfo.getNumberOfPivotNegationUnderlyingEvaluationPlans(m_currentNegationPlanIndex));
        if (m_currentUnderlyingPlanIndex == 0)
            m_ruleInfo.loadPivotNegationEvaluationPlan(m_currentNegationPlanIndex, m_rulePlan);
        m_ruleInfo.loadPivotNegationUnderlyingEvaluationPlan(m_currentNegationPlanIndex, m_currentUnderlyingPlanIndex, m_underlyingNegationPlan);
        return true;
    }
    else
        return false;
}

RDFStoreRulePlanIterator::RDFStoreRulePlanIterator(const RuleInfo& ruleInfo) :
    m_ruleInfo(ruleInfo),
    m_rulePlan(),
    m_underlyingNegationPlan(),
    m_currentPositivePlanIndex(0),
    m_currentNegationPlanIndex(0),
    m_currentUnderlyingPlanIndex(0)
{
}

size_t RDFStoreRulePlanIterator::getNumberOfLiterals() const {
    return m_rulePlan.size();
}

const Literal& RDFStoreRulePlanIterator::getLiteral(const size_t literalIndex) const {
    return m_rulePlan[literalIndex]->getLiteral();
}

bool RDFStoreRulePlanIterator::isLiteralAfterPivot(const size_t literalIndex) const {
    return m_rulePlan[literalIndex]->getLiteralPosition() == AFTER_PIVOT_ATOM;
}

size_t RDFStoreRulePlanIterator::getLiteralComponentLevel(const size_t literalIndex) const {
    return m_rulePlan[literalIndex]->getComponentLevel();
}

size_t RDFStoreRulePlanIterator::getNumberOfUnderlyingLiteralsInPivot() const {
    if (m_currentPositivePlanIndex < m_ruleInfo.getNumberOfPivotPositiveEvaluationPlans())
        return 1;
    else
        return m_underlyingNegationPlan.size();
}

const Literal& RDFStoreRulePlanIterator::getUnderlyingLiteral(const size_t underlyingLiteralIndex) const {
    if (m_currentPositivePlanIndex < m_ruleInfo.getNumberOfPivotPositiveEvaluationPlans())
        return m_rulePlan[0]->getLiteral();
    else
        return m_underlyingNegationPlan[underlyingLiteralIndex]->getLiteral();
}

bool RDFStoreRulePlanIterator::isUnderlyingLiteralfterPivot(const size_t underlyingLiteralIndex) const {
    if (m_currentPositivePlanIndex < m_ruleInfo.getNumberOfPivotPositiveEvaluationPlans())
        return false;
    else
        return m_underlyingNegationPlan[underlyingLiteralIndex]->getLiteralPosition() == AFTER_PIVOT_ATOM;
}

bool RDFStoreRulePlanIterator::open() {
    m_currentPositivePlanIndex = m_currentNegationPlanIndex = m_currentUnderlyingPlanIndex = 0;
    return load();
}

bool RDFStoreRulePlanIterator::advance() {
    if (m_currentPositivePlanIndex == m_ruleInfo.getNumberOfPivotPositiveEvaluationPlans() && m_currentNegationPlanIndex < m_ruleInfo.getNumberOfPivotNegationEvaluationPlans()) {
        ++m_currentUnderlyingPlanIndex;
        if (m_currentUnderlyingPlanIndex == m_ruleInfo.getNumberOfPivotNegationUnderlyingEvaluationPlans(m_currentNegationPlanIndex)) {
            ++m_currentNegationPlanIndex;
            m_currentUnderlyingPlanIndex = 0;
        }
    }
    else if (m_currentPositivePlanIndex < m_ruleInfo.getNumberOfPivotPositiveEvaluationPlans())
        ++m_currentPositivePlanIndex;
    return load();
}
