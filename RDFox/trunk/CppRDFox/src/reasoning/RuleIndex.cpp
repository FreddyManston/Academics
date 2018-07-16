// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/ResourceValueCache.h"
#include "../builtins/BuiltinExpressionEvaluator.h"
#include "../builtins/FilterTupleIterator.h"
#include "../builtins/BindTupleIterator.h"
#include "../aggregates/AggregateFunctionEvaluator.h"
#include "../aggregates/AggregateTupleIterator.h"
#include "../formats/datalog/DatalogParser.h"
#include "../formats/sources/MemorySource.h"
#include "../querying/EmptyTupleIterator.h"
#include "../querying/NegationIterator.h"
#include "../querying/NestedIndexLoopJoinIterator.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/ThreadContext.h"
#include "../util/InputImporter.h"
#include "../util/LockFreeQueue.h"
#include "RuleIndexImpl.h"

template<class RT>
static std::unique_ptr<TupleIterator> compileConjunction(const ArgumentIndexSet& initiallyBoundVariables, const std::vector<ResourceID>& defaultArgumentsBuffer, const std::vector<Literal>& conjunction, const std::vector<ArgumentIndexSet>& literalVariables, const std::vector<std::vector<ArgumentIndex> >& argumentIndexes, DataStore& dataStore, RT& resolver, ResourceValueCache& resourceValueCache, const TermArray& termArray, std::vector<ResourceID>& argumentsBuffer, const TupleFilter* & positiveTupleFilter, const TupleFilter* & negativeTupleFilter, const void* const tupleFilterContext, size_t* compiledConjunctIndexes);

template<class RT>
static std::unique_ptr<TupleIterator> createTupleIterator(const ArgumentIndexSet& variablesBoundSoFar, const std::vector<ResourceID>& defaultArgumentsBuffer, const Literal& literal, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, DataStore& dataStore, RT& resolver, ResourceValueCache& resourceValueCache, const TermArray& termArray, std::vector<ResourceID>& argumentsBuffer, const TupleFilter* & tupleFilter, const void* const tupleFilterContext) {
    switch (literal->getType()) {
    case ATOM_FORMULA:
        {
            // Add the constants to the tuple iterator, where constants are all arguments that are not variables.
            ArgumentIndexSet allInputArguments(variablesBoundSoFar);
            allInputArguments.intersectWith(literalVariables);
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
                if (!literalVariables.contains(*iterator))
                    allInputArguments.add(*iterator);
            return dataStore.getTupleTable(to_pointer_cast<Atom>(literal)->getPredicate()->getName()).createTupleIterator(argumentsBuffer, argumentIndexes, allInputArguments, allInputArguments, tupleFilter, tupleFilterContext);
        }
    case FILTER_FORMULA:
        {
            // All arguments of FILTER are variables.
            ArgumentIndexSet allInputArguments(variablesBoundSoFar);
            allInputArguments.intersectWith(literalVariables);
            std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluatorMain(BuiltinExpressionEvaluator::compile(nullptr, dataStore, resourceValueCache, termArray, allInputArguments, allInputArguments, argumentsBuffer, to_pointer_cast<Filter>(literal)->getBuiltinExpression()));
            return ::newFilterTupleIterator(std::move(builtinExpressionEvaluatorMain), nullptr, argumentsBuffer, argumentIndexes, allInputArguments, allInputArguments);
        }
    case BIND_FORMULA:
        {
            // All arguments of BIND are variables.
            ArgumentIndexSet allInputArguments(variablesBoundSoFar);
            allInputArguments.intersectWith(literalVariables);
            // If the bound term is a constant, then add it to the input arguments.
            if (to_pointer_cast<Bind>(literal)->getBoundTerm()->getType() != VARIABLE)
                allInputArguments.add(argumentIndexes[0]);
            std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluatorMain(BuiltinExpressionEvaluator::compile(nullptr, dataStore, resourceValueCache, termArray, allInputArguments, allInputArguments, argumentsBuffer, to_pointer_cast<Bind>(literal)->getBuiltinExpression()));
            return ::newBindTupleIterator<RT>(resolver, std::move(builtinExpressionEvaluatorMain), nullptr, argumentsBuffer, argumentIndexes, allInputArguments, allInputArguments);
        }
    case NEGATION_FORMULA:
        {
            // Add the constants to the tuple iterator, where constants are all arguments that are not variables.
            const std::vector<AtomicFormula>& atomicFormulas = to_reference_cast<Negation>(literal).getAtomicFormulas();
            ArgumentIndexSet initiallyBoundVariables(variablesBoundSoFar);
            initiallyBoundVariables.intersectWith(literalVariables);
            std::vector<Literal> underlyingConjunction;
            std::vector<ArgumentIndexSet> underlyingLiteralVariables;
            std::vector<std::vector<ArgumentIndex> > underlyingArgumentIndexes;
            for (auto atomicFormulaIterator = atomicFormulas.begin(); atomicFormulaIterator != atomicFormulas.end(); ++atomicFormulaIterator) {
                underlyingConjunction.push_back(*atomicFormulaIterator);
                underlyingLiteralVariables.emplace_back();
                underlyingArgumentIndexes.emplace_back();
                const std::vector<Term>& arguments = (*atomicFormulaIterator)->getArguments();
                for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator) {
                    const ArgumentIndex argumentIndex = termArray.getPosition(*argumentIterator);
                    if ((*argumentIterator)->getType() == VARIABLE)
                        underlyingLiteralVariables.back().add(argumentIndex);
                    underlyingArgumentIndexes.back().push_back(argumentIndex);
                }
            }
            return ::newNegationIterator(nullptr, compileConjunction<RT>(initiallyBoundVariables, defaultArgumentsBuffer, underlyingConjunction, underlyingLiteralVariables, underlyingArgumentIndexes, dataStore, resolver, resourceValueCache, termArray, argumentsBuffer, tupleFilter, tupleFilter, tupleFilterContext, nullptr));
        }
    case AGGREGATE_FORMULA:
        {
            Aggregate aggregate = static_pointer_cast<Aggregate>(literal);
            const std::vector<AtomicFormula>& atomicFormulas = aggregate->getAtomicFormulas();
            ArgumentIndexSet initiallyBoundVariables(variablesBoundSoFar);
            initiallyBoundVariables.intersectWith(literalVariables);
            std::vector<Literal> underlyingConjunction;
            std::vector<ArgumentIndexSet> underlyingLiteralVariables;
            std::vector<std::vector<ArgumentIndex> > underlyingArgumentIndexes;
            for (auto atomicFormulaIterator = atomicFormulas.begin(); atomicFormulaIterator != atomicFormulas.end(); ++atomicFormulaIterator) {
                underlyingConjunction.push_back(*atomicFormulaIterator);
                underlyingLiteralVariables.emplace_back();
                underlyingArgumentIndexes.emplace_back();
                const std::vector<Term>& arguments = (*atomicFormulaIterator)->getArguments();
                for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator) {
                    const ArgumentIndex argumentIndex = termArray.getPosition(*argumentIterator);
                    if ((*argumentIterator)->getType() == VARIABLE)
                        underlyingLiteralVariables.back().add(argumentIndex);
                    underlyingArgumentIndexes.back().push_back(argumentIndex);
                }
            }
            std::unique_ptr<TupleIterator> tupleIterator = compileConjunction<RT>(initiallyBoundVariables, defaultArgumentsBuffer, underlyingConjunction, underlyingLiteralVariables, underlyingArgumentIndexes, dataStore, resolver, resourceValueCache, termArray, argumentsBuffer, tupleFilter, tupleFilter, tupleFilterContext, nullptr);
            ArgumentIndexSet allInputArguments;
            allInputArguments.setToUnion(variablesBoundSoFar, tupleIterator->getAllArguments());
            std::vector<AggregateFunctionCallInfo> aggregateFunctionCallInfos;
            const std::vector<AggregateBind>& aggregateBinds = aggregate->getAggregateBinds();
            for (auto bindIterator = aggregateBinds.begin(); bindIterator != aggregateBinds.end(); ++bindIterator) {
                const std::vector<BuiltinExpression>& arguments = (*bindIterator)->getArguments();
                aggregateFunctionCallInfos.emplace_back(AggregateFunctionEvaluator::createAggregateFunctionEvaluator((*bindIterator)->getFunctionName(), arguments.size()), unique_ptr_vector<BuiltinExpressionEvaluator>());
                for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator)
                    aggregateFunctionCallInfos.back().second.push_back(BuiltinExpressionEvaluator::compile(nullptr, dataStore, resourceValueCache, termArray, allInputArguments, allInputArguments, argumentsBuffer, *argumentIterator));
            }
            return ::newAggregateTupleIterator<RT>(resolver, nullptr, argumentIndexes, allInputArguments, allInputArguments, std::move(tupleIterator), aggregateFunctionCallInfos);
        }
    default:
        UNREACHABLE;
    }
}

static size_t getCountEstimate(const ArgumentIndexSet& boundVariablesFoFar, DataStore& dataStore, const std::vector<ResourceID>& defaultArgumentsBuffer, const Literal& literal, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes) {
    switch (literal->getType()) {
    case ATOM_FORMULA:
        {
            ArgumentIndexSet atomConstants;
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
                if (literalVariables.contains(*iterator))
                    atomConstants.add(*iterator);
            return dataStore.getTupleTable(to_reference_cast<Atom>(literal).getPredicate()->getName()).getCountEstimate(ThreadContext::getCurrentThreadContext(), defaultArgumentsBuffer, argumentIndexes, atomConstants);
        }
    case FILTER_FORMULA:
        // All arguments are guaranteed to be variables, because that is how FILTER atoms are constructed.
        for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            if (!boundVariablesFoFar.contains(*iterator))
                return static_cast<size_t>(-1);
        return 1;
    case BIND_FORMULA:
        // All arguments are guaranteed to be variables, because that is how BIND atoms are constructed.
        for (auto iterator = argumentIndexes.begin() + 1; iterator != argumentIndexes.end(); ++iterator)
            if (!boundVariablesFoFar.contains(*iterator))
                return static_cast<size_t>(-1);
        return 1;
    case NEGATION_FORMULA:
        for (auto iterator = literalVariables.begin(); iterator != literalVariables.end(); ++iterator)
            if (!boundVariablesFoFar.contains(*iterator))
                return static_cast<size_t>(-1);
        return 1;
    case AGGREGATE_FORMULA:
        {
            // The group variables are placed before all other arguments in AGGREGATE atoms.
            const auto groupVariablesEnd = argumentIndexes.begin() + to_pointer_cast<Aggregate>(literal)->getGroupVariables().size();
            for (auto iterator = argumentIndexes.begin(); iterator != groupVariablesEnd; ++iterator)
                if (!boundVariablesFoFar.contains(*iterator))
                    return static_cast<size_t>(-1);
            return 1;
        }
    default:
        UNREACHABLE;
    }
}

static size_t getNextBodyLiteralIndex(const ArgumentIndexSet& boundVariablesSoFar, DataStore& dataStore, const std::vector<ResourceID>& defaultArgumentsBuffer, const std::vector<Literal>& conjunction, std::vector<bool>& usedConjuncts, const std::vector<ArgumentIndexSet>& literalVariables, const std::vector<std::vector<ArgumentIndex> >& argumentIndexes) {
    size_t nextConjunctIndex = static_cast<size_t>(-1);
    bool bestSoFarIsCrossProduct = true;
    size_t bestSoFarCountEstimate = 0xffffffffffffffffULL;
    const size_t numberOfConjuncts = conjunction.size();
    for (size_t conjunctIndex = 0; conjunctIndex < numberOfConjuncts; ++conjunctIndex) {
        if (!usedConjuncts[conjunctIndex]) {
            const bool isCrossProduct = boundVariablesSoFar.size() != 0 && literalVariables[conjunctIndex].hasEmptyIntersectionWith(boundVariablesSoFar);
            const size_t countEstimate = getCountEstimate(boundVariablesSoFar, dataStore, defaultArgumentsBuffer, conjunction[conjunctIndex], literalVariables[conjunctIndex], argumentIndexes[conjunctIndex]);
            if (countEstimate != static_cast<size_t>(-1) && (nextConjunctIndex == static_cast<size_t>(-1) || (bestSoFarIsCrossProduct && !isCrossProduct) || (!bestSoFarIsCrossProduct && !isCrossProduct && countEstimate < bestSoFarCountEstimate))) {
                nextConjunctIndex = conjunctIndex;
                bestSoFarIsCrossProduct = isCrossProduct;
                bestSoFarCountEstimate = countEstimate;
            }
        }
    }
    return nextConjunctIndex;
}

template<class RT>
static std::unique_ptr<TupleIterator> compileConjunction(const ArgumentIndexSet& initiallyBoundVariables, const std::vector<ResourceID>& defaultArgumentsBuffer, const std::vector<Literal>& conjunction, const std::vector<ArgumentIndexSet>& literalVariables, const std::vector<std::vector<ArgumentIndex> >& argumentIndexes, DataStore& dataStore, RT& resolver, ResourceValueCache& resourceValueCache, const TermArray& termArray, std::vector<ResourceID>& argumentsBuffer, const TupleFilter* & positiveTupleFilter, const TupleFilter* & negativeTupleFilter, const void* const tupleFilterContext, size_t* compiledConjunctIndexes) {
    ArgumentIndexSet boundVariablesSoFar(initiallyBoundVariables);
    unique_ptr_vector<TupleIterator> childIterators;
    if (conjunction.size() == 0) {
        ArgumentIndexSet noBindings;
        childIterators.push_back(::newEmptyTupleIterator(nullptr, argumentsBuffer, noBindings, noBindings));
    }
    else {
        size_t conjunctsToMatch = conjunction.size();
        std::vector<bool> usedConjuncts(conjunctsToMatch, false);
        while (conjunctsToMatch != 0) {
            const size_t nextConjunctIndex = getNextBodyLiteralIndex(boundVariablesSoFar, dataStore, defaultArgumentsBuffer, conjunction, usedConjuncts, literalVariables, argumentIndexes);
            if (nextConjunctIndex == static_cast<size_t>(-1)) {
                std::ostringstream message;
                message << "Cannot reorder conjuncts to satisfy the binding requirements.";
                throw RDF_STORE_EXCEPTION(message.str());
            }
            const FormulaType nextConjunctType = conjunction[nextConjunctIndex]->getType();
            childIterators.push_back(createTupleIterator<RT>(boundVariablesSoFar, defaultArgumentsBuffer, conjunction[nextConjunctIndex], literalVariables[nextConjunctIndex], argumentIndexes[nextConjunctIndex], dataStore, resolver, resourceValueCache, termArray, argumentsBuffer, nextConjunctType == NEGATION_FORMULA || nextConjunctType == AGGREGATE_FORMULA ? negativeTupleFilter : positiveTupleFilter, tupleFilterContext));
            boundVariablesSoFar.unionWith(literalVariables[nextConjunctIndex]);
            usedConjuncts[nextConjunctIndex] = true;
            if (compiledConjunctIndexes != nullptr)
                compiledConjunctIndexes[conjunction.size() - conjunctsToMatch] = nextConjunctIndex;
            --conjunctsToMatch;
        }
    }
    return ::newNestedIndexLoopJoinIterator(nullptr, CARDINALITY_NOT_EXACT, dataStore, argumentsBuffer, initiallyBoundVariables, initiallyBoundVariables, boundVariablesSoFar, boundVariablesSoFar, boundVariablesSoFar, std::move(childIterators));
}

template<class LiteralInfoIndexType, class PlanType>
static void compileForPivot(SmartPointer<typename PlanType::PivotLiteralInfoType> pivotLiteralInfo, RuleIndex& ruleIndex, LiteralInfoIndexType& literalInfoIndex, DataStore& dataStore, std::vector<ResourceID>& defaultArgumentsBuffer, ArgumentIndexSet& boundArgumentsTemporary, const std::vector<ArgumentIndexSet>& literalVariables, const std::vector<std::vector<ArgumentIndex> >& argumentIndexes, const std::vector<Literal>& conjunction, const size_t pivotLiteralIndex, std::vector<PlanType>& evaluationPlans, typename LiteralInfoIndexType::LiteralInfoType** literalInfosInOrder) {
    SmartPointer<typename LiteralInfoIndexType::LiteralInfoType> lastLiteralInfo(pivotLiteralInfo);
    size_t atomsToMatch = conjunction.size();
    std::vector<bool> usedLiterals(atomsToMatch, false);
    ArgumentIndexSet boundVariablesSoFar(literalVariables[pivotLiteralIndex]);
    usedLiterals[pivotLiteralIndex] = true;
    if (literalInfosInOrder != nullptr)
        literalInfosInOrder[pivotLiteralIndex] = lastLiteralInfo.get();
    const bool pivotLiteralIsPositive = (conjunction[pivotLiteralIndex]->getType() != NEGATION_FORMULA);
    --atomsToMatch;
    while (atomsToMatch != 0) {
        const size_t nextLiteralIndex = getNextBodyLiteralIndex(boundVariablesSoFar, dataStore, defaultArgumentsBuffer, conjunction, usedLiterals, literalVariables, argumentIndexes);
        if (nextLiteralIndex == static_cast<size_t>(-1)) {
            std::ostringstream message;
            message << "Cannot reorder atoms of the conjunction to satisfy the binding requirements.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        const bool nextLiteralIsPositive = (conjunction[nextLiteralIndex]->getType() != NEGATION_FORMULA);
        // All negative atoms are made after all positive atoms. Thus, unless negative atom is pivot, all negative atoms are after pivot, which is beneficial
        // since in such cases one does not need a "before pivot" filter for negative atoms.
        const LiteralPosition literalPosition = ((!pivotLiteralIsPositive && nextLiteralIsPositive) || (pivotLiteralIsPositive == nextLiteralIsPositive && nextLiteralIndex < pivotLiteralIndex)) ? BEFORE_PIVOT_ATOM : AFTER_PIVOT_ATOM;
        lastLiteralInfo = literalInfoIndex.getNonPivotLiteralInfo(ruleIndex, literalVariables[nextLiteralIndex], argumentIndexes[nextLiteralIndex], conjunction[nextLiteralIndex], lastLiteralInfo.get(), literalPosition);
        boundVariablesSoFar.unionWith(literalVariables[nextLiteralIndex]);
        usedLiterals[nextLiteralIndex] = true;
        if (literalInfosInOrder != nullptr)
            literalInfosInOrder[nextLiteralIndex] = lastLiteralInfo.get();
        --atomsToMatch;
    }
    ArgumentIndexSet noBindings;
    const size_t planCost = getCountEstimate(noBindings, dataStore, defaultArgumentsBuffer, conjunction[pivotLiteralIndex], literalVariables[pivotLiteralIndex], argumentIndexes[pivotLiteralIndex]);
    evaluationPlans.emplace_back(*pivotLiteralInfo, lastLiteralInfo, planCost);
}

// ApplicationManager

always_inline ApplicationManager::ApplicationManager(const TermArray& termArray, const Literal& literal, const ArgumentIndexSet* variablesBoundBefore) :
    m_compareVariables(),
    m_copyToBuffer()
{
    const size_t numberOfArguments = literal->getNumberOfArguments();
    std::unordered_map<Variable, size_t> unboundVariablesToFirstOccurrences;
    for (size_t index = 0; index < numberOfArguments; ++index) {
        const Term& argument = literal->getArgument(index);
        if (argument->getType() == VARIABLE) {
            const ArgumentIndex argumentIndex = termArray.getPosition(argument);
            if (variablesBoundBefore == nullptr || !variablesBoundBefore->contains(argumentIndex)) {
                const Variable variable = static_pointer_cast<Variable>(argument);
                std::unordered_map<Variable, size_t>::iterator iterator = unboundVariablesToFirstOccurrences.find(variable);
                if (iterator == unboundVariablesToFirstOccurrences.end()) {
                    unboundVariablesToFirstOccurrences[variable] = index;
                    m_copyToBuffer.push_back(ArgumentToBuffer(index, argumentIndex));
                }
                else
                    m_compareVariables.push_back(CompareVariables(iterator->second, index));
            }
        }
    }
}

// LiteralPattern

template<class ObjectType, class PatternType>
always_inline LiteralPattern<ObjectType, PatternType>::LiteralPattern(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal) :
    m_originalIndexingPattern(),
    m_currentIndexingPattern(),
    m_indexingPatternNumber(0),
    m_previousIndexed(nullptr),
    m_nextIndexed(nullptr),
    m_inByPatternIndex(false)
{
    const size_t numberOfArguments = literal->getNumberOfArguments();
    for (size_t index = 0; index < numberOfArguments; ++index) {
        m_indexingPatternNumber *= 2;
        const Term& argument = literal->getArgument(index);
        if (argument->getType() == VARIABLE) {
            m_originalIndexingPattern.push_back(INVALID_RESOURCE_ID);
            m_currentIndexingPattern.push_back(INVALID_RESOURCE_ID);
        }
        else {
            ++m_indexingPatternNumber;
            const ArgumentIndex argumentIndex = termArray.getPosition(argument);
            const ResourceID resourceID = defaultArgumentsBuffer[argumentIndex];
            m_originalIndexingPattern.push_back(resourceID);
            m_currentIndexingPattern.push_back(equalityManager.normalize(resourceID));
        }
    }
}

template<class ObjectType, class PatternType>
always_inline void LiteralPattern<ObjectType, PatternType>::ensureNormalized(const EqualityManager& equalityManager) {
    bool wasRemoved = false;
    const size_t indexingPatternSize = m_currentIndexingPattern.size();
    for (size_t index = 0; index < indexingPatternSize; ++index) {
        const ResourceID originalID = m_originalIndexingPattern[index];
        if (originalID != INVALID_RESOURCE_ID) {
            const ResourceID normalID = equalityManager.normalize(originalID);
            if (normalID != m_currentIndexingPattern[index]) {
                if (m_inByPatternIndex) {
                    static_cast<PatternType*>(this)->getLiteralPatternIndex().removeObject(static_cast<ObjectType*>(this));
                    wasRemoved = true;
                }
                m_currentIndexingPattern[index] = normalID;
            }
        }
    }
    if (wasRemoved)
        static_cast<PatternType*>(this)->getLiteralPatternIndex().addObject(static_cast<ObjectType*>(this));
}

// HeadAtomInfoPatternMain

always_inline HeadAtomInfoPatternMain::HeadAtomInfoPatternMain(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal) :
    LiteralPattern<HeadAtomInfo, HeadAtomInfoPatternMain>(defaultArgumentsBuffer, equalityManager, termArray, literal)
{
}

always_inline LiteralPatternIndex<HeadAtomInfo, HeadAtomInfoPatternMain>& HeadAtomInfoPatternMain::getLiteralPatternIndex() {
    return static_cast<HeadAtomInfo*>(this)->m_ruleInfo.m_ruleIndex.m_headAtomInfoByPatternIndexMain;
}

// PivotPositiveBodyLiteralInfoPatternMain

always_inline PivotPositiveBodyLiteralInfoPatternMain::PivotPositiveBodyLiteralInfoPatternMain(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal) :
    LiteralPattern<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternMain>(defaultArgumentsBuffer, equalityManager, termArray, literal)
{
}

always_inline LiteralPatternIndex<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternMain>& PivotPositiveBodyLiteralInfoPatternMain::getLiteralPatternIndex() {
    return static_cast<PivotPositiveBodyLiteralInfo*>(this)->m_ruleIndex.m_pivotPositiveBodyLiteralInfoByPatternIndexMain;
}

// PivotPositiveBodyLiteralInfoPatternIncremental

always_inline PivotPositiveBodyLiteralInfoPatternIncremental::PivotPositiveBodyLiteralInfoPatternIncremental(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal) :
    LiteralPattern<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternIncremental>(defaultArgumentsBuffer, equalityManager, termArray, literal)
{
}

always_inline LiteralPatternIndex<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternIncremental>& PivotPositiveBodyLiteralInfoPatternIncremental::getLiteralPatternIndex() {
    return static_cast<PivotPositiveBodyLiteralInfo*>(this)->m_ruleIndex.m_pivotPositiveBodyLiteralInfoByPatternIndexIncremental;
}

// PivotUnderlyingNegationLiteralInfoPatternMain

always_inline PivotUnderlyingNegationLiteralInfoPatternMain::PivotUnderlyingNegationLiteralInfoPatternMain(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal) :
    LiteralPattern<PivotUnderlyingNegationLiteralInfo, PivotUnderlyingNegationLiteralInfoPatternMain>(defaultArgumentsBuffer, equalityManager, termArray, literal)
{
}

always_inline LiteralPatternIndex<PivotUnderlyingNegationLiteralInfo, PivotUnderlyingNegationLiteralInfoPatternMain>& PivotUnderlyingNegationLiteralInfoPatternMain::getLiteralPatternIndex() {
    return static_cast<PivotUnderlyingNegationLiteralInfo*>(this)->m_ruleIndex.m_pivotUnderlyingNegationLiteralInfoByPatternIndexMain;
}

// LiteralInfoBase

template<class InfoType, class NonPivotInfoType>
always_inline void LiteralInfoBase<InfoType, NonPivotInfoType>::resizeComponentLevelFilters() {
    for (int index = 0; index < ALL_COMPONENTS; ++index)
        m_componentLevelFilters[index].reset(new uint8_t[m_ruleIndex.m_componentLevelFilterLength]);
}

template<class InfoType, class NonPivotInfoType>
always_inline void LiteralInfoBase<InfoType, NonPivotInfoType>::clearComponentLevelFilters() {
    for (int index = 0; index < ALL_COMPONENTS; ++index)
        ::memset(m_componentLevelFilters[index].get(), 0, m_ruleIndex.m_componentLevelFilterLength);
}

template<class InfoType, class NonPivotInfoType>
always_inline void LiteralInfoBase<InfoType, NonPivotInfoType>::addToComponentLevelFilter(const ComponentLevelFilter componentLevelFilter, const size_t componentLevel) {
    m_componentLevelFilters[componentLevelFilter][componentLevel >> 3] |= static_cast<uint8_t>(1) << (componentLevel & 0x7);
}

template<class InfoType, class NonPivotInfoType>
LiteralInfoBase<InfoType, NonPivotInfoType>::LiteralInfoBase(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const size_t hashCode) :
    m_ruleIndex(ruleIndex),
    m_referenceCount(0),
    m_literal(literal),
    m_hashCode(hashCode),
    m_firstChild(nullptr),
    m_argumentIndexes(argumentIndexes),
    m_variablesBoundAfterLiteral(),
    m_componentLevelFilters()
{
    m_variablesBoundAfterLiteral.unionWith(literalVariables);
    m_variablesBoundAfterLiteral.shrinkToFit();
    // Put all atoms initially in component 0. In this way we don't need to initialize component levels
    // in case of equality reasoning; othrwise, this will get rewritten appropriately before reasoning.
    for (int index = 0; index < ALL_COMPONENTS; ++index) {
        m_componentLevelFilters[index].reset(new uint8_t[m_ruleIndex.m_componentLevelFilterLength]);
        m_componentLevelFilters[index][0] = 1;
    }
}

// BodyLiteralInfo

always_inline void BodyLiteralInfo::addHeadAtomInfo(HeadAtomInfo& headAtomInfo) {
    m_lastLiteralForHeadAtoms.push_back(&headAtomInfo);
}

always_inline void BodyLiteralInfo::removeHeadAtomInfo(HeadAtomInfo& headAtomInfo) {
    std::vector<HeadAtomInfo*>::iterator iterator = m_lastLiteralForHeadAtoms.begin();
    while (iterator != m_lastLiteralForHeadAtoms.end()) {
        if (*iterator == &headAtomInfo)
            iterator = m_lastLiteralForHeadAtoms.erase(iterator);
        else
            ++iterator;
    }
}

BodyLiteralInfo::BodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const size_t hashCode) :
    LiteralInfoBase<BodyLiteralInfo, NonPivotBodyLiteralInfo>(ruleIndex, literalVariables, argumentIndexes, literal, hashCode),
    m_componentLevel(0),
    m_lastLiteralForHeadAtoms()
{
}

BodyLiteralInfo::~BodyLiteralInfo() {
    if (!m_ruleIndex.m_fastDestructors)
        m_ruleIndex.m_bodyLiteralInfoIndex.removeLiteralInfo(this);
}

// PivotPositiveBodyLiteralInfo

void PivotPositiveBodyLiteralInfo::setThreadCapacity(const size_t numberOfThreads) {
}

void PivotPositiveBodyLiteralInfo::ensureThreadReady(const size_t threadIndex) {
}

PivotPositiveBodyLiteralInfo::PivotPositiveBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, BodyLiteralInfo* const parent) :
    ApplicationManager(ruleIndex.m_termArray, literal, nullptr),
    PivotPositiveBodyLiteralInfoPatternMain(ruleIndex.m_defaultArgumentsBuffer, ruleIndex.m_dataStore.getEqualityManager(), ruleIndex.m_termArray, literal),
    PivotPositiveBodyLiteralInfoPatternIncremental(ruleIndex.m_defaultArgumentsBuffer, ruleIndex.m_provingEqualityManager, ruleIndex.m_termArray, literal),
    BodyLiteralInfo(ruleIndex, literalVariables, argumentIndexes, literal, hashCodeFor(nullptr, literal, PIVOT_ATOM))
{
    assert(literalPosition == PIVOT_ATOM);
    assert(parent == nullptr);
    assert(m_literal->getType() == ATOM_FORMULA);
    PivotPositiveBodyLiteralInfoPatternMain::getLiteralPatternIndex().addObject(this);
    PivotPositiveBodyLiteralInfoPatternIncremental::getLiteralPatternIndex().addObject(this);
}

PivotPositiveBodyLiteralInfo::~PivotPositiveBodyLiteralInfo() {
    if (!m_ruleIndex.m_fastDestructors) {
        PivotPositiveBodyLiteralInfoPatternMain::getLiteralPatternIndex().removeObject(this);
        PivotPositiveBodyLiteralInfoPatternIncremental::getLiteralPatternIndex().removeObject(this);
    }
}

BodyLiteralInfo* PivotPositiveBodyLiteralInfo::getParent() const {
    return nullptr;
}

LiteralPosition PivotPositiveBodyLiteralInfo::getLiteralPosition() const {
    return PIVOT_ATOM;
}

// PivotNegationBodyLiteralInfo::UnderlyingEvaluationPlan

always_inline UnderlyingNegationLiteralInfoIndex& PivotNegationBodyLiteralInfo::getUnderlyingLiteralInfoIndex(RuleIndex& ruleIndex) {
    return ruleIndex.m_underlyingNegationLiteralInfoIndex;
}

always_inline PivotNegationBodyLiteralInfo::UnderlyingEvaluationPlan::UnderlyingEvaluationPlan(PivotUnderlyingNegationLiteralInfo& pivotUnderlyingNegationLiteralInfo, SmartPointer<UnderlyingNegationLiteralInfo> lastUnderlyingNegationLiteralInfo, size_t cost) :
    m_pivotUnderlyingNegationLiteralInfo(pivotUnderlyingNegationLiteralInfo),
    m_lastUnderlyingNegationLiteralInfo(lastUnderlyingNegationLiteralInfo),
    m_cost(cost)
{
}

// PivotNegationBodyLiteralInfo

void PivotNegationBodyLiteralInfo::setThreadCapacity(const size_t numberOfThreads) {
    while (m_tupleIteratorsByThreadMain.size() < numberOfThreads)
        m_tupleIteratorsByThreadMain.emplace_back(nullptr);
    m_tupleIteratorsByThreadMain.erase(m_tupleIteratorsByThreadMain.begin() + numberOfThreads, m_tupleIteratorsByThreadMain.end());
}

void PivotNegationBodyLiteralInfo::ensureThreadReady(const size_t threadIndex) {
    if (m_tupleIteratorsByThreadMain[threadIndex].get() == nullptr) {
        CloneReplacements cloneReplacements;
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_resourceValueCache);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_mainNegativeBodyLiteralPivotOrAfterPivotFilter, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_mainNegativeBodyLiteralPivotOrAfterPivotFilter);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferMain);
        m_tupleIteratorsByThreadMain[threadIndex] = m_tupleIteratorsByThreadMain[0]->clone(cloneReplacements);
    }
}

PivotNegationBodyLiteralInfo::PivotNegationBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, BodyLiteralInfo* const parent) :
    BodyLiteralInfo(ruleIndex, literalVariables, argumentIndexes, literal, hashCodeFor(nullptr, literal, PIVOT_ATOM)),
    m_hasProjection(to_reference_cast<Negation>(m_literal).getNumberOfExistentialVariables() != 0),
    m_underlyingEvaluationPlans(),
    m_tupleIteratorsByThreadMain()
{
    assert(literalPosition == PIVOT_ATOM);
    assert(parent == nullptr);
    assert(m_literal->getType() == NEGATION_FORMULA);
    // Compile underlying atoms.
    TermArray& termArrayBody = m_ruleIndex.m_termArray;
    const std::vector<AtomicFormula>& atomicFormulas = to_reference_cast<Negation>(literal).getAtomicFormulas();
    std::vector<Literal> underlyingConjunction;
    std::vector<ArgumentIndexSet> underlyingLiteralVariables;
    std::vector<std::vector<ArgumentIndex> > underlyingArgumentIndexes;
    for (auto atomicFormulaIterator = atomicFormulas.begin(); atomicFormulaIterator != atomicFormulas.end(); ++atomicFormulaIterator) {
        underlyingConjunction.push_back(*atomicFormulaIterator);
        underlyingLiteralVariables.emplace_back();
        underlyingArgumentIndexes.emplace_back();
        const std::vector<Term>& arguments = (*atomicFormulaIterator)->getArguments();
        for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator) {
            const ArgumentIndex argumentIndex = termArrayBody.getPosition(*argumentIterator);
            if ((*argumentIterator)->getType() == VARIABLE)
                underlyingLiteralVariables.back().add(argumentIndex);
            underlyingArgumentIndexes.back().push_back(argumentIndex);
        }
    }
    ArgumentIndexSet boundArgumentsTemporary;
    for (size_t pivotUnderlyingLiteralIndex = 0; pivotUnderlyingLiteralIndex < underlyingConjunction.size(); ++pivotUnderlyingLiteralIndex) {
        const Literal& underlyingLiteral = underlyingConjunction[pivotUnderlyingLiteralIndex];
        if (underlyingLiteral->getType() == ATOM_FORMULA) {
            SmartPointer<PivotUnderlyingNegationLiteralInfo> pivotUnderlyingNegationLiteralInfo = m_ruleIndex.m_underlyingNegationLiteralInfoIndex.getPivotUnderlyingLiteralInfo(m_ruleIndex, underlyingLiteralVariables[pivotUnderlyingLiteralIndex], underlyingArgumentIndexes[pivotUnderlyingLiteralIndex], underlyingLiteral);
            compileForPivot<UnderlyingNegationLiteralInfoIndex, UnderlyingEvaluationPlan>(pivotUnderlyingNegationLiteralInfo, m_ruleIndex, m_ruleIndex.m_underlyingNegationLiteralInfoIndex, m_ruleIndex.m_dataStore, m_ruleIndex.m_defaultArgumentsBuffer, boundArgumentsTemporary, underlyingLiteralVariables, underlyingArgumentIndexes, underlyingConjunction, pivotUnderlyingLiteralIndex, m_underlyingEvaluationPlans, nullptr);
            m_underlyingEvaluationPlans.back().m_lastUnderlyingNegationLiteralInfo->addParentBodyLiteralInfo(*this);
        }
    }
    // Compile the tuple iterator for one-step rederivation.
    m_tupleIteratorsByThreadMain.push_back(createTupleIterator<ResourceValueCache>(literalVariables, m_ruleIndex.m_defaultArgumentsBuffer, m_literal, literalVariables, m_argumentIndexes, m_ruleIndex.m_dataStore, m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, m_ruleIndex.m_termArray, m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, m_ruleIndex.m_byThreadInfos[0]->m_mainNegativeBodyLiteralPivotOrAfterPivotFilter, this));
}

PivotNegationBodyLiteralInfo::~PivotNegationBodyLiteralInfo() {
    if (!m_ruleIndex.m_fastDestructors) {
        for (auto iterator = m_underlyingEvaluationPlans.begin(); iterator != m_underlyingEvaluationPlans.end(); ++iterator)
            iterator->m_lastUnderlyingNegationLiteralInfo->removeParentBodyLiteralInfo(*this);
    }
}

BodyLiteralInfo* PivotNegationBodyLiteralInfo::getParent() const {
    return nullptr;
}

LiteralPosition PivotNegationBodyLiteralInfo::getLiteralPosition() const {
    return PIVOT_ATOM;
}

void PivotNegationBodyLiteralInfo::normalizeConstantsMain(const EqualityManager& equalityManager) {
    for (auto iterator = m_underlyingEvaluationPlans.begin(); iterator != m_underlyingEvaluationPlans.end(); ++iterator)
        iterator->m_pivotUnderlyingNegationLiteralInfo.PivotUnderlyingNegationLiteralInfoPatternMain::ensureNormalized(equalityManager);
}

// NonPivotBodyLiteralInfo

always_inline const TupleFilter* & NonPivotBodyLiteralInfo::getTupleFilterMain(const size_t threadIndex) {
    RuleIndex::ByThreadInfo& byThreadInfo = *m_ruleIndex.m_byThreadInfos[threadIndex];
    const FormulaType formulaType = m_literal->getType();
    if (formulaType == NEGATION_FORMULA || formulaType == AGGREGATE_FORMULA) {
        if (m_literalPosition == BEFORE_PIVOT_ATOM) {
            if ((formulaType == NEGATION_FORMULA ? to_reference_cast<Negation>(m_literal).getNumberOfAtomicFormulas() : to_reference_cast<Aggregate>(m_literal).getNumberOfAtomicFormulas()) == 1)
                return byThreadInfo.m_mainNegativeBodyLiteralSingleAtomBeforePivotFilter;
            else
                return byThreadInfo.m_mainNegativeBodyLiteralMultipleAtomsBeforePivotFilter;
        }
        else
            return byThreadInfo.m_mainNegativeBodyLiteralPivotOrAfterPivotFilter;
    }
    else
        return m_literalPosition == BEFORE_PIVOT_ATOM ? byThreadInfo.m_mainPositiveBodyLiteralBeforePivotFilter : byThreadInfo.m_mainPositiveBodyLiteralAfterPivotFilter;
}

always_inline const TupleFilter* & NonPivotBodyLiteralInfo::getTupleFilterIncremental(const size_t threadIndex) {
    RuleIndex::ByThreadInfo& byThreadInfo = *m_ruleIndex.m_byThreadInfos[threadIndex];
    const FormulaType formulaType = m_literal->getType();
    if (formulaType == NEGATION_FORMULA || formulaType == AGGREGATE_FORMULA)
        return byThreadInfo.m_incrementalNegativeBodyLiteralFilterIncremental;
    else
        return m_literalPosition == BEFORE_PIVOT_ATOM ? byThreadInfo.m_incrementalPositiveBodyLiteralBeforePivotFilter : byThreadInfo.m_incrementalPositiveBodyLiteralAfterPivotFilter;
}

void NonPivotBodyLiteralInfo::setThreadCapacity(const size_t numberOfThreads) {
    while (m_tupleIteratorsByThreadMain.size() < numberOfThreads)
        m_tupleIteratorsByThreadMain.emplace_back(nullptr);
    m_tupleIteratorsByThreadMain.erase(m_tupleIteratorsByThreadMain.begin() + numberOfThreads, m_tupleIteratorsByThreadMain.end());
    while (m_tupleIteratorsByThreadIncremental.size() < numberOfThreads)
        m_tupleIteratorsByThreadIncremental.emplace_back(nullptr);
    m_tupleIteratorsByThreadIncremental.erase(m_tupleIteratorsByThreadIncremental.begin() + numberOfThreads, m_tupleIteratorsByThreadIncremental.end());
}

void NonPivotBodyLiteralInfo::ensureThreadReady(const size_t threadIndex) {
    if (m_tupleIteratorsByThreadMain[threadIndex].get() == nullptr) {
        CloneReplacements cloneReplacements;
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_resourceValueCache);
        cloneReplacements.registerReplacement(&getTupleFilterMain(0), &getTupleFilterMain(threadIndex));
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferMain);
        m_tupleIteratorsByThreadMain[threadIndex] = m_tupleIteratorsByThreadMain[0]->clone(cloneReplacements);
    }
    if (m_tupleIteratorsByThreadIncremental[threadIndex].get() == nullptr) {
        CloneReplacements cloneReplacements;
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_resourceValueCache);
        cloneReplacements.registerReplacement(&getTupleFilterIncremental(0), &getTupleFilterIncremental(threadIndex));
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferIncremental, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferIncremental);
        m_tupleIteratorsByThreadIncremental[threadIndex] = m_tupleIteratorsByThreadIncremental[0]->clone(cloneReplacements);
    }
}

NonPivotBodyLiteralInfo::NonPivotBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, BodyLiteralInfo* const parent) :
    BodyLiteralInfo(ruleIndex, literalVariables, argumentIndexes, literal, hashCodeFor(parent, literal, literalPosition)),
    m_literalPosition(literalPosition),
    m_parent(parent),
    m_nextSibling(m_parent->m_firstChild),
    m_previousSibling(nullptr),
    m_tupleIteratorsByThreadMain(),
    m_tupleIteratorsByThreadIncremental()
{
    if (m_nextSibling)
        m_nextSibling->m_previousSibling = this;
    m_parent->m_firstChild = this;
    m_variablesBoundAfterLiteral.unionWith(m_parent->m_variablesBoundAfterLiteral);
    m_variablesBoundAfterLiteral.shrinkToFit();
    m_tupleIteratorsByThreadMain.push_back(createTupleIterator<Dictionary>(m_parent->m_variablesBoundAfterLiteral, m_ruleIndex.m_defaultArgumentsBuffer, m_literal, literalVariables, argumentIndexes, m_ruleIndex.m_dataStore, m_ruleIndex.m_dataStore.getDictionary(), m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, m_ruleIndex.m_termArray, m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, getTupleFilterMain(0), this));
    m_tupleIteratorsByThreadIncremental.push_back(createTupleIterator<Dictionary>(m_parent->m_variablesBoundAfterLiteral, m_ruleIndex.m_defaultArgumentsBuffer, m_literal, literalVariables, argumentIndexes, m_ruleIndex.m_dataStore, m_ruleIndex.m_dataStore.getDictionary(), m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, m_ruleIndex.m_termArray, m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferIncremental, getTupleFilterIncremental(0), this));
    setThreadCapacity(m_ruleIndex.getCurrentNumberOfThreads());
}

NonPivotBodyLiteralInfo::~NonPivotBodyLiteralInfo() {
    if (!m_ruleIndex.m_fastDestructors) {
        if (m_nextSibling)
            m_nextSibling->m_previousSibling = m_previousSibling;
        if (m_previousSibling)
            m_previousSibling->m_nextSibling = m_nextSibling;
        else
            m_parent->m_firstChild = m_nextSibling;
    }
}

BodyLiteralInfo* NonPivotBodyLiteralInfo::getParent() const {
    return m_parent.get();
}

LiteralPosition NonPivotBodyLiteralInfo::getLiteralPosition() const {
    return m_literalPosition;
}

// UnderlyingLiteralInfo

template<class PBT>
always_inline void UnderlyingLiteralInfo<PBT>::addParentBodyLiteralInfo(ParentBodyType& parentBodyLiteralInfo) {
    m_lastLiteralForParentBodyLiterals.push_back(&parentBodyLiteralInfo);
}

template<class PBT>
always_inline void UnderlyingLiteralInfo<PBT>::removeParentBodyLiteralInfo(ParentBodyType& parentBodyLiteralInfo) {
    for (auto iterator = m_lastLiteralForParentBodyLiterals.begin(); iterator != m_lastLiteralForParentBodyLiterals.end();) {
        if (*iterator == &parentBodyLiteralInfo)
            iterator = m_lastLiteralForParentBodyLiterals.erase(iterator);
        else
            ++iterator;
    }
}

template<class PBT>
UnderlyingLiteralInfo<PBT>::UnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const size_t hashCode) :
    LiteralInfoBase<UnderlyingLiteralInfo<PBT>, NonPivotUnderlyingLiteralInfo<PBT> >(ruleIndex, literalVariables, argumentIndexes, literal, hashCode),
    m_lastLiteralForParentBodyLiterals()
{
}

template<class PBT>
UnderlyingLiteralInfo<PBT>::~UnderlyingLiteralInfo() {
    if (!this->m_ruleIndex.m_fastDestructors)
        PBT::getUnderlyingLiteralInfoIndex(this->m_ruleIndex).removeLiteralInfo(this);
}

// PivotUnderlyingLiteralInfo

template<class PBT>
void PivotUnderlyingLiteralInfo<PBT>::setThreadCapacity(const size_t numberOfThreads) {
}

template<class PBT>
void PivotUnderlyingLiteralInfo<PBT>::ensureThreadReady(const size_t threadIndex) {
}

template<class PBT>
PivotUnderlyingLiteralInfo<PBT>::PivotUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, UnderlyingLiteralInfoType* const parent) :
    ApplicationManager(ruleIndex.m_termArray, literal, nullptr),
    PivotUnderlyingNegationLiteralInfoPatternMain(ruleIndex.m_defaultArgumentsBuffer, ruleIndex.m_dataStore.getEqualityManager(), ruleIndex.m_termArray, literal),
    UnderlyingLiteralInfo<PBT>(ruleIndex, literalVariables, argumentIndexes, literal, this->hashCodeFor(nullptr, literal, PIVOT_ATOM))
{
    assert(literalPosition == PIVOT_ATOM);
    assert(parent == nullptr);
    assert(literal->getType() == ATOM_FORMULA);
    PBT::PivotUnderlyingLiteralPatternMain::getLiteralPatternIndex().addObject(this);
}

template<class PBT>
PivotUnderlyingLiteralInfo<PBT>::~PivotUnderlyingLiteralInfo() {
    if (!this->m_ruleIndex.m_fastDestructors)
        PBT::PivotUnderlyingLiteralPatternMain::getLiteralPatternIndex().removeObject(this);
}

template<class PBT>
typename PivotUnderlyingLiteralInfo<PBT>::UnderlyingLiteralInfoType* PivotUnderlyingLiteralInfo<PBT>::getParent() const {
    return nullptr;
}

template<class PBT>
LiteralPosition PivotUnderlyingLiteralInfo<PBT>::getLiteralPosition() const {
    return PIVOT_ATOM;
}

// NonPivotUnderlyingLiteralInfo

template<class PBT>
const TupleFilter* & NonPivotUnderlyingLiteralInfo<PBT>::getTupleFilter(const size_t threadIndex) {
    RuleIndex::ByThreadInfo& byThreadInfo = *this->m_ruleIndex.m_byThreadInfos[threadIndex];
    return m_literalPosition == BEFORE_PIVOT_ATOM ? byThreadInfo.m_underlyingLiteralBeforePivotFilter : byThreadInfo.m_underlyingLiteralAfterPivotFilter;
}

template<class PBT>
void NonPivotUnderlyingLiteralInfo<PBT>::setThreadCapacity(const size_t numberOfThreads) {
    while (m_tupleIteratorsByThread.size() < numberOfThreads)
        m_tupleIteratorsByThread.emplace_back(nullptr);
    m_tupleIteratorsByThread.erase(m_tupleIteratorsByThread.begin() + numberOfThreads, m_tupleIteratorsByThread.end());
}

template<class PBT>
void NonPivotUnderlyingLiteralInfo<PBT>::ensureThreadReady(const size_t threadIndex) {
    if (m_tupleIteratorsByThread[threadIndex].get() == nullptr) {
        CloneReplacements cloneReplacements;
        cloneReplacements.registerReplacement(&this->m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, &this->m_ruleIndex.m_byThreadInfos[threadIndex]->m_resourceValueCache);
        cloneReplacements.registerReplacement(&getTupleFilter(0), &getTupleFilter(threadIndex));
        cloneReplacements.registerReplacement(&this->m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, &this->m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferMain);
        m_tupleIteratorsByThread[threadIndex] = m_tupleIteratorsByThread[0]->clone(cloneReplacements);
    }
}

template<class PBT>
NonPivotUnderlyingLiteralInfo<PBT>::NonPivotUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, UnderlyingLiteralInfoType* const parent) :
    UnderlyingLiteralInfo<PBT>(ruleIndex, literalVariables, argumentIndexes, literal, this->hashCodeFor(parent, literal, literalPosition)),
    m_literalPosition(literalPosition),
    m_parent(parent),
    m_nextSibling(m_parent->m_firstChild),
    m_previousSibling(nullptr),
    m_tupleIteratorsByThread()
{
    if (m_nextSibling)
        m_nextSibling->m_previousSibling = this;
    m_parent->m_firstChild = this;
    this->m_variablesBoundAfterLiteral.unionWith(m_parent->m_variablesBoundAfterLiteral);
    this->m_variablesBoundAfterLiteral.shrinkToFit();
    m_tupleIteratorsByThread.push_back(createTupleIterator<Dictionary>(m_parent->m_variablesBoundAfterLiteral, this->m_ruleIndex.m_defaultArgumentsBuffer, this->m_literal, literalVariables, argumentIndexes, this->m_ruleIndex.m_dataStore, this->m_ruleIndex.m_dataStore.getDictionary(), this->m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, this->m_ruleIndex.m_termArray, this->m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, getTupleFilter(0), this));
    setThreadCapacity(this->m_ruleIndex.getCurrentNumberOfThreads());
}

template<class PBT>
NonPivotUnderlyingLiteralInfo<PBT>::~NonPivotUnderlyingLiteralInfo() {
    if (!this->m_ruleIndex.m_fastDestructors) {
        if (m_nextSibling)
            m_nextSibling->m_previousSibling = m_previousSibling;
        if (m_previousSibling)
            m_previousSibling->m_nextSibling = m_nextSibling;
        else
            m_parent->m_firstChild = m_nextSibling;
    }
}

template<class PBT>
typename NonPivotUnderlyingLiteralInfo<PBT>::UnderlyingLiteralInfoType* NonPivotUnderlyingLiteralInfo<PBT>::getParent() const {
    return m_parent.get();
}

template<class PBT>
LiteralPosition NonPivotUnderlyingLiteralInfo<PBT>::getLiteralPosition() const {
    return m_literalPosition;
}

// LiteralInfoIndexBase::LiteralInfoIndexPolicy

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::getBucketContents(const uint8_t* const bucket, typename LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::BucketContents& bucketContents) {
    bucketContents.m_literalInfo = const_cast<InfoType*>(*reinterpret_cast<const InfoType* const*>(bucket));
}

template<class InfoType>
always_inline BucketStatus LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::getBucketContentsStatus(const typename LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::BucketContents& bucketContents, const size_t valuesHashCode, const InfoType* parent, const Literal& literal, const LiteralPosition literalPosition) {
    if (bucketContents.m_literalInfo == nullptr)
        return BUCKET_EMPTY;
    else
        return bucketContents.m_literalInfo->getParent() == parent && bucketContents.m_literalInfo->getLiteral() == literal && bucketContents.m_literalInfo->getLiteralPosition() == literalPosition ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

template<class InfoType>
always_inline BucketStatus LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::getBucketContentsStatus(const typename LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::BucketContents& bucketContents, const size_t valuesHashCode, const InfoType* literalInfo) {
    if (bucketContents.m_literalInfo == nullptr)
        return BUCKET_EMPTY;
    else
        return bucketContents.m_literalInfo == literalInfo ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

template<class InfoType>
always_inline bool LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::setBucketContentsIfEmpty(uint8_t* const bucket, const typename LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::BucketContents& bucketContents) {
    if (*reinterpret_cast<InfoType**>(bucket) == nullptr) {
        setLiteralInfo(bucket, bucketContents.m_literalInfo);
        return true;
    }
    else
        return false;
}

template<class InfoType>
always_inline bool LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::isBucketContentsEmpty(const typename LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::BucketContents& bucketContents) {
    return bucketContents.m_literalInfo == nullptr;
}

template<class InfoType>
always_inline size_t LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::getBucketContentsHashCode(const typename LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::BucketContents& bucketContents) {
    return bucketContents.m_literalInfo->getHashCode();
}

template<class InfoType>
always_inline size_t LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::hashCodeFor(const InfoType* parent, const Literal& literal, const LiteralPosition literalPosition) {
    return InfoType::hashCodeFor(parent, literal, literalPosition);
}

template<class InfoType>
always_inline size_t LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::hashCodeFor(const InfoType* literalInfo) {
    return literalInfo->getHashCode();
}

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::makeBucketEmpty(uint8_t* const bucket) {
    *reinterpret_cast<InfoType**>(bucket) = nullptr;
}

template<class InfoType>
always_inline const InfoType* LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::getLiteralInfo(const uint8_t* const bucket) {
    return *reinterpret_cast<const InfoType* const*>(bucket);
}

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::LiteralInfoIndexPolicy::setLiteralInfo(uint8_t* const bucket, InfoType* const literalInfo) {
    *reinterpret_cast<InfoType**>(bucket) = literalInfo;
}

// LiteralInfoIndexBase

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::removeLiteralInfo(const InfoType* literalInfo) {
    typename SequentialHashTable<LiteralInfoIndexPolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_index.acquireBucket(threadContext, bucketDescriptor, literalInfo);
    m_index.continueBucketSearch(threadContext, bucketDescriptor, literalInfo);
    m_index.deleteBucket(threadContext, bucketDescriptor);
    m_index.releaseBucket(threadContext, bucketDescriptor);
}

template<class InfoType>
LiteralInfoIndexBase<InfoType>::LiteralInfoIndexBase(MemoryManager& memoryManager) :
    m_index(memoryManager, HASH_TABLE_LOAD_FACTOR)
{
}

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::initialize() {
    if (!m_index.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize LiteralInfoIndex.");
}

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::setThreadCapacity(const size_t numberOfThreads) {
    if (m_index.isInitialized()) {
        const uint8_t* bucket = m_index.getFirstBucket();
        while (bucket != m_index.getAfterLastBucket()) {
            InfoType* literalInfo = const_cast<InfoType*>(m_index.getPolicy().getLiteralInfo(bucket));
            if (literalInfo != nullptr)
                literalInfo->setThreadCapacity(numberOfThreads);
            bucket += m_index.getPolicy().BUCKET_SIZE;
        }
    }
}

template<class InfoType>
always_inline void LiteralInfoIndexBase<InfoType>::ensureThreadReady(const size_t threadIndex) {
    if (m_index.isInitialized()) {
        const uint8_t* bucket = m_index.getFirstBucket();
        while (bucket != m_index.getAfterLastBucket()) {
            InfoType* literalInfo = const_cast<InfoType*>(m_index.getPolicy().getLiteralInfo(bucket));
            if (literalInfo != nullptr)
                literalInfo->ensureThreadReady(threadIndex);
            bucket += m_index.getPolicy().BUCKET_SIZE;
        }
    }
}

template<class InfoType>
InfoType* LiteralInfoIndexBase<InfoType>::firstLiteralLocation(uint8_t* & location) {
    location = const_cast<uint8_t*>(m_index.getFirstBucket());
    while (location < m_index.getAfterLastBucket()) {
        InfoType* const literalInfo = const_cast<InfoType*>(LiteralInfoIndexPolicy::getLiteralInfo(location));
        if (literalInfo != nullptr)
            return literalInfo;
        location += LiteralInfoIndexPolicy::BUCKET_SIZE;
    }
    return nullptr;
}

template<class InfoType>
InfoType* LiteralInfoIndexBase<InfoType>::nextLiteralLocation(uint8_t* & location) {
    location += LiteralInfoIndexPolicy::BUCKET_SIZE;
    while (location < m_index.getAfterLastBucket()) {
        InfoType* const literalInfo = const_cast<InfoType*>(LiteralInfoIndexPolicy::getLiteralInfo(location));
        if (literalInfo != nullptr)
            return literalInfo;
        location += LiteralInfoIndexPolicy::BUCKET_SIZE;
    }
    return nullptr;
}

// BodyLiteralInfoIndex

template<class AT>
always_inline SmartPointer<AT> BodyLiteralInfoIndex::getBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, BodyLiteralInfo* const parent, const LiteralPosition literalPosition) {
    SequentialHashTable<LiteralInfoIndexPolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_index.acquireBucket(threadContext, bucketDescriptor, parent, literal, literalPosition);
    if (m_index.continueBucketSearch(threadContext, bucketDescriptor, parent, literal, literalPosition) == BUCKET_EMPTY) {
        bucketDescriptor.m_bucketContents.m_literalInfo = new AT(ruleIndex, literalVariables, argumentIndexes, literal, literalPosition, parent);
        m_index.getPolicy().setLiteralInfo(bucketDescriptor.m_bucket, bucketDescriptor.m_bucketContents.m_literalInfo);
        m_index.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    m_index.releaseBucket(threadContext, bucketDescriptor);
    return SmartPointer<AT>(static_cast<AT*>(bucketDescriptor.m_bucketContents.m_literalInfo));
}

BodyLiteralInfoIndex::BodyLiteralInfoIndex(MemoryManager& memoryManager) : LiteralInfoIndexBase<BodyLiteralInfo> (memoryManager) {
}

PivotPositiveBodyLiteralInfoPtr BodyLiteralInfoIndex::getPivotPositiveBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal) {
    return getBodyLiteralInfo<PivotPositiveBodyLiteralInfo>(ruleIndex, literalVariables, argumentIndexes, literal, nullptr, PIVOT_ATOM);
}

PivotNegationBodyLiteralInfoPtr BodyLiteralInfoIndex::getPivotNegationBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal) {
    return getBodyLiteralInfo<PivotNegationBodyLiteralInfo>(ruleIndex, literalVariables, argumentIndexes, literal, nullptr, PIVOT_ATOM);
}

NonPivotBodyLiteralInfoPtr BodyLiteralInfoIndex::getNonPivotLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, BodyLiteralInfo* const parent, const LiteralPosition literalPosition) {
    return getBodyLiteralInfo<NonPivotBodyLiteralInfo>(ruleIndex, literalVariables, argumentIndexes, literal, parent, literalPosition);
}

// UnderlyingLiteralInfoIndex

template<class PBT>
template<class AT>
always_inline SmartPointer<AT> UnderlyingLiteralInfoIndex<PBT>::getUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, UnderlyingLiteralInfoType* const parent, const LiteralPosition literalPosition) {
    typename SequentialHashTable<typename UnderlyingLiteralInfoIndex<PBT>::LiteralInfoIndexPolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    this->m_index.acquireBucket(threadContext, bucketDescriptor, parent, literal, literalPosition);
    if (this->m_index.continueBucketSearch(threadContext, bucketDescriptor, parent, literal, literalPosition) == BUCKET_EMPTY) {
        bucketDescriptor.m_bucketContents.m_literalInfo = new AT(ruleIndex, literalVariables, argumentIndexes, literal, literalPosition, parent);
        this->m_index.getPolicy().setLiteralInfo(bucketDescriptor.m_bucket, bucketDescriptor.m_bucketContents.m_literalInfo);
        this->m_index.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    this->m_index.releaseBucket(threadContext, bucketDescriptor);
    return SmartPointer<AT>(static_cast<AT*>(bucketDescriptor.m_bucketContents.m_literalInfo));
}

template<class PBT>
UnderlyingLiteralInfoIndex<PBT>::UnderlyingLiteralInfoIndex(MemoryManager& memoryManager) : LiteralInfoIndexBase<UnderlyingLiteralInfoType> (memoryManager) {
}

template<class PBT>
SmartPointer<typename UnderlyingLiteralInfoIndex<PBT>::PivotUnderlyingLiteralInfoType> UnderlyingLiteralInfoIndex<PBT>::getPivotUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal) {
    return getUnderlyingLiteralInfo<PivotUnderlyingNegationLiteralInfo>(ruleIndex, literalVariables, argumentIndexes, literal, nullptr, PIVOT_ATOM);
}

template<class PBT>
SmartPointer<typename UnderlyingLiteralInfoIndex<PBT>::NonPivotUnderlyingLiteralInfoType> UnderlyingLiteralInfoIndex<PBT>::getNonPivotLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, UnderlyingLiteralInfoType* const parent, const LiteralPosition literalPosition) {
    return getUnderlyingLiteralInfo<NonPivotUnderlyingNegationLiteralInfo>(ruleIndex, literalVariables, argumentIndexes, literal, parent, literalPosition);
}

// SupportingFactsEvaluator

SupportingFactsEvaluator::SupportingFactsEvaluator(ResourceValueCache& resourceValueCache, const ArgumentIndexSet& headVariables, DataStore& dataStore, const std::vector<ResourceID>& defaultArgumentsBuffer, const Rule& rule, const std::vector<ArgumentIndexSet>& literalVariables, const std::vector<std::vector<ArgumentIndex> >& argumentIndexes, const TermArray& termArray, const TupleFilter* & positiveTupleFilter, const TupleFilter* & negativeTupleFilter, size_t* compiledConjunctIndexes) :
    m_resourceValueCache(resourceValueCache),
    m_argumentsBuffer(defaultArgumentsBuffer),
    m_tupleIterator(compileConjunction<ResourceValueCache>(headVariables, defaultArgumentsBuffer, rule->getBody(), literalVariables, argumentIndexes, dataStore, m_resourceValueCache, m_resourceValueCache, termArray, m_argumentsBuffer, positiveTupleFilter, negativeTupleFilter, nullptr, compiledConjunctIndexes))
{
}

SupportingFactsEvaluator::SupportingFactsEvaluator(const SupportingFactsEvaluator& other) :
    m_resourceValueCache(other.m_resourceValueCache),
    m_argumentsBuffer(other.m_argumentsBuffer),
    m_tupleIterator(other.m_tupleIterator->clone(CloneReplacements().registerReplacement(&other.m_resourceValueCache, &m_resourceValueCache).registerReplacement(&other.m_argumentsBuffer, &m_argumentsBuffer)))
{
}

SupportingFactsEvaluator::~SupportingFactsEvaluator() {
}

// HeadAtomInfo

HeadAtomInfo::HeadAtomInfo(RuleInfo& ruleInfo, const size_t headAtomIndex, TermArray& termArraySupporting, const std::vector<ArgumentIndexSet>& literalVariablesSupporting, const std::vector<std::vector<ArgumentIndex> >& argumentIndexesSupporting) :
    ApplicationManager(ruleInfo.m_ruleIndex.m_termArray, ruleInfo.getRule()->getHead(headAtomIndex), nullptr),
    HeadAtomInfoPatternMain(ruleInfo.m_ruleIndex.m_defaultArgumentsBuffer, ruleInfo.m_ruleIndex.m_dataStore.getEqualityManager(), ruleInfo.m_ruleIndex.m_termArray, ruleInfo.getRule()->getHead(headAtomIndex)),
    m_ruleInfo(ruleInfo),
    m_headAtomIndex(headAtomIndex),
    m_componentLevel(0),
    m_recursive(true),
    m_headArgumentIndexes(),
    m_supportingFactsHeadArgumentIndexes(),
    m_supportingBodyLiteralInfosOrdered(),
    m_supportingFactsCopyToBuffer(),
    m_supportingFactsEvaluatorPrototype(),
    m_supportingFactsEvaluators()
{
    DataStore& dataStore = m_ruleInfo.m_ruleIndex.m_dataStore;
    TermArray& termArrayBody = m_ruleInfo.m_ruleIndex.m_termArray;

    // Index head atoms
    const std::vector<Term>& headAtomArguments = m_ruleInfo.getRule()->getHead(headAtomIndex)->getArguments();
    ArgumentIndexSet headVariablesSupporting;
    for (std::vector<Term>::const_iterator argumentIterator = headAtomArguments.begin(); argumentIterator != headAtomArguments.end(); ++argumentIterator) {
        const ArgumentIndex argumentIndexBody = termArrayBody.getPosition(*argumentIterator);
        const ArgumentIndex argumentIndexSupporting = termArraySupporting.getPosition(*argumentIterator);
        m_headArgumentIndexes.push_back(argumentIndexBody);
        m_supportingFactsHeadArgumentIndexes.push_back(argumentIndexSupporting);
        if ((*argumentIterator)->getType() == VARIABLE && headVariablesSupporting.add(argumentIndexSupporting))
            m_supportingFactsCopyToBuffer.emplace_back(argumentIterator - headAtomArguments.begin(), argumentIndexSupporting);
    }

    // Compile the supporting facts evaluator
    const size_t numberOfBodyLiterals = m_ruleInfo.m_rule->getNumberOfBodyLiterals();
    std::unique_ptr<size_t[]> compiledConjunctIndexes(new size_t[numberOfBodyLiterals]);
    m_supportingFactsEvaluatorPrototype.reset(new SupportingFactsEvaluator(m_ruleInfo.m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, headVariablesSupporting, dataStore, m_ruleInfo.m_supportingFactsDefaultArgumentsBuffer, m_ruleInfo.getRule(), literalVariablesSupporting, argumentIndexesSupporting, termArraySupporting, m_ruleInfo.m_ruleIndex.m_byThreadInfos[0]->m_supportingFactsPositiveFilter, m_ruleInfo.m_ruleIndex.m_byThreadInfos[0]->m_supportingFactsNegativeFilter, compiledConjunctIndexes.get()));
    dataStore.getEqualityManager().normalize(m_supportingFactsEvaluatorPrototype->m_argumentsBuffer, m_ruleInfo.m_supportingFactsConstantsIndexes);
    const size_t numberOfBodyLiteralsCompiled = m_supportingFactsEvaluatorPrototype->getNumberOfBodyLiterals();
    m_supportingBodyLiteralInfosOrdered.reset(new BodyLiteralInfo*[numberOfBodyLiteralsCompiled]);
    for (size_t compiledLiteralIndex = 0; compiledLiteralIndex < numberOfBodyLiteralsCompiled; ++compiledLiteralIndex) {
        const size_t literalIndexInRule = compiledConjunctIndexes[compiledLiteralIndex];
        if (literalIndexInRule >= numberOfBodyLiterals)
            m_supportingBodyLiteralInfosOrdered[compiledLiteralIndex] = nullptr;
        else
            m_supportingBodyLiteralInfosOrdered[compiledLiteralIndex] = m_ruleInfo.m_bodyAtomInfosInOrder[literalIndexInRule];
    }
}

always_inline void HeadAtomInfo::normalizeConstantsMain(const EqualityManager& equalityManager) {
    std::vector<ResourceID>& supportingFactsDefaultArgumentsBuffer = m_ruleInfo.m_supportingFactsDefaultArgumentsBuffer;
    std::vector<ArgumentIndex>& supportingFactsConstantsIndexes = m_ruleInfo.m_supportingFactsConstantsIndexes;
    equalityManager.normalize(supportingFactsDefaultArgumentsBuffer, m_supportingFactsEvaluatorPrototype->m_argumentsBuffer, supportingFactsConstantsIndexes);
    for (auto iterator = m_supportingFactsEvaluators.begin(); iterator != m_supportingFactsEvaluators.end(); ++iterator)
        equalityManager.normalize(supportingFactsDefaultArgumentsBuffer, (*iterator)->m_argumentsBuffer, supportingFactsConstantsIndexes);
    HeadAtomInfoPatternMain::ensureNormalized(equalityManager);
}

// RuleInfo::RuleConstant

always_inline RuleInfo::RuleConstant::RuleConstant(RuleInfo& forRule, const ResourceID resourceID, const EqualityManager& equalityManager, const EqualityManager& provingEqualityManager) :
    m_forRuleInfo(forRule),
    m_defaultID(resourceID),
    m_currentMainID(equalityManager.normalize(resourceID)),
    m_currentIncrementalID(provingEqualityManager.normalize(resourceID)),
    m_previous(nullptr),
    m_next(nullptr)
{
}

// RuleInfo::PivotPositiveEvaluationPlan

always_inline RuleInfo::PivotPositiveEvaluationPlan::PivotPositiveEvaluationPlan(PivotPositiveBodyLiteralInfo& pivotPositiveBodyLiteralInfo, BodyLiteralInfoPtr lastBodyLiteralInfo, size_t cost) :
    m_pivotPositiveBodyLiteralInfo(pivotPositiveBodyLiteralInfo),
    m_lastBodyLiteralInfo(lastBodyLiteralInfo),
    m_cost(cost)
{
}

// RuleInfo::PivotNegationEvaluationPlan

always_inline RuleInfo::PivotNegationEvaluationPlan::PivotNegationEvaluationPlan(PivotNegationBodyLiteralInfo& pivotNegationBodyLiteralInfo, BodyLiteralInfoPtr lastBodyLiteralInfo, size_t cost) :
    m_pivotNegationBodyLiteralInfo(pivotNegationBodyLiteralInfo),
    m_lastBodyLiteralInfo(lastBodyLiteralInfo),
    m_cost(cost)
{
}

// RuleInfo

always_inline void RuleInfo::ensureActive() {
    if (!m_active) {
        for (auto headAtomInfoIterator = m_headAtomInfos.begin(); headAtomInfoIterator != m_headAtomInfos.end(); ++headAtomInfoIterator) {
            HeadAtomInfo& headAtomInfo = **headAtomInfoIterator;
            for (auto plansIterator = m_pivotPositiveEvaluationPlans.begin(); plansIterator != m_pivotPositiveEvaluationPlans.end(); ++plansIterator)
                plansIterator->m_lastBodyLiteralInfo->addHeadAtomInfo(headAtomInfo);
            for (auto plansIterator = m_pivotNegationEvaluationPlans.begin(); plansIterator != m_pivotNegationEvaluationPlans.end(); ++plansIterator)
                plansIterator->m_lastBodyLiteralInfo->addHeadAtomInfo(headAtomInfo);
            m_ruleIndex.m_headAtomInfoByPatternIndexMain.addObject(&headAtomInfo);
        }
        if (m_ruleIndex.m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF)
            m_ruleIndex.m_dependencyGraph.addRuleInfo(*this);
        m_active = true;
    }
}

always_inline void RuleInfo::ensureInactive() {
    if (m_active) {
        if (m_ruleIndex.m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF)
            m_ruleIndex.m_dependencyGraph.removeRuleInfo(*this);
        for (auto headAtomInfoIterator = m_headAtomInfos.begin(); headAtomInfoIterator != m_headAtomInfos.end(); ++headAtomInfoIterator) {
            HeadAtomInfo& headAtomInfo = **headAtomInfoIterator;
            m_ruleIndex.m_headAtomInfoByPatternIndexMain.removeObject(&headAtomInfo);
            for (auto iterator = m_pivotPositiveEvaluationPlans.begin(); iterator != m_pivotPositiveEvaluationPlans.end(); ++iterator)
                iterator->m_lastBodyLiteralInfo->removeHeadAtomInfo(headAtomInfo);
            for (auto iterator = m_pivotNegationEvaluationPlans.begin(); iterator != m_pivotNegationEvaluationPlans.end(); ++iterator)
                iterator->m_lastBodyLiteralInfo->removeHeadAtomInfo(headAtomInfo);
        }
        m_active = false;
    }
}

void RuleInfo::resizeComponentLevelFilter() {
    m_componentLevelFilter.reset(new uint8_t[m_ruleIndex.m_componentLevelFilterLength]);
    m_componentLevelFilter[0] = 1;
}

void RuleInfo::updateComponentLevel() {
    ::memset(m_componentLevelFilter.get(), 0, m_ruleIndex.m_componentLevelFilterLength);
    for (auto iterator = m_headAtomInfos.begin(); iterator != m_headAtomInfos.end(); ++iterator) {
        HeadAtomInfo& headAtomInfo = **iterator;
        // A head atom is recrusive if there is a plan with a recursive pivot. We look at all plans to update all the body atoms appropriately;
        // but then, to determine whether a head atom is recursive, it suffices to just look at the pivot atom in each plan: we will look
        // through all positive atoms so we will determine the status of the head atom appropriately.
        headAtomInfo.m_recursive = false;
        const size_t componentLevel = headAtomInfo.getComponentLevel();
        m_componentLevelFilter[componentLevel >> 3] |= static_cast<uint8_t>(1) << (componentLevel & 0x7);
        PivotPositiveEvaluationPlan* bestNonrecursivePlan = nullptr;
        for (auto iterator = m_pivotPositiveEvaluationPlans.begin(); iterator != m_pivotPositiveEvaluationPlans.end(); ++iterator) {
            PivotPositiveEvaluationPlan& evaluationPlan = *iterator;
            const bool pivotLiteralIsRecursive = (evaluationPlan.m_pivotPositiveBodyLiteralInfo.getComponentLevel() == componentLevel);
            if (pivotLiteralIsRecursive)
                headAtomInfo.m_recursive = true;
            else if (bestNonrecursivePlan == nullptr || evaluationPlan.m_cost < bestNonrecursivePlan->m_cost)
                bestNonrecursivePlan = &evaluationPlan;
            for (BodyLiteralInfo* bodyLiteralInfo = evaluationPlan.m_lastBodyLiteralInfo.get(); bodyLiteralInfo != nullptr; bodyLiteralInfo = bodyLiteralInfo->getParent()) {
                bodyLiteralInfo->addToComponentLevelFilter(ALL_IN_COMPONENT, componentLevel);
                if (pivotLiteralIsRecursive)
                    bodyLiteralInfo->addToComponentLevelFilter(WITH_PIVOT_IN_COMPONENT, componentLevel);
            }
        }
        // If the head atom is not recursive, this means that the pivot atom in all positive pivot plans is from an eariler level.
        // Thus, we take the best nonrecusrive plan and declare one pivot atom to be in the component of the head atom.
        if (!headAtomInfo.m_recursive && bestNonrecursivePlan != nullptr)
            for (BodyLiteralInfo* bodyLiteralInfo = bestNonrecursivePlan->m_lastBodyLiteralInfo.get(); bodyLiteralInfo != nullptr; bodyLiteralInfo = bodyLiteralInfo->getParent())
                bodyLiteralInfo->addToComponentLevelFilter(WITH_PIVOT_IN_COMPONENT, componentLevel);
        // Finally, we process all plans with a negative pivot atom and add all the atoms to the component of the head atom.
        for (auto iterator = m_pivotNegationEvaluationPlans.begin(); iterator != m_pivotNegationEvaluationPlans.end(); ++iterator) {
            for (BodyLiteralInfo* bodyLiteralInfo = iterator->m_lastBodyLiteralInfo.get(); bodyLiteralInfo != nullptr; bodyLiteralInfo = bodyLiteralInfo->getParent())
                bodyLiteralInfo->addToComponentLevelFilter(ALL_IN_COMPONENT, componentLevel);
            std::vector<PivotNegationBodyLiteralInfo::UnderlyingEvaluationPlan>& underlyingEvaluationPlans = iterator->m_pivotNegationBodyLiteralInfo.m_underlyingEvaluationPlans;
            for (auto underlyingEvaluationPlanIterator = underlyingEvaluationPlans.begin(); underlyingEvaluationPlanIterator != underlyingEvaluationPlans.end(); ++underlyingEvaluationPlanIterator)
                for (UnderlyingNegationLiteralInfo* underlyingNegationLiteralInfo = underlyingEvaluationPlanIterator->m_lastUnderlyingNegationLiteralInfo.get(); underlyingNegationLiteralInfo != nullptr; underlyingNegationLiteralInfo = underlyingNegationLiteralInfo->getParent())
                    underlyingNegationLiteralInfo->addToComponentLevelFilter(ALL_IN_COMPONENT, componentLevel);
        }
        // Update ComponentLevelInfo
        RuleIndex::ComponentLevelInfo& componentLevelInfo = m_ruleIndex.m_componentLevelInfos[componentLevel];
        if (headAtomInfo.m_recursive)
            componentLevelInfo.m_hasRecursiveRules = true;
        else
            componentLevelInfo.m_hasNonrecursiveRules = true;
        if (isPivotless())
            componentLevelInfo.m_hasPivotlessRules = true;
        if (hasNegation())
            componentLevelInfo.m_hasRulesWithNegation = true;
        if (hasAggregation())
            componentLevelInfo.m_hasRulesWithAggregation = true;
    }
}

always_inline void RuleInfo::normalizeConstantsMain(const EqualityManager& equalityManager) {
    for (auto iterator = m_headAtomInfos.begin(); iterator != m_headAtomInfos.end(); ++iterator)
        (*iterator)->normalizeConstantsMain(equalityManager);
    for (auto iterator = m_pivotPositiveEvaluationPlans.begin(); iterator != m_pivotPositiveEvaluationPlans.end(); ++iterator)
        iterator->m_pivotPositiveBodyLiteralInfo.PivotPositiveBodyLiteralInfoPatternMain::ensureNormalized(equalityManager);
    for (auto iterator = m_pivotNegationEvaluationPlans.begin(); iterator != m_pivotNegationEvaluationPlans.end(); ++iterator)
        iterator->m_pivotNegationBodyLiteralInfo.normalizeConstantsMain(equalityManager);
}

always_inline void RuleInfo::normalizeConstantsIncremental(const EqualityManager& equalityManager) {
    for (auto iterator = m_pivotPositiveEvaluationPlans.begin(); iterator != m_pivotPositiveEvaluationPlans.end(); ++iterator)
        iterator->m_pivotPositiveBodyLiteralInfo.PivotPositiveBodyLiteralInfoPatternIncremental::ensureNormalized(equalityManager);
}

always_inline void RuleInfo::indexAtomicFormulaConstants(const AtomicFormula& atomicFormula, std::unordered_set<ResourceID>& ruleConstants) {
    EqualityManager& provingEqualityManager = m_ruleIndex.m_provingEqualityManager;
    DataStore& dataStore = m_ruleIndex.m_dataStore;
    RuleConstantIndex& ruleConstantIndex = m_ruleIndex.m_ruleConstantIndex;
    std::vector<ResourceID>& defaultArgumentsBufferBody = m_ruleIndex.m_defaultArgumentsBuffer;
    TermArray& termArrayBody = m_ruleIndex.m_termArray;
    const std::vector<Term>& arguments = atomicFormula->getArguments();
    for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator) {
        if ((*iterator)->getType() != VARIABLE) {
            const ArgumentIndex argumentIndexBody = termArrayBody.getPosition(*iterator);
            const ResourceID constantID = defaultArgumentsBufferBody[argumentIndexBody];
            if (ruleConstants.insert(constantID).second) {
                m_ruleConstants.push_back(std::unique_ptr<RuleConstant>(new RuleConstant(*this, constantID, dataStore.getEqualityManager(), provingEqualityManager)));
                ruleConstantIndex.addRuleConstant(*m_ruleConstants.back());
            }
        }
    }
}

RuleInfo::RuleInfo(RuleIndex& ruleIndex, const Rule& rule, const bool internalRule, const bool active, const bool justAdded, const bool justDeleted) :
    m_ruleIndex(ruleIndex),
    m_rule(rule),
    m_internalRule(internalRule),
    m_pivotless(true),
    m_hasNegation(false),
    m_hasAggregation(false),
    m_active(active),
    m_justAdded(justAdded),
    m_justDeleted(justDeleted),
    m_componentLevelFilter(new uint8_t[m_ruleIndex.m_componentLevelFilterLength]),
    m_nextRuleInfo(nullptr),
    m_previousRuleInfo(nullptr),
    m_ruleConstants(),
    m_pivotPositiveEvaluationPlans(),
    m_pivotNegationEvaluationPlans(),
    m_bodyAtomInfosInOrder(new BodyLiteralInfo*[m_rule->getNumberOfBodyLiterals()]),
    m_headAtomInfos(),
    m_ruleEvaluatorsByThreadMain(),
    m_ruleEvaluatorsByThreadIncremental(),
    m_supportingFactsVariableIndexes(),
    m_supportingFactsConstantsIndexes(),
    m_supportingFactsDefaultArgumentsBuffer()
{
    // Finish the initialization of the object
    m_componentLevelFilter[0] = 1;

    // These are just for easier access
    DataStore& dataStore = m_ruleIndex.m_dataStore;
    std::unordered_set<ResourceID> ruleConstants;
    const size_t numberOfHeadAtoms = m_rule->getNumberOfHeadAtoms();
    const std::vector<Literal>& bodyLiterals = m_rule->getBody();

    // Initialize *Body objects
    TermArray& termArrayBody = m_ruleIndex.m_termArray;
    std::vector<ResourceID>& defaultArgumentsBufferBody = m_ruleIndex.m_defaultArgumentsBuffer;
    std::vector<ArgumentIndexSet> literalVariablesBody;
    std::vector<std::vector<ArgumentIndex> > argumentIndexesBody;

    // Initialize *Supporting objects
    TermArray termArraySupporting(m_rule);
    for (ArgumentIndex argumentIndex = 0; static_cast<size_t>(argumentIndex) < termArraySupporting.getNumberOfTerms(); ++ argumentIndex) {
        const Term& term = termArraySupporting.getTerm(argumentIndex);
        const ResourceID resourceID = defaultArgumentsBufferBody[termArrayBody.getPosition(term)];
        m_supportingFactsDefaultArgumentsBuffer.push_back(resourceID);
        if (term->getType() == VARIABLE)
            m_supportingFactsVariableIndexes.push_back(std::make_pair(static_pointer_cast<Variable>(term), argumentIndex));
        else
            m_supportingFactsConstantsIndexes.push_back(argumentIndex);
    }
    std::vector<ArgumentIndexSet> literalVariablesSupporting;
    std::vector<std::vector<ArgumentIndex> > argumentIndexesSupporting;

    // Index body atoms
    for (auto bodyLiteralIterator = bodyLiterals.begin(); bodyLiteralIterator != bodyLiterals.end(); ++bodyLiteralIterator) {
        // Extend the *Body objects
        literalVariablesBody.emplace_back();
        argumentIndexesBody.emplace_back();
        // Extend the *Supporting objects
        literalVariablesSupporting.emplace_back();
        argumentIndexesSupporting.emplace_back();
        // Process the body literals
        const std::vector<Term>& bodyLiteralArguments = (*bodyLiteralIterator)->getArguments();
        for (auto argumentIterator = bodyLiteralArguments.begin(); argumentIterator != bodyLiteralArguments.end(); ++argumentIterator) {
            const ArgumentIndex argumentIndexBody = termArrayBody.getPosition(*argumentIterator);
            const ArgumentIndex argumentIndexSupporting = termArraySupporting.getPosition(*argumentIterator);
            argumentIndexesBody.back().push_back(argumentIndexBody);
            argumentIndexesSupporting.back().push_back(argumentIndexSupporting);
            if ((*argumentIterator)->getType() == VARIABLE) {
                literalVariablesBody.back().add(argumentIndexBody);
                literalVariablesSupporting.back().add(argumentIndexSupporting);
            }
        }
    }

    // Compile body atoms
    ArgumentIndexSet boundArgumentsTemporary;
    BodyLiteralInfo** bodyAtomInfosInOrder = m_bodyAtomInfosInOrder.get();
    for (std::vector<Literal>::const_iterator iterator = bodyLiterals.begin(); iterator != bodyLiterals.end(); ++iterator) {
        size_t pivotBodyLiteralIndex = iterator - bodyLiterals.begin();
        switch ((*iterator)->getType()) {
        case ATOM_FORMULA:
            {
                indexAtomicFormulaConstants(static_pointer_cast<AtomicFormula>(*iterator), ruleConstants);
                PivotPositiveBodyLiteralInfoPtr pivotPositiveBodyLiteralInfo = m_ruleIndex.m_bodyLiteralInfoIndex.getPivotPositiveBodyLiteralInfo(m_ruleIndex, literalVariablesBody[pivotBodyLiteralIndex], argumentIndexesBody[pivotBodyLiteralIndex], m_rule->getBody(pivotBodyLiteralIndex));
                compileForPivot<BodyLiteralInfoIndex, PivotPositiveEvaluationPlan>(pivotPositiveBodyLiteralInfo, m_ruleIndex, m_ruleIndex.m_bodyLiteralInfoIndex, m_ruleIndex.m_dataStore, m_ruleIndex.m_defaultArgumentsBuffer, boundArgumentsTemporary, literalVariablesBody, argumentIndexesBody, m_rule->getBody(), pivotBodyLiteralIndex, m_pivotPositiveEvaluationPlans, bodyAtomInfosInOrder);
                m_pivotless = false;
                bodyAtomInfosInOrder = nullptr;
            }
            break;
        case NEGATION_FORMULA:
            {
                const std::vector<AtomicFormula>& atomicFormulas = to_reference_cast<Negation>(*iterator).getAtomicFormulas();
                for (auto iterator = atomicFormulas.begin(); iterator != atomicFormulas.end(); ++iterator)
                    indexAtomicFormulaConstants(*iterator, ruleConstants);
                PivotNegationBodyLiteralInfoPtr pivotCompositeBodyLiteralInfo = m_ruleIndex.m_bodyLiteralInfoIndex.getPivotNegationBodyLiteralInfo(m_ruleIndex, literalVariablesBody[pivotBodyLiteralIndex], argumentIndexesBody[pivotBodyLiteralIndex], m_rule->getBody(pivotBodyLiteralIndex));
                compileForPivot<BodyLiteralInfoIndex, PivotNegationEvaluationPlan>(pivotCompositeBodyLiteralInfo, m_ruleIndex, m_ruleIndex.m_bodyLiteralInfoIndex, m_ruleIndex.m_dataStore, m_ruleIndex.m_defaultArgumentsBuffer,boundArgumentsTemporary, literalVariablesBody, argumentIndexesBody, m_rule->getBody(), pivotBodyLiteralIndex, m_pivotNegationEvaluationPlans, bodyAtomInfosInOrder);
                m_hasNegation = true;
                bodyAtomInfosInOrder = nullptr;
            }
            break;
        case AGGREGATE_FORMULA:
            {
                const std::vector<AtomicFormula>& atomicFormulas = to_reference_cast<Aggregate>(*iterator).getAtomicFormulas();
                for (auto iterator = atomicFormulas.begin(); iterator != atomicFormulas.end(); ++iterator)
                    indexAtomicFormulaConstants(*iterator, ruleConstants);
                m_hasAggregation = true;
            }
            break;
        default:
            break;
        }
    }
    // The following is possible if we did not have a pivot literal.
    if (bodyAtomInfosInOrder != nullptr)
        for (size_t bodyLiteralIndex = 0; bodyLiteralIndex < m_rule->getNumberOfBodyLiterals(); ++bodyLiteralIndex)
            bodyAtomInfosInOrder[bodyLiteralIndex] = nullptr;

    // Index head atoms after the body atoms so that m_bodyLiteralInfosInOrder is properly initialized
    for (size_t headAtomIndex = 0; headAtomIndex < numberOfHeadAtoms; ++headAtomIndex) {
        indexAtomicFormulaConstants(m_rule->getHead(headAtomIndex), ruleConstants);
        m_headAtomInfos.push_back(std::unique_ptr<HeadAtomInfo>(new HeadAtomInfo(*this, headAtomIndex, termArraySupporting, literalVariablesSupporting, argumentIndexesSupporting)));
    }

    // Compile the rule evaluators
    m_ruleEvaluatorsByThreadMain.push_back(compileConjunction<Dictionary>(ArgumentIndexSet(), defaultArgumentsBufferBody, m_rule->getBody(), literalVariablesBody, argumentIndexesBody, dataStore, m_ruleIndex.m_dataStore.getDictionary(), m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, m_ruleIndex.m_termArray, m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, m_ruleIndex.m_byThreadInfos[0]->m_mainRuleEvaluatorPositiveFilter, m_ruleIndex.m_byThreadInfos[0]->m_mainRuleEvaluatorNegativeFilter, nullptr, nullptr));
    CloneReplacements cloneReplacements;
    cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, &m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferIncremental);
    cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_mainRuleEvaluatorPositiveFilter, &m_ruleIndex.m_byThreadInfos[0]->m_incrementalRuleEvaluatorPositiveFilter);
    cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_mainRuleEvaluatorNegativeFilter, &m_ruleIndex.m_byThreadInfos[0]->m_incrementalRuleEvaluatorNegativeFilter);
    m_ruleEvaluatorsByThreadIncremental.push_back(m_ruleEvaluatorsByThreadMain[0]->clone(cloneReplacements));

    // Set the thread capacity
    setThreadCapacity(m_ruleIndex.getCurrentNumberOfThreads());

    // If m_active is true, we need to ensure that the rule is properly indexed. We do this using a mild hack
    // where we temporarily set if to false so that addToMain() correctly performs its work.
    if (m_active) {
        m_active = false;
        ensureActive();
    }
}

RuleInfo::~RuleInfo() {
    if (!m_ruleIndex.m_fastDestructors) {
        for (auto iterator = m_ruleConstants.begin(); iterator != m_ruleConstants.end(); ++iterator)
            m_ruleIndex.m_ruleConstantIndex.removeRuleConstant(**iterator);
        ensureInactive();
    }
}

always_inline void RuleInfo::setThreadCapacity(const size_t numberOfThreads) {
    while (m_ruleEvaluatorsByThreadMain.size() < numberOfThreads)
        m_ruleEvaluatorsByThreadMain.push_back(nullptr);
    m_ruleEvaluatorsByThreadMain.erase(m_ruleEvaluatorsByThreadMain.begin() + numberOfThreads, m_ruleEvaluatorsByThreadMain.end());
    while (m_ruleEvaluatorsByThreadIncremental.size() < numberOfThreads)
        m_ruleEvaluatorsByThreadIncremental.push_back(nullptr);
    m_ruleEvaluatorsByThreadIncremental.erase(m_ruleEvaluatorsByThreadIncremental.begin() + numberOfThreads, m_ruleEvaluatorsByThreadIncremental.end());
}

always_inline void RuleInfo::ensureThreadReady(const size_t threadIndex) {
    if (m_ruleEvaluatorsByThreadMain[threadIndex].get() == nullptr) {
        CloneReplacements cloneReplacements;
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_resourceValueCache);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferMain, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferMain);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_mainRuleEvaluatorPositiveFilter, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_mainRuleEvaluatorPositiveFilter);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_mainRuleEvaluatorNegativeFilter, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_mainRuleEvaluatorNegativeFilter);
        m_ruleEvaluatorsByThreadMain[threadIndex] = m_ruleEvaluatorsByThreadMain[0]->clone(cloneReplacements);
    }
    if (m_ruleEvaluatorsByThreadIncremental[threadIndex].get() == nullptr) {
        CloneReplacements cloneReplacements;
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_resourceValueCache, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_resourceValueCache);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_argumentBufferIncremental, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferIncremental);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_incrementalRuleEvaluatorPositiveFilter, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_incrementalRuleEvaluatorPositiveFilter);
        cloneReplacements.registerReplacement(&m_ruleIndex.m_byThreadInfos[0]->m_incrementalRuleEvaluatorNegativeFilter, &m_ruleIndex.m_byThreadInfos[threadIndex]->m_incrementalRuleEvaluatorNegativeFilter);
        m_ruleEvaluatorsByThreadIncremental[threadIndex] = m_ruleEvaluatorsByThreadIncremental[0]->clone(cloneReplacements);
    }
}

void RuleInfo::loadPivotPositiveEvaluationPlan(const size_t positivePlanIndex, std::vector<BodyLiteralInfo*>& rulePlan) const {
    rulePlan.clear();
    BodyLiteralInfo* bodyLiteralInfo = m_pivotPositiveEvaluationPlans[positivePlanIndex].m_lastBodyLiteralInfo.get();
    while (bodyLiteralInfo != nullptr) {
        rulePlan.insert(rulePlan.begin(), bodyLiteralInfo);
        bodyLiteralInfo = bodyLiteralInfo->getParent();
    }
}

void RuleInfo::loadPivotNegationEvaluationPlan(const size_t negationPlanIndex, std::vector<BodyLiteralInfo*>& rulePlan) const {
    rulePlan.clear();
    BodyLiteralInfo* bodyLiteralInfo = m_pivotNegationEvaluationPlans[negationPlanIndex].m_lastBodyLiteralInfo.get();
    while (bodyLiteralInfo != nullptr) {
        rulePlan.insert(rulePlan.begin(), bodyLiteralInfo);
        bodyLiteralInfo = bodyLiteralInfo->getParent();
    }

}

void RuleInfo::loadPivotNegationUnderlyingEvaluationPlan(const size_t negationPlanIndex, const size_t underlyingPlanIndex, std::vector<UnderlyingNegationLiteralInfo*>& underlyingNegationPlan) const {
    underlyingNegationPlan.clear();
    UnderlyingNegationLiteralInfo* underlyingNegationLiteralInfo = m_pivotNegationEvaluationPlans[negationPlanIndex].m_pivotNegationBodyLiteralInfo.m_underlyingEvaluationPlans[underlyingPlanIndex].m_lastUnderlyingNegationLiteralInfo.get();
    while (underlyingNegationLiteralInfo != nullptr) {
        underlyingNegationPlan.insert(underlyingNegationPlan.begin(), underlyingNegationLiteralInfo);
        underlyingNegationLiteralInfo = underlyingNegationLiteralInfo->getParent();
    }
}

// LiteralPatternIndex

template<class ObjectType, class PatternType>
always_inline LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndex(MemoryManager& memoryManager) : m_index(memoryManager, HASH_TABLE_LOAD_FACTOR) {
    for (size_t index = 0; index < 8; ++index)
        m_patternCounts[index] = 0;
}

template<class ObjectType, class PatternType>
always_inline void LiteralPatternIndex<ObjectType, PatternType>::initialize() {
    if (!m_index.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize LiteralPatternIndex.");
    for (size_t index = 0; index < 8; ++index)
        m_patternCounts[index] = 0;
}

template<class ObjectType, class PatternType>
always_inline void LiteralPatternIndex<ObjectType, PatternType>::addObject(ObjectType* object) {
    if (!object->PatternType::m_inByPatternIndex) {
        ++m_patternCounts[object->PatternType::m_indexingPatternNumber];
        typename SequentialHashTable<LiteralPatternIndexPolicy>::BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        m_index.acquireBucket(threadContext, bucketDescriptor, object);
        if (m_index.continueBucketSearch(threadContext, bucketDescriptor, object) == BUCKET_EMPTY)
            m_index.acknowledgeInsert(threadContext, bucketDescriptor);
        else
            bucketDescriptor.m_bucketContents.m_object->PatternType::m_previousIndexed = object;
        object->PatternType::m_previousIndexed = nullptr;
        object->PatternType::m_nextIndexed = bucketDescriptor.m_bucketContents.m_object;
        m_index.getPolicy().setObject(bucketDescriptor.m_bucket, object);
        m_index.releaseBucket(threadContext, bucketDescriptor);
        object->PatternType::m_inByPatternIndex = true;
    }
}

template<class ObjectType, class PatternType>
always_inline void LiteralPatternIndex<ObjectType, PatternType>::removeObject(ObjectType* object) {
    if (object->PatternType::m_inByPatternIndex) {
        --m_patternCounts[object->PatternType::m_indexingPatternNumber];
        if (object->PatternType::m_nextIndexed != nullptr)
            object->PatternType::m_nextIndexed->PatternType::m_previousIndexed = object->PatternType::m_previousIndexed;
        if (object->PatternType::m_previousIndexed != nullptr)
            object->PatternType::m_previousIndexed->PatternType::m_nextIndexed = object->PatternType::m_nextIndexed;
        else {
            typename SequentialHashTable<LiteralPatternIndexPolicy>::BucketDescriptor bucketDescriptor;
            ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
            m_index.acquireBucket(threadContext, bucketDescriptor, object);
            m_index.continueBucketSearch(threadContext, bucketDescriptor, object);
            if (object->PatternType::m_nextIndexed != nullptr)
                m_index.getPolicy().setObject(bucketDescriptor.m_bucket, object->PatternType::m_nextIndexed);
            else
                m_index.deleteBucket(threadContext, bucketDescriptor);
            m_index.releaseBucket(threadContext, bucketDescriptor);
        }
        object->PatternType::m_previousIndexed = nullptr;
        object->PatternType::m_nextIndexed = nullptr;
        object->PatternType::m_inByPatternIndex = false;
    }
}

// HeadAtomInfoByPatternIndex

template<class PatternType>
always_inline HeadAtomInfoByPatternIndex<PatternType>::HeadAtomInfoByPatternIndex(MemoryManager& memoryManager) :
    LiteralPatternIndex<HeadAtomInfo, PatternType>(memoryManager)
{
}

// PivotPositiveBodyLiteralInfoByPatternIndex

template<class PatternType>
always_inline PivotPositiveBodyLiteralInfoByPatternIndex<PatternType>::PivotPositiveBodyLiteralInfoByPatternIndex(MemoryManager& memoryManager) :
    LiteralPatternIndex<PivotPositiveBodyLiteralInfo, PatternType>(memoryManager)
{
}

// PivotUnderlyingLiteralInfoByPatternIndex

template<class PatternType>
always_inline PivotUnderlyingLiteralInfoByPatternIndex<PatternType>::PivotUnderlyingLiteralInfoByPatternIndex(MemoryManager& memoryManager) :
    LiteralPatternIndex<typename PatternType::PivotUnderlyingLiteralInfoType, PatternType>(memoryManager)
{
}

// RuleConstantIndex::Entry

always_inline RuleConstantIndex::Entry::Entry() : m_firstRuleConstant(nullptr) {
    // We deliberately do not initialize m_argumentsBufferIndex as that will be done in addConstant()
}

// RuleConstantIndex

always_inline RuleConstantIndex::RuleConstantIndex() : m_index() {
}

always_inline void RuleConstantIndex::initialize() {
    m_index.clear();
}

always_inline void RuleConstantIndex::newConstant(const ResourceID resourceID, const ArgumentIndex argumentsBufferIndex) {
    assert(m_index.find(resourceID) == m_index.end());
    m_index[resourceID].m_argumentsBufferIndex = argumentsBufferIndex;
}

always_inline bool RuleConstantIndex::getConstant(const ResourceID resourceID, ArgumentIndex& argumentsBufferIndex, RuleInfo::RuleConstant* & firstRuleConstant) const {
    std::unordered_map<ResourceID, Entry>::const_iterator iterator = m_index.find(resourceID);
    if (iterator == m_index.end())
        return false;
    else {
        argumentsBufferIndex = iterator->second.m_argumentsBufferIndex;
        firstRuleConstant = iterator->second.m_firstRuleConstant;
        return true;
    }
}

always_inline void RuleConstantIndex::addRuleConstant(RuleInfo::RuleConstant& ruleConstant) {
    assert(m_index.find(ruleConstant.m_defaultID) != m_index.end());
    Entry& entry = m_index[ruleConstant.m_defaultID];
    ruleConstant.m_previous = nullptr;
    ruleConstant.m_next = entry.m_firstRuleConstant;
    entry.m_firstRuleConstant = &ruleConstant;
}

always_inline void RuleConstantIndex::removeRuleConstant(RuleInfo::RuleConstant& ruleConstant) {
    if (ruleConstant.m_previous != nullptr)
        ruleConstant.m_previous->m_next = ruleConstant.m_next;
    else {
        assert(m_index.find(ruleConstant.m_defaultID) != m_index.end());
        m_index[ruleConstant.m_defaultID].m_firstRuleConstant = ruleConstant.m_next;
    }
    if (ruleConstant.m_next != nullptr)
        ruleConstant.m_next->m_previous = ruleConstant.m_previous;
    ruleConstant.m_previous = ruleConstant.m_next = nullptr;
}

// RuleIndex::ByThreadInfo

always_inline RuleIndex::ByThreadInfo::ByThreadInfo(const Dictionary& dictionary, MemoryManager& memoryManager, const std::vector<ResourceID>& defaultArgumentsBuffer) :
    m_resourceValueCache(dictionary, memoryManager),
    m_argumentBufferMain(defaultArgumentsBuffer),
    m_argumentBufferIncremental(defaultArgumentsBuffer),
    m_mainPositiveBodyLiteralBeforePivotFilter(nullptr),
    m_mainPositiveBodyLiteralAfterPivotFilter(nullptr),
    m_mainNegativeBodyLiteralSingleAtomBeforePivotFilter(nullptr),
    m_mainNegativeBodyLiteralMultipleAtomsBeforePivotFilter(nullptr),
    m_mainNegativeBodyLiteralPivotOrAfterPivotFilter(nullptr),
    m_mainRuleEvaluatorPositiveFilter(nullptr),
    m_mainRuleEvaluatorNegativeFilter(nullptr),
    m_incrementalPositiveBodyLiteralBeforePivotFilter(nullptr),
    m_incrementalPositiveBodyLiteralAfterPivotFilter(nullptr),
    m_incrementalNegativeBodyLiteralFilterIncremental(nullptr),
    m_incrementalRuleEvaluatorPositiveFilter(nullptr),
    m_incrementalRuleEvaluatorNegativeFilter(nullptr),
    m_supportingFactsPositiveFilter(nullptr),
    m_supportingFactsNegativeFilter(nullptr),
    m_underlyingLiteralBeforePivotFilter(nullptr),
    m_underlyingLiteralAfterPivotFilter(nullptr)
{
}

// RuleIndex::ComponentLevelInfo

always_inline RuleIndex::ComponentLevelInfo::ComponentLevelInfo() :
    m_hasRulesWithNegation(false),
    m_hasRulesWithAggregation(false),
    m_hasPivotlessRules(false),
    m_hasNonrecursiveRules(false),
    m_hasRecursiveRules(false)
{
}

always_inline void RuleIndex::ComponentLevelInfo::reset() {
    m_hasRulesWithNegation = m_hasRulesWithAggregation = m_hasPivotlessRules = m_hasNonrecursiveRules = m_hasRecursiveRules = false;
}

// RuleIndex

void RuleIndex::addRuleInfo(const Rule& rule, const bool isInternalRule, const bool inMain, const bool justAdded, const bool justDeleted) {
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_termArray.visitFormula(rule);
    while (m_defaultArgumentsBuffer.size() < m_termArray.getNumberOfTerms()) {
        const ArgumentIndex argumentIndex = static_cast<ArgumentIndex>(m_defaultArgumentsBuffer.size());
        const Term& term = m_termArray.getTerm(argumentIndex);
        ResourceID resourceID;
        switch (term->getType()) {
        case RESOURCE_BY_ID:
            resourceID = to_reference_cast<ResourceByID>(term).getResourceID();
            break;
        case RESOURCE_BY_NAME:
            resourceID = m_dataStore.getDictionary().resolveResource(threadContext, nullptr, to_reference_cast<ResourceByName>(term).getResourceText());
            break;
        case VARIABLE:
        default:
            resourceID = INVALID_RESOURCE_ID;
            break;
        }
        if (term->getType() != VARIABLE)
            m_ruleConstantIndex.newConstant(resourceID, argumentIndex);
        const ResourceID normalizedMainResourceID = m_equalityManager.normalize(resourceID);
        const ResourceID normalizedIncrementalResourceID = m_provingEqualityManager.normalize(resourceID);
        for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator) {
            assert((*iterator)->m_argumentBufferMain.size() == m_defaultArgumentsBuffer.size());
            assert((*iterator)->m_argumentBufferIncremental.size() == m_defaultArgumentsBuffer.size());
            (*iterator)->m_argumentBufferMain.push_back(normalizedMainResourceID);
            (*iterator)->m_argumentBufferIncremental.push_back(normalizedIncrementalResourceID);
        }
        m_defaultArgumentsBuffer.push_back(resourceID);
    }
    std::unique_ptr<RuleInfo>& ruleInfo = m_ruleInfosByRule[rule];
    try {
        ruleInfo.reset(new RuleInfo(*this, rule, isInternalRule, inMain, justAdded, justDeleted));
    }
    catch (...) {
        std::vector<std::exception_ptr> causes;
        causes.push_back(std::current_exception());
        std::ostringstream message;
        message << "Error while compiling rule:" << std::endl << "        " << rule->toString(Prefixes::s_defaultPrefixes);
        throw RDF_STORE_EXCEPTION_WITH_CAUSES(message.str(), causes);
    }
    if (m_lastRuleInfo == nullptr)
        m_firstRuleInfo = ruleInfo.get();
    else
        m_lastRuleInfo->m_nextRuleInfo = ruleInfo.get();
    ruleInfo->m_previousRuleInfo = m_lastRuleInfo;
    m_lastRuleInfo = ruleInfo.get();
    if (ruleInfo->m_pivotless)
        ++m_numberOfPivotlessRules;
    if (ruleInfo->m_hasNegation)
        ++m_numberOfRulesWithNegation;
    if (ruleInfo->m_hasAggregation)
        ++m_numberOfRulesWithAggregation;
    if (ruleInfo->m_justAdded)
        ++m_numberOfJustAddedRuleInfos;
    if (ruleInfo->m_justDeleted)
        ++m_numberOfJustDeletedRuleInfos;
}

always_inline size_t RuleIndex::getCurrentNumberOfThreads() const {
    return m_byThreadInfos.size();
}

void RuleIndex::updateDependencyGraph() {
    if (m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF && m_dependencyGraph.updateComponents()) {
        const size_t maxComponentLevel = m_dependencyGraph.getMaxComponentLevel();
        while (m_componentLevelInfos.size() <= maxComponentLevel)
            m_componentLevelInfos.emplace_back();
        m_componentLevelInfos.erase(m_componentLevelInfos.begin() + maxComponentLevel + 1, m_componentLevelInfos.end());
        for (auto iterator = m_componentLevelInfos.begin(); iterator != m_componentLevelInfos.end(); ++iterator)
            iterator->reset();
        const size_t requiredComponentLevelFilterLength = (maxComponentLevel / sizeof(uint8_t)) + 1;
        assert(requiredComponentLevelFilterLength != 0);
        uint8_t* literalLocation;
        if (requiredComponentLevelFilterLength < m_componentLevelFilterLength) {
            m_componentLevelFilterLength = requiredComponentLevelFilterLength;
            for (BodyLiteralInfo* bodyLiteralInfo = m_bodyLiteralInfoIndex.firstLiteralLocation(literalLocation); bodyLiteralInfo != nullptr; bodyLiteralInfo = m_bodyLiteralInfoIndex.nextLiteralLocation(literalLocation))
                bodyLiteralInfo->resizeComponentLevelFilters();
            for (UnderlyingNegationLiteralInfo* underlyingNegationLiteralInfo = m_underlyingNegationLiteralInfoIndex.firstLiteralLocation(literalLocation); underlyingNegationLiteralInfo != nullptr; underlyingNegationLiteralInfo = m_underlyingNegationLiteralInfoIndex.nextLiteralLocation(literalLocation))
                underlyingNegationLiteralInfo->resizeComponentLevelFilters();
            RuleInfo* ruleInfo = m_firstRuleInfo;
            while (ruleInfo != nullptr) {
                ruleInfo->resizeComponentLevelFilter();
                ruleInfo = ruleInfo->m_nextRuleInfo;
            }
        }
        for (BodyLiteralInfo* bodyLiteralInfo = m_bodyLiteralInfoIndex.firstLiteralLocation(literalLocation); bodyLiteralInfo != nullptr; bodyLiteralInfo = m_bodyLiteralInfoIndex.nextLiteralLocation(literalLocation))
            bodyLiteralInfo->clearComponentLevelFilters();
        for (UnderlyingNegationLiteralInfo* underlyingNegationLiteralInfo = m_underlyingNegationLiteralInfoIndex.firstLiteralLocation(literalLocation); underlyingNegationLiteralInfo != nullptr; underlyingNegationLiteralInfo = m_underlyingNegationLiteralInfoIndex.nextLiteralLocation(literalLocation))
            underlyingNegationLiteralInfo->clearComponentLevelFilters();
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            ruleInfo->updateComponentLevel();
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
    }
}

RuleIndex::RuleIndex(DataStore& dataStore) :
    m_dataStore(dataStore),
    m_equalityManager(m_dataStore.getEqualityManager()),
    m_provingEqualityManager(m_dataStore.getMemoryManager()),
    m_fastDestructors(false),
    m_termArray(),
    m_logicFactory(::newLogicFactory()),
    m_defaultArgumentsBuffer(),
    m_byThreadInfos(),
    m_dependencyGraph(m_termArray, m_dataStore.getMemoryManager()),
    m_componentLevelFilterLength(1),
    m_componentLevelInfos(1),
    m_bodyLiteralInfoIndex(m_dataStore.getMemoryManager()),
    m_underlyingNegationLiteralInfoIndex(m_dataStore.getMemoryManager()),
    m_headAtomInfoByPatternIndexMain(m_dataStore.getMemoryManager()),
    m_pivotPositiveBodyLiteralInfoByPatternIndexMain(m_dataStore.getMemoryManager()),
    m_pivotUnderlyingNegationLiteralInfoByPatternIndexMain(m_dataStore.getMemoryManager()),
    m_pivotPositiveBodyLiteralInfoByPatternIndexIncremental(m_dataStore.getMemoryManager()),
    m_ruleConstantIndex(),
    m_ruleInfosByRule(),
    m_firstRuleInfo(nullptr),
    m_lastRuleInfo(nullptr),
    m_numberOfPivotlessRules(0),
    m_numberOfRulesWithNegation(0),
    m_numberOfRulesWithAggregation(0),
    m_numberOfJustAddedRuleInfos(0),
    m_numberOfJustDeletedRuleInfos(0)
{
}

RuleIndex::~RuleIndex() {
    m_fastDestructors = true;
}

void RuleIndex::initialize() {
    // The order of the following calls is the same as in the destructor of RuleIndex, which is critical for correct destruction.
    m_fastDestructors = true;
    m_numberOfPivotlessRules = 0;
    m_numberOfRulesWithAggregation = 0;
    m_numberOfRulesWithNegation = 0;
    m_numberOfJustDeletedRuleInfos = 0;
    m_numberOfJustAddedRuleInfos = 0;
    m_lastRuleInfo = nullptr;
    m_firstRuleInfo = nullptr;
    m_ruleInfosByRule.clear();
    m_ruleConstantIndex.initialize();
    m_pivotPositiveBodyLiteralInfoByPatternIndexIncremental.initialize();
    m_pivotUnderlyingNegationLiteralInfoByPatternIndexMain.initialize();
    m_pivotPositiveBodyLiteralInfoByPatternIndexMain.initialize();
    m_headAtomInfoByPatternIndexMain.initialize();
    m_underlyingNegationLiteralInfoIndex.initialize();
    m_bodyLiteralInfoIndex.initialize();
    if (m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF) {
        m_dependencyGraph.initialize();
        m_componentLevelFilterLength = 1;
        m_componentLevelInfos.clear();
        m_componentLevelInfos.emplace_back();
    }
    for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator) {
        (*iterator)->m_resourceValueCache.clear();
        (*iterator)->m_argumentBufferMain.clear();
        (*iterator)->m_argumentBufferIncremental.clear();
    }
    m_defaultArgumentsBuffer.clear();
    m_termArray.clear();
    if (!m_provingEqualityManager.initialize())
        throw RDF_STORE_EXCEPTION("Cannot initialize proving equality manager.");
    m_fastDestructors = false;
}

const RuleInfo* RuleIndex::getRuleInfoFor(const Rule& rule) const {
    const Rule ownRule = rule->clone(m_logicFactory);
    std::unordered_map<Rule, std::unique_ptr<RuleInfo> >::const_iterator iterator = m_ruleInfosByRule.find(ownRule);
    if (iterator == m_ruleInfosByRule.end())
        return nullptr;
    else
        return iterator->second.get();
}

void RuleIndex::forgetTemporaryConstants() {
    for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator)
        (*iterator)->m_resourceValueCache.clear();
}

void RuleIndex::resetProving() {
    if (!m_provingEqualityManager.initialize())
        throw RDF_STORE_EXCEPTION("Cannot initialize proving equality manager.");
    ensureConstantsAreNormalizedIncremental();
}

void RuleIndex::setThreadCapacity(const size_t numberOfThreads) {
    while (m_byThreadInfos.size() < numberOfThreads)
        m_byThreadInfos.push_back(std::unique_ptr<ByThreadInfo>(new ByThreadInfo(m_dataStore.getDictionary(), m_dataStore.getMemoryManager(), m_defaultArgumentsBuffer)));
    m_byThreadInfos.erase(m_byThreadInfos.begin() + numberOfThreads, m_byThreadInfos.end());
    m_bodyLiteralInfoIndex.setThreadCapacity(numberOfThreads);
    m_underlyingNegationLiteralInfoIndex.setThreadCapacity(numberOfThreads);
    RuleInfo* ruleInfo = m_firstRuleInfo;
    while (ruleInfo != nullptr) {
        ruleInfo->setThreadCapacity(numberOfThreads);
        ruleInfo = ruleInfo->m_nextRuleInfo;
    }
}

void RuleIndex::ensureThreadReady(const size_t threadIndex) {
    m_bodyLiteralInfoIndex.ensureThreadReady(threadIndex);
    m_underlyingNegationLiteralInfoIndex.ensureThreadReady(threadIndex);
    RuleInfo* ruleInfo = m_firstRuleInfo;
    while (ruleInfo != nullptr) {
        ruleInfo->ensureThreadReady(threadIndex);
        ruleInfo = ruleInfo->m_nextRuleInfo;
    }
}

bool RuleIndex::addRule(const size_t currentNumberOfThreads, const Rule& rule, const bool isInternalRule) {
    const Rule ownRule = rule->clone(m_logicFactory);
    std::unordered_map<Rule, std::unique_ptr<RuleInfo> >::iterator iterator = m_ruleInfosByRule.find(ownRule);
    if (iterator != m_ruleInfosByRule.end()) {
        if (iterator->second->m_justAdded)
            return false;
        iterator->second->m_justAdded = true;
        ++m_numberOfJustAddedRuleInfos;
    }
    else
        addRuleInfo(ownRule, isInternalRule, false, true, false);
    return true;
}

bool RuleIndex::removeRule(const Rule& rule) {
    Rule ownRule = rule->clone(m_logicFactory);
    std::unordered_map<Rule, std::unique_ptr<RuleInfo> >::iterator iterator = m_ruleInfosByRule.find(ownRule);
    if (iterator != m_ruleInfosByRule.end() && iterator->second->m_active && !iterator->second->m_justDeleted) {
        iterator->second->m_justDeleted = true;
        ++m_numberOfJustDeletedRuleInfos;
        return true;
    }
    else
        return false;
}

void RuleIndex::propagateDeletions() {
    if (m_numberOfJustDeletedRuleInfos > 0) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            RuleInfo* nextRuleInfo = ruleInfo->m_nextRuleInfo;
            if (ruleInfo->m_justDeleted) {
                ruleInfo->ensureInactive();
                ruleInfo->m_justDeleted = false;
                --m_numberOfJustDeletedRuleInfos;
                if (!ruleInfo->m_justAdded) {
                    if (ruleInfo->m_previousRuleInfo == nullptr)
                        m_firstRuleInfo = ruleInfo->m_nextRuleInfo;
                    else
                        ruleInfo->m_previousRuleInfo->m_nextRuleInfo = ruleInfo->m_nextRuleInfo;
                    if (ruleInfo->m_nextRuleInfo == nullptr)
                        m_lastRuleInfo = ruleInfo->m_previousRuleInfo;
                    else
                        ruleInfo->m_nextRuleInfo->m_previousRuleInfo = ruleInfo->m_previousRuleInfo;
                    if (ruleInfo->m_pivotless)
                        --m_numberOfPivotlessRules;
                    if (ruleInfo->m_hasNegation)
                        --m_numberOfRulesWithNegation;
                    if (ruleInfo->m_hasAggregation)
                        --m_numberOfRulesWithAggregation;
                    m_ruleInfosByRule.erase(m_ruleInfosByRule.find(ruleInfo->getRule()));
                }
            }
            ruleInfo = nextRuleInfo;
        }
        assert(m_numberOfJustDeletedRuleInfos == 0);
    }
}

void RuleIndex::propagateInsertions() {
    if (m_numberOfJustAddedRuleInfos > 0) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            if (ruleInfo->m_justAdded) {
                ruleInfo->ensureActive();
                ruleInfo->m_justAdded = false;
                --m_numberOfJustAddedRuleInfos;
            }
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
        assert(m_numberOfJustAddedRuleInfos == 0);
    }
    updateDependencyGraph();
}

void RuleIndex::enqueueDeletedRules(LockFreeQueue<RuleInfo*>& ruleQueue) {
    if (m_numberOfJustDeletedRuleInfos > 0) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            if (ruleInfo->m_active && ruleInfo->m_justDeleted)
                ruleQueue.enqueue<false>(ruleInfo);
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
    }
}

void RuleIndex::enqueueInsertedRules(LockFreeQueue<RuleInfo*>& ruleQueue) {
    if (m_numberOfJustAddedRuleInfos > 0) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            if (!ruleInfo->m_active && ruleInfo->m_justAdded)
                ruleQueue.enqueue<false>(ruleInfo);
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
    }
}

template<bool checkComponentLevel>
void RuleIndex::enqueueRulesWithoutPositivePivot(const size_t componentLevel, LockFreeQueue<RuleInfo*>& ruleQueue) {
    if (m_numberOfPivotlessRules > 0) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            if (ruleInfo->m_active && ruleInfo->m_pivotless && (!checkComponentLevel || ruleInfo->isInComponentLevelFilter(componentLevel)))
                ruleQueue.enqueue<false>(ruleInfo);
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
    }
}

bool RuleIndex::ensureConstantsAreNormalizedMain() {
    bool hasChange = false;
    std::vector<ResourceID>& argumentsBufferForFirstThreadMain = m_byThreadInfos[0]->m_argumentBufferMain;
    const size_t numberOfTerms = argumentsBufferForFirstThreadMain.size();
    for (size_t index = 0; index < numberOfTerms; ++index) {
        const ResourceID originalID = m_defaultArgumentsBuffer[index];
        if (originalID != INVALID_RESOURCE_ID) {
            const ResourceID normalizedID = m_equalityManager.normalize(originalID);
            if (argumentsBufferForFirstThreadMain[index] != normalizedID) {
                for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator)
                    (*iterator)->m_argumentBufferMain[index] = normalizedID;
                hasChange = true;
            }
        }
    }
    if (hasChange) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            unique_ptr_vector<RuleInfo::RuleConstant>& ruleConstants = ruleInfo->m_ruleConstants;
            bool hasChange = false;
            for (unique_ptr_vector<RuleInfo::RuleConstant>::iterator iterator = ruleConstants.begin(); iterator != ruleConstants.end(); ++iterator) {
                const ResourceID normalizedID = m_equalityManager.normalize((*iterator)->m_defaultID);
                if (normalizedID != (*iterator)->m_currentMainID) {
                    (*iterator)->m_currentMainID = normalizedID;
                    hasChange = true;
                }
            }
            if (hasChange)
                ruleInfo->normalizeConstantsMain(m_equalityManager);
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
    }
    return hasChange;
}

template<bool checkComponentLevel>
void RuleIndex::enqueueRulesToReevaluateMain(const ResourceID mergedID, const size_t componentLevel, LockFreeQueue<RuleInfo*>& ruleQueue) {
    const ResourceID normalizedID = m_equalityManager.normalize(mergedID);
    for (ResourceID resourceID = mergedID; resourceID != INVALID_RESOURCE_ID; resourceID = m_equalityManager.getNextEqual(resourceID)) {
        ArgumentIndex argumentsBufferIndex;
        RuleInfo::RuleConstant* ruleConstant;
        if (m_ruleConstantIndex.getConstant(resourceID, argumentsBufferIndex, ruleConstant)) {
            for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator)
                (*iterator)->m_argumentBufferMain[argumentsBufferIndex] = normalizedID;
            while (ruleConstant != nullptr) {
                ruleConstant->m_currentMainID = normalizedID;
                RuleInfo& ruleInfo = ruleConstant->m_forRuleInfo;
                if (ruleInfo.m_active) {
                    if (!checkComponentLevel || ruleInfo.isInComponentLevelFilter(componentLevel))
                        ruleQueue.enqueue<false>(&ruleInfo);
                    ruleInfo.normalizeConstantsMain(m_equalityManager);
                }
                ruleConstant = ruleConstant->m_next;
            }
        }
    }
}

bool RuleIndex::ensureConstantsAreNormalizedIncremental() {
    bool hasChange = false;
    std::vector<ResourceID>& argumentsBufferForFirstThreadIncremental = m_byThreadInfos[0]->m_argumentBufferIncremental;
    const size_t numberOfTerms = argumentsBufferForFirstThreadIncremental.size();
    for (size_t index = 0; index < numberOfTerms; ++index) {
        const ResourceID originalID = m_defaultArgumentsBuffer[index];
        if (originalID != INVALID_RESOURCE_ID) {
            const ResourceID normalizedID = m_provingEqualityManager.normalize(originalID);
            if (argumentsBufferForFirstThreadIncremental[index] != normalizedID) {
                for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator)
                    (*iterator)->m_argumentBufferIncremental[index] = normalizedID;
                hasChange = true;
            }
        }
    }
    if (hasChange) {
        RuleInfo* ruleInfo = m_firstRuleInfo;
        while (ruleInfo != nullptr) {
            unique_ptr_vector<RuleInfo::RuleConstant>& ruleConstants = ruleInfo->m_ruleConstants;
            bool hasChange = false;
            for (unique_ptr_vector<RuleInfo::RuleConstant>::iterator iterator = ruleConstants.begin(); iterator != ruleConstants.end(); ++iterator) {
                const ResourceID normalizedID = m_provingEqualityManager.normalize((*iterator)->m_defaultID);
                if (normalizedID != (*iterator)->m_currentIncrementalID) {
                    (*iterator)->m_currentIncrementalID = normalizedID;
                    hasChange = true;
                }
            }
            if (hasChange)
                ruleInfo->normalizeConstantsMain(m_provingEqualityManager);
            ruleInfo = ruleInfo->m_nextRuleInfo;
        }
    }
    return hasChange;
}

template<bool checkComponentLevel>
void RuleIndex::enqueueRulesToReevaluateIncremental(const ResourceID mergedID, const size_t componentLevel, LockFreeQueue<RuleInfo*>& ruleQueue) {
    const ResourceID normalizedID = m_provingEqualityManager.normalize(mergedID);
    for (ResourceID resourceID = mergedID; resourceID != INVALID_RESOURCE_ID; resourceID = m_provingEqualityManager.getNextEqual(resourceID)) {
        ArgumentIndex argumentsBufferIndex;
        RuleInfo::RuleConstant* ruleConstant;
        if (m_ruleConstantIndex.getConstant(resourceID, argumentsBufferIndex, ruleConstant)) {
            for (auto iterator = m_byThreadInfos.begin(); iterator != m_byThreadInfos.end(); ++iterator)
                (*iterator)->m_argumentBufferIncremental[argumentsBufferIndex] = normalizedID;
            while (ruleConstant != nullptr) {
                ruleConstant->m_currentIncrementalID = normalizedID;
                RuleInfo& ruleInfo = ruleConstant->m_forRuleInfo;
                if (ruleInfo.m_active) {
                    if (!checkComponentLevel || ruleInfo.isInComponentLevelFilter(componentLevel))
                        ruleQueue.enqueue<false>(&ruleInfo);
                    ruleInfo.normalizeConstantsIncremental(m_provingEqualityManager);
                }
                ruleConstant = ruleConstant->m_next;
            }
        }
    }
}

void RuleIndex::recompileRules() {
    struct RuleStorage : private Unmovable {
        Rule m_rule;
        bool m_isInternalRule;
        bool m_active;
        bool m_justAdded;
        bool m_justDeleted;

        RuleStorage(const RuleInfo* const ruleInfo) : m_rule(ruleInfo->getRule()), m_isInternalRule(ruleInfo->m_internalRule), m_active(ruleInfo->m_active), m_justAdded(ruleInfo->m_justAdded), m_justDeleted(ruleInfo->m_justDeleted) {
        }
    };

    unique_ptr_vector<RuleStorage> ruleStorage;
    RuleInfo* ruleInfo = m_firstRuleInfo;
    while (ruleInfo != nullptr) {
        ruleStorage.push_back(std::unique_ptr<RuleStorage>(new RuleStorage(ruleInfo)));
        ruleInfo = ruleInfo->m_nextRuleInfo;
    }
    initialize();
    for (unique_ptr_vector<RuleStorage>::iterator iterator = ruleStorage.begin(); iterator != ruleStorage.end(); ++iterator)
        addRuleInfo((*iterator)->m_rule, (*iterator)->m_isInternalRule, (*iterator)->m_active, (*iterator)->m_justAdded, (*iterator)->m_justDeleted);
}

void RuleIndex::save(OutputStream& outputStream) const {
    outputStream.writeString("RuleIndex");
    outputStream.write(m_ruleInfosByRule.size());
    RuleInfo* ruleInfo = m_firstRuleInfo;
    while (ruleInfo != nullptr) {
        outputStream.writeString(ruleInfo->getRule()->toString(Prefixes::s_emptyPrefixes));
        outputStream.write(ruleInfo->isInternalRule());
        outputStream.write(ruleInfo->m_active);
        outputStream.write(ruleInfo->m_justAdded);
        outputStream.write(ruleInfo->m_justDeleted);
        ruleInfo = ruleInfo->m_nextRuleInfo;
    }
}

void RuleIndex::load(InputStream& inputStream) {
    initialize();
    if (!inputStream.checkNextString("RuleIndex"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load RuleIndex.");
    const size_t numberOfRules = inputStream.read<size_t>();
    Prefixes prefixes;
    DatalogParser parser(prefixes);
    DatalogProgram datalogProgram;
    std::vector<Atom> facts;
    DatalogProgramInputImporter datalogProgramInputImporter(m_logicFactory, datalogProgram, facts, nullptr);
    std::string ruleText;
    for (size_t index = 0; index < numberOfRules; ++index) {
        inputStream.readString(ruleText, 1000000);
        const bool isInternalRule = inputStream.read<bool>();
        const bool inMain = inputStream.read<bool>();
        const bool justAdded = inputStream.read<bool>();
        const bool justDeleted = inputStream.read<bool>();
        datalogProgram.clear();
        MemorySource memorySource(ruleText.c_str(), ruleText.length());
        parser.bind(memorySource);
        parser.parse(m_logicFactory, datalogProgramInputImporter);
        parser.unbind();
        assert(datalogProgram.size() == 1);
        addRuleInfo(datalogProgram[0], isInternalRule, inMain, justAdded, justDeleted);
    }
    updateDependencyGraph();
}

template void RuleIndex::enqueueRulesWithoutPositivePivot<true>(const size_t, LockFreeQueue<RuleInfo*>&);
template void RuleIndex::enqueueRulesWithoutPositivePivot<false>(const size_t, LockFreeQueue<RuleInfo*>&);
template void RuleIndex::enqueueRulesToReevaluateMain<true>(const ResourceID, const size_t, LockFreeQueue<RuleInfo*>&);
template void RuleIndex::enqueueRulesToReevaluateMain<false>(const ResourceID, const size_t, LockFreeQueue<RuleInfo*>&);
template void RuleIndex::enqueueRulesToReevaluateIncremental<false>(const ResourceID, const size_t, LockFreeQueue<RuleInfo*>&);
template void RuleIndex::enqueueRulesToReevaluateIncremental<true>(const ResourceID, const size_t, LockFreeQueue<RuleInfo*>&);
