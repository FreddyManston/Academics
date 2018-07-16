// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TUPLEITERATOR_H_
#define TUPLEITERATOR_H_

#include "../Common.h"
#include "ArgumentIndexSet.h"

class TupleIteratorMonitor;

class CloneReplacements {

protected:

    std::unordered_map<const void*, const void*> m_replacements;

public:

    always_inline CloneReplacements() : m_replacements() {
    }

    template<typename T>
    always_inline const T* getReplacement(const T* forObject) {
        if (forObject == nullptr)
            return nullptr;
        else {
            auto iterator = m_replacements.find(forObject);
            if (iterator == m_replacements.end())
                return forObject;
            else
                return reinterpret_cast<const T*>(iterator->second);
        }
    }

    template<typename T>
    always_inline T* getReplacement(T* forObject) {
        return const_cast<T*>(getReplacement(const_cast<const T*>(forObject)));
    }

    template<typename T>
    always_inline CloneReplacements& registerReplacement(const T* forObject, const T* replacement) {
        m_replacements[forObject] = reinterpret_cast<const void*>(replacement);
        return *this;
    }

};

class TupleIterator : private Unmovable {

protected:

    TupleIteratorMonitor* const m_tupleIteratorMonitor;
    std::vector<ResourceID>& m_argumentsBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;
    ArgumentIndexSet m_allInputArguments;
    ArgumentIndexSet m_surelyBoundInputArguments;
    ArgumentIndexSet m_allArguments;
    ArgumentIndexSet m_surelyBoundArguments;

    always_inline void eliminateArgumentIndexesRedundancy() {
        ArgumentIndexSet visitedArguments;
        for (std::vector<ArgumentIndex>::iterator iterator = m_argumentIndexes.begin(); iterator != m_argumentIndexes.end();) {
            if (visitedArguments.contains(*iterator) || !m_allArguments.contains(*iterator))
                iterator = m_argumentIndexes.erase(iterator);
            else {
                visitedArguments.add(*iterator);
                ++iterator;
            }
        }
        m_argumentIndexes.shrink_to_fit();
    }

public:

    TupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer) :
        m_tupleIteratorMonitor(tupleIteratorMonitor),
        m_argumentsBuffer(argumentsBuffer),
        m_argumentIndexes(),
        m_allInputArguments(),
        m_surelyBoundInputArguments(),
        m_allArguments(),
        m_surelyBoundArguments()
    {
    }

    TupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) :
        m_tupleIteratorMonitor(tupleIteratorMonitor),
        m_argumentsBuffer(argumentsBuffer),
        m_argumentIndexes(argumentIndexes),
        m_allInputArguments(),
        m_surelyBoundInputArguments(),
        m_allArguments(),
        m_surelyBoundArguments()
    {
    }

    TupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments) :
        m_tupleIteratorMonitor(tupleIteratorMonitor),
        m_argumentsBuffer(argumentsBuffer),
        m_argumentIndexes(),
        m_allInputArguments(allInputArguments),
        m_surelyBoundInputArguments(surelyBoundInputArguments),
        m_allArguments(allArguments),
        m_surelyBoundArguments(surelyBoundArguments)
    {
        for (ArgumentIndexSet::iterator iterator = m_allArguments.begin(); iterator != m_allArguments.end(); ++iterator)
            m_argumentIndexes.push_back(*iterator);
        m_argumentIndexes.shrink_to_fit();
    }

    TupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, int dummy) :
        m_tupleIteratorMonitor(tupleIteratorMonitor),
        m_argumentsBuffer(argumentsBuffer),
        m_argumentIndexes(),
        m_allInputArguments(allInputArguments),
        m_surelyBoundInputArguments(surelyBoundInputArguments),
        m_allArguments(allArguments),
        m_surelyBoundArguments(surelyBoundArguments)
    {
    }

    TupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) :
        m_tupleIteratorMonitor(tupleIteratorMonitor),
        m_argumentsBuffer(argumentsBuffer),
        m_argumentIndexes(argumentIndexes),
        m_allInputArguments(allInputArguments),
        m_surelyBoundInputArguments(surelyBoundInputArguments),
        m_allArguments(),
        m_surelyBoundArguments()
    {
    }

    TupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments) :
        m_tupleIteratorMonitor(tupleIteratorMonitor),
        m_argumentsBuffer(argumentsBuffer),
        m_argumentIndexes(argumentIndexes),
        m_allInputArguments(allInputArguments),
        m_surelyBoundInputArguments(surelyBoundInputArguments),
        m_allArguments(allArguments),
        m_surelyBoundArguments(surelyBoundArguments)
    {
    }

    TupleIterator(const TupleIterator& other, CloneReplacements& cloneReplacements) :
        m_tupleIteratorMonitor(cloneReplacements.getReplacement(other.m_tupleIteratorMonitor)),
        m_argumentsBuffer(*cloneReplacements.getReplacement(&other.m_argumentsBuffer)),
        m_argumentIndexes(other.m_argumentIndexes),
        m_allInputArguments(other.m_allInputArguments),
        m_surelyBoundInputArguments(other.m_surelyBoundInputArguments),
        m_allArguments(other.m_allArguments),
        m_surelyBoundArguments(other.m_surelyBoundArguments)
    {
    }

    virtual ~TupleIterator() {
    }

    virtual const char* getName() const = 0;

    virtual size_t getNumberOfChildIterators() const = 0;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const = 0;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const = 0;

    always_inline size_t getArity() const {
        return m_argumentIndexes.size();
    }

    always_inline std::vector<ResourceID>& getArgumentsBuffer() const {
        return m_argumentsBuffer;
    }

    always_inline const std::vector<ArgumentIndex>& getArgumentIndexes() const {
        return m_argumentIndexes;
    }

    always_inline const ArgumentIndexSet& getAllInputArguments() const {
        return m_allInputArguments;
    }

    always_inline const ArgumentIndexSet& getSurelyBoundInputArguments() const {
        return m_surelyBoundInputArguments;
    }

    always_inline const ArgumentIndexSet& getAllArguments() const {
        return m_allArguments;
    }

    always_inline const ArgumentIndexSet& getSurelyBoundArguments() const {
        return m_surelyBoundArguments;
    }

    virtual size_t open(ThreadContext& threadContext) = 0;

    virtual size_t open() = 0;

    virtual size_t advance() = 0;

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const = 0;

    virtual TupleIndex getCurrentTupleIndex() const = 0;
    
};

#endif /* TUPLEITERATOR_H_ */
