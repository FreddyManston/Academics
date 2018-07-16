// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef GROUPITERATOR_H_
#define GROUPITERATOR_H_

#include "../storage/TupleIterator.h"

// GroupAllBoundOneIterator

template<bool callMonitor>
class GroupAllBoundOneIterator : public TupleIterator {

protected:

    const std::unique_ptr<TupleIterator> m_tupleIterator;

public:

    GroupAllBoundOneIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator);

    GroupAllBoundOneIterator(const GroupAllBoundOneIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;
    
};

// GroupAllBoundIterator

template<bool callMonitor>
class GroupAllBoundIterator : public TupleIterator {

protected:

    const std::unique_ptr<TupleIterator> m_tupleIterator;
    std::vector<std::pair<ArgumentIndex, ResourceID> > m_groupedValues;

    bool currentTupleMatches() const;

public:

    GroupAllBoundIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator);

    GroupAllBoundIterator(const GroupAllBoundIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;

};

// GroupOneUnboundIterator

template<bool callMonitor>
class GroupOneUnboundIterator : public TupleIterator {

protected:

    const std::unique_ptr<TupleIterator> m_tupleIterator;
    std::unordered_map<ResourceID, size_t> m_groups;
    std::unordered_map<ResourceID, size_t>::iterator m_currentGroup;

public:

    GroupOneUnboundIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator);

    GroupOneUnboundIterator(const GroupOneUnboundIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;
    
};

// newGroupIterator

always_inline std::unique_ptr<TupleIterator> newGroupIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator) {
    if (argumentIndexes.size() == 1) {
        if (tupleIterator->getSurelyBoundInputArguments().contains(argumentIndexes[0])) {
            if (tupleIteratorMonitor)
                return std::unique_ptr<TupleIterator>(new GroupAllBoundOneIterator<true>(tupleIteratorMonitor, argumentIndexes, std::move(tupleIterator)));
            else
                return std::unique_ptr<TupleIterator>(new GroupAllBoundOneIterator<false>(tupleIteratorMonitor, argumentIndexes, std::move(tupleIterator)));
        }
        else {
            if (tupleIteratorMonitor)
                return std::unique_ptr<TupleIterator>(new GroupOneUnboundIterator<true>(tupleIteratorMonitor, argumentIndexes, std::move(tupleIterator)));
            else
                return std::unique_ptr<TupleIterator>(new GroupOneUnboundIterator<false>(tupleIteratorMonitor, argumentIndexes, std::move(tupleIterator)));
        }
    }
    else {
        for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            if (!tupleIterator->getSurelyBoundInputArguments().contains(*iterator))
                return tupleIterator;
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new GroupAllBoundIterator<true>(tupleIteratorMonitor, argumentIndexes, std::move(tupleIterator)));
        else
            return std::unique_ptr<TupleIterator>(new GroupAllBoundIterator<false>(tupleIteratorMonitor, argumentIndexes, std::move(tupleIterator)));
    }
}

#endif /* GROUPITERATOR_H_ */
