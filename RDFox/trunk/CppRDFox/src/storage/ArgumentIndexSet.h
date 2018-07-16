// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ARGUMENTINDEXSET_H_
#define ARGUMENTINDEXSET_H_

#include "../Common.h"

class ArgumentIndexSet : private std::vector<ArgumentIndex> {

public:

    typedef std::vector<ArgumentIndex>::iterator iterator;
    typedef std::vector<ArgumentIndex>::const_iterator const_iterator;

    always_inline ArgumentIndexSet() : std::vector<ArgumentIndex>() {
    }

    always_inline ArgumentIndexSet(const ArgumentIndexSet& other) : std::vector<ArgumentIndex>(other) {
    }

    always_inline ArgumentIndexSet(ArgumentIndexSet&& other) : std::vector<ArgumentIndex>(std::move(other)) {
    }

    always_inline ArgumentIndexSet(std::initializer_list<ArgumentIndex> initializer) {
        for (std::initializer_list<ArgumentIndex>::iterator iterator = initializer.begin(); iterator != initializer.end(); ++iterator)
            add(*iterator);
    }

    always_inline ArgumentIndexSet& operator=(const ArgumentIndexSet& other) {
        std::vector<ArgumentIndex>::operator=(other);
        return *this;
    }

    always_inline bool operator==(const ArgumentIndexSet& other) const {
        if (size() != other.size())
            return false;
        const_iterator iterator1 = begin();
        const_iterator iterator2 = other.begin();
        while (iterator1 != end()) {
            if (*iterator1 != *iterator2)
                return false;
            ++iterator1;
            ++iterator2;
        }
        return true;
    }

    always_inline bool operator!=(const ArgumentIndexSet& other) const {
        return !operator==(other);
    }

    always_inline iterator begin() {
        return std::vector<ArgumentIndex>::begin();
    }

    always_inline const_iterator begin() const {
        return std::vector<ArgumentIndex>::begin();
    }

    always_inline iterator end() {
        return std::vector<ArgumentIndex>::end();
    }

    always_inline const_iterator end() const {
        return std::vector<ArgumentIndex>::end();
    }

    always_inline void clear() {
        std::vector<ArgumentIndex>::clear();
    }

    always_inline void shrinkToFit() {
        std::vector<ArgumentIndex>::shrink_to_fit();
    }

    always_inline bool contains(const ArgumentIndex argumentIndex) const {
        return std::binary_search(begin(), end(), argumentIndex);
    }

    always_inline bool add(const ArgumentIndex argumentIndex) {
        iterator position = std::lower_bound(begin(), end(), argumentIndex);
        if (position == end() || *position > argumentIndex) {
            std::vector<ArgumentIndex>::insert(position, argumentIndex);
            return true;
        }
        else
            return false;
    }

    always_inline bool remove(const ArgumentIndex argumentIndex) {
        iterator position = std::lower_bound(begin(), end(), argumentIndex);
        if (position != end() && *position == argumentIndex) {
            std::vector<ArgumentIndex>::erase(position);
            return true;
        }
        else
            return false;
    }

    always_inline bool empty() const {
        return std::vector<ArgumentIndex>::empty();
    }

    always_inline size_t size() const {
        return std::vector<ArgumentIndex>::size();
    }

    always_inline bool contains(const ArgumentIndexSet& containee) const {
        for (const_iterator iterator = containee.begin(); iterator != containee.end(); ++iterator)
            if (!contains(*iterator))
                return false;
        return true;
    }

    always_inline bool hasNonemptyIntersectionWith(const ArgumentIndexSet& other) const {
        if (size() < other.size()) {
            for (const_iterator iterator = begin(); iterator != end(); ++iterator)
                if (other.contains(*iterator))
                    return true;
        }
        else {
            for (const_iterator iterator = other.begin(); iterator != other.end(); ++iterator)
                if (contains(*iterator))
                    return true;
        }
        return false;
    }

    always_inline bool hasEmptyIntersectionWith(const ArgumentIndexSet& other) const {
        if (size() < other.size()) {
            for (const_iterator iterator = begin(); iterator != end(); ++iterator)
                if (other.contains(*iterator))
                    return false;
        }
        else {
            for (const_iterator iterator = other.begin(); iterator != other.end(); ++iterator)
                if (contains(*iterator))
                    return false;
        }
        return true;
    }

    always_inline void intersectWith(const ArgumentIndexSet& other) {
        iterator iterator = begin();
        while (iterator != end()) {
            if (!other.contains(*iterator))
                iterator = std::vector<ArgumentIndex>::erase(iterator);
            else
                ++iterator;
        }
    }

    always_inline void setToUnion(const ArgumentIndexSet& other1, const ArgumentIndexSet& other2) {
        clear();
        if (other1.size() < other2.size()) {
            for (const_iterator iterator = other2.begin(); iterator != other2.end(); ++iterator)
                std::vector<ArgumentIndex>::push_back(*iterator);
            for (const_iterator iterator = other1.begin(); iterator != other1.end(); ++iterator)
                if (!other2.contains(*iterator))
                    add(*iterator);
        }
        else {
            for (const_iterator iterator = other1.begin(); iterator != other1.end(); ++iterator)
                std::vector<ArgumentIndex>::push_back(*iterator);
            for (const_iterator iterator = other2.begin(); iterator != other2.end(); ++iterator)
                if (!other1.contains(*iterator))
                    add(*iterator);
        }
    }

    always_inline void setToIntersection(const ArgumentIndexSet& other1, const ArgumentIndexSet& other2) {
        clear();
        if (other1.size() < other2.size()) {
            for (const_iterator iterator = other1.begin(); iterator != other1.end(); ++iterator)
                if (other2.contains(*iterator))
                    std::vector<ArgumentIndex>::push_back(*iterator);
        }
        else {
            for (const_iterator iterator = other2.begin(); iterator != other2.end(); ++iterator)
                if (other1.contains(*iterator))
                    std::vector<ArgumentIndex>::push_back(*iterator);
        }
    }

    always_inline void unionWith(const ArgumentIndexSet& other) {
        for (const_iterator iterator = other.begin(); iterator != other.end(); ++iterator)
            add(*iterator);
    }

    always_inline void unionWithIntersection(const ArgumentIndexSet& other1, const ArgumentIndexSet& other2) {
        if (other1.size() < other2.size()) {
            for (const_iterator iterator = other1.begin(); iterator != other1.end(); ++iterator)
                if (other2.contains(*iterator))
                    add(*iterator);
        }
        else {
            for (const_iterator iterator = other2.begin(); iterator != other2.end(); ++iterator)
                if (other1.contains(*iterator))
                    add(*iterator);
        }
    }

    always_inline void removeAll(const ArgumentIndexSet& other) {
        for (const_iterator iterator = other.begin(); iterator != other.end(); ++iterator)
            remove(*iterator);
    }

};

#endif /* ARGUMENTINDEXSET_H_ */
