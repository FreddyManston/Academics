// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef COMPONENTSTATISTICS_H_
#define COMPONENTSTATISTICS_H_

#include "../Common.h"

class ComponentStatistics : private Unmovable {

protected:


    struct Item : private Unmovable {

        const std::string m_name;
        const uint64_t m_integerValue;
        const uint16_t m_thousandsValue;

        Item(const std::string& name, const uint64_t value);

        Item(const std::string& name, const double value);

    };

    const std::string m_name;
    unique_ptr_vector<Item> m_items;
    unique_ptr_vector<ComponentStatistics> m_subcomponents;

    void print(std::ostream& output, const std::string& parentPath) const;

public:

    ComponentStatistics(const std::string& name);

    always_inline const std::string& getName() const {
        return m_name;
    }

    void addIntegerItem(const std::string& itemName, const uint64_t itemValue);

    void addFloatingPointItem(const std::string& itemName, const double itemValue);

    uint64_t getItemIntegerValue(const std::string& itemName) const;

    uint16_t getItemThousandsValue(const std::string& itemName) const;

    always_inline void addSubcomponent(std::unique_ptr<ComponentStatistics> subcomponent) {
        m_subcomponents.push_back(std::move(subcomponent));
    }

    always_inline size_t getNumberOfSubcomponents() const {
        return m_subcomponents.size();
    }

    always_inline const ComponentStatistics& getSubcomponent(int index) const {
        return *m_subcomponents[index];
    }

    always_inline void print(std::ostream& output) const {
        print(output, "");
    }

};

#endif /* COMPONENTSTATISTICS_H_ */
