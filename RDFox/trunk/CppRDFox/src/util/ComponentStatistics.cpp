// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "ComponentStatistics.h"

// ComponentStatistics::Item

ComponentStatistics::Item::Item(const std::string& name, const uint64_t value) :
    m_name(name),
    m_integerValue(value),
    m_thousandsValue(0)
{
}

always_inline uint16_t getThousands(const double value) {
    const double absValue = std::abs(value);
    const double rest = absValue - static_cast<double>(static_cast<uint64_t>(absValue));
    return static_cast<uint16_t>(rest * 1000.0);
}

ComponentStatistics::Item::Item(const std::string& name, const double value) :
    m_name(name),
    m_integerValue(static_cast<uint64_t>(value)),
    m_thousandsValue(getThousands(value))
{
}

// ComponentStatistics

void ComponentStatistics::print(std::ostream& output, const std::string& parentPath) const {
    size_t maxItemNameLength = 0;
    uint64_t maxIntegerItemValue = 0;
    size_t maxFloatingPointItemLength = 0;
    for (unique_ptr_vector<Item>::const_iterator iterator = m_items.begin(); iterator != m_items.end(); ++iterator) {
        size_t itemNameLength = (*iterator)->m_name.length();
        if (itemNameLength > maxItemNameLength)
            maxItemNameLength = itemNameLength;
        if ((*iterator)->m_integerValue > maxIntegerItemValue)
            maxIntegerItemValue = (*iterator)->m_integerValue;
    }
    const size_t maxItemValueLength = std::max(::getNumberOfDigitsFormated(maxIntegerItemValue), maxFloatingPointItemLength);
    const size_t maxLineLength = std::max(2 + parentPath.length() + 2 + m_name.length() + 2, 4 + maxItemNameLength + 4 + maxItemValueLength + 4 + 9 + 4);
    output << std::endl << std::endl << "  " << parentPath << "::" << m_name << std::endl;
    for (size_t index = maxLineLength; index > 0; --index)
        output << '-';
    output << std::endl;
    for (unique_ptr_vector<Item>::const_iterator iterator = m_items.begin(); iterator != m_items.end(); ++iterator) {
        const std::string itemName = (*iterator)->m_name;
        output << "    ";
        output << itemName;
        for (size_t index = itemName.length(); index < maxItemNameLength; ++index)
            output << ' ';
        output << " :  ";
        printNumberFormatted(output, (*iterator)->m_integerValue, maxItemValueLength);
        if ((*iterator)->m_thousandsValue != 0) {
            output << '.';
            if ((*iterator)->m_thousandsValue <= 9)
                output << "00";
            else if ((*iterator)->m_thousandsValue <= 99)
                output << '0';
            output << (*iterator)->m_thousandsValue;
        }
        else {
            output << "     (";
            printNumberAbbreviated(output, (*iterator)->m_integerValue);
            output << ')';
        }
        output << std::endl;
    }
    std::string myPath(parentPath + "::" + m_name);
    for (unique_ptr_vector<ComponentStatistics>::const_iterator iterator = m_subcomponents.begin(); iterator != m_subcomponents.end(); ++iterator)
        (*iterator)->print(output, myPath);
}

ComponentStatistics::ComponentStatistics(const std::string& name) : m_name(name), m_items(), m_subcomponents() {
}

void ComponentStatistics::addIntegerItem(const std::string& itemName, const uint64_t itemValue) {
    std::unique_ptr<Item> item(new Item(itemName, itemValue));
    m_items.push_back(std::move(item));
}

void ComponentStatistics::addFloatingPointItem(const std::string& itemName, const double itemValue) {
    std::unique_ptr<Item> item(new Item(itemName, itemValue));
    m_items.push_back(std::move(item));
}

uint64_t ComponentStatistics::getItemIntegerValue(const std::string& itemName) const {
    for (unique_ptr_vector<Item>::const_iterator iterator = m_items.begin(); iterator != m_items.end(); ++iterator)
        if ((*iterator)->m_name == itemName)
            return (*iterator)->m_integerValue;
    std::ostringstream message;
    message << "Item with name '" << itemName << "' not found.";
    throw RDF_STORE_EXCEPTION(message.str());
}

uint16_t ComponentStatistics::getItemThousandsValue(const std::string& itemName) const {
    for (unique_ptr_vector<Item>::const_iterator iterator = m_items.begin(); iterator != m_items.end(); ++iterator)
        if ((*iterator)->m_name == itemName)
            return (*iterator)->m_thousandsValue;
    std::ostringstream message;
    message << "Item with name '" << itemName << "' not found.";
    throw RDF_STORE_EXCEPTION(message.str());
}
