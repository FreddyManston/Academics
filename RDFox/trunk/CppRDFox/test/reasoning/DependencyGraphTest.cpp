// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST DependencyGraphTest

#include <CppTest/AutoTest.h>

#include "../../src/RDFStoreException.h"
#include "../../src/logic/Logic.h"
#include "../../src/reasoning/RuleIndex.h"
#include "../../src/dictionary/Dictionary.h"
#include "../../src/formats/InputOutput.h"
#include "../../src/formats/InputConsumer.h"
#include "../../src/formats/sources/MemorySource.h"
#include "../../src/storage/Parameters.h"
#include "../../src/storage/DataStore.h"
#include "..//logic/AbstractLogicTest.h"

// RuleConsumer

class RuleConsumer : public InputConsumer {

protected:

    DatalogProgram m_program;

public:

    RuleConsumer() : m_program() {
    }

    const DatalogProgram& getDatalogProgram() const {
        return m_program;
    }

    virtual void start() {
    }

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription) {
        throw RDF_STORE_EXCEPTION(errorDescription);
    }

    virtual void consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI) {
    }

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    }

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule) {
        m_program.push_back(rule);
    }

    virtual void finish() {
    }

};

// DependencyGraphTest

class DependencyGraphTest {

protected:

    Prefixes m_prefixes;
    LogicFactory m_factory;
    std::unique_ptr<DataStore> m_dataStore;
    RuleIndex m_ruleIndex;
    std::vector<ResourceID> m_argumentsBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;

    const RuleInfo* addRule(const char* const ruleText) {
        MemorySource memorySource(ruleText, ::strlen(ruleText));
        RuleConsumer ruleConsumer;
        std::string formatName;
        ::load(memorySource, m_prefixes, m_factory, ruleConsumer, formatName);
        if (ruleConsumer.getDatalogProgram().size() != 1)
            throw RDF_STORE_EXCEPTION("Just one rule should be specified.");
        m_ruleIndex.addRule(1, ruleConsumer.getDatalogProgram()[0], false);
        return m_ruleIndex.getRuleInfoFor(ruleConsumer.getDatalogProgram()[0]);
    }

    void removeRule(const RuleInfo* ruleInfo) {
        m_ruleIndex.removeRule(ruleInfo->getRule());
    }

    void assertLevel(const char* const fileName, const long lineNumber, const size_t expectedLevel, const std::string& subject, const std::string& predicate, const std::string& object) {
        m_argumentsBuffer[0] = m_dataStore->getDictionary().resolveResource(m_prefixes.decodeIRI(subject), D_IRI_REFERENCE);
        m_argumentsBuffer[1] = m_dataStore->getDictionary().resolveResource(m_prefixes.decodeIRI(predicate), D_IRI_REFERENCE);
        m_argumentsBuffer[2] = m_dataStore->getDictionary().resolveResource(m_prefixes.decodeIRI(object), D_IRI_REFERENCE);
        const size_t actualLevel = m_ruleIndex.getComponentLevel(m_argumentsBuffer, m_argumentIndexes);
        CppTest::assertEqual(expectedLevel, actualLevel, fileName, lineNumber);
    }
    
    static Parameters getDataStoreParameters() {
        Parameters dataStoreParameters;
        dataStoreParameters.setString("equality", "off");
        return dataStoreParameters;
    }

public:

    DependencyGraphTest() : m_prefixes(), m_factory(::newLogicFactory()), m_dataStore(::newDataStore("par-simple-nn", getDataStoreParameters())), m_ruleIndex(*m_dataStore), m_argumentsBuffer(3, INVALID_RESOURCE_ID), m_argumentIndexes() {
        m_prefixes.declareStandardPrefixes();
        m_prefixes.declarePrefix(":", "http://krr.cs.ox.ac.uk/RDF-store/test#");
        m_argumentIndexes.push_back(0);
        m_argumentIndexes.push_back(1);
        m_argumentIndexes.push_back(2);
    }

    void initialize() {
        m_dataStore->initialize();
        m_ruleIndex.initialize();
        m_ruleIndex.setThreadCapacity(1);
    }

};

#define ASSERT_LEVEL(expectedLevel, s, p, o) \
    assertLevel(__FILE__, __LINE__, expectedLevel, s, p, o)

TEST(testBasic) {
    const RuleInfo* r1 = addRule(":B(?X) :- :A(?X) .");
    const RuleInfo* r2 = addRule(":C(?X) :- :B(?X) .");
    const RuleInfo* r3 = addRule(":C(?X) :- :A(?X) .");
    const RuleInfo* r4 = addRule(":C(?X) :- :D(?X) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r4->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, m_ruleIndex.getMaxComponentLevel());
}

TEST(testRecursiveFirst) {
    const RuleInfo* r1 = addRule(":B(?X) :- :A(?X) .");
    const RuleInfo* r2 = addRule(":A(?X) :- :B(?X) .");
    const RuleInfo* r3 = addRule(":C(?X) :- :A(?X) .");
    const RuleInfo* r4 = addRule(":C(?X) :- :D(?X) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(0, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(0, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(0, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r4->getHeadAtomInfo(0).getComponentLevel());
}

TEST(testComplex) {
    const RuleInfo* r1 = addRule(":B(?X) :- :A(?X) .");
    const RuleInfo* r2 = addRule(":C(?X) :- :B(?X) .");
    const RuleInfo* r3 = addRule(":B(?X) :- :C(?X) .");
    const RuleInfo* r4 = addRule(":E(?X) :- :B(?X) .");
    const RuleInfo* r5 = addRule(":F(?X) :- :E(?X) .");
    const RuleInfo* r6 = addRule(":E(?X) :- :F(?X) .");
    const RuleInfo* r7 = addRule(":H(?X) :- :F(?X) .");
    const RuleInfo* r8 = addRule(":G(?X) :- :C(?X) .");
    const RuleInfo* r9 = addRule(":H(?X) :- :G(?X) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r4->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r5->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r6->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r7->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r8->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r9->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, m_ruleIndex.getMaxComponentLevel());

    // Deletions do not cause the levels to recompute so that we can use the old levels for incremental reasoning.
    removeRule(r3);
    m_ruleIndex.propagateDeletions();
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r4->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r5->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r6->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r7->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r8->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r9->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, m_ruleIndex.getMaxComponentLevel());

    // Propataion of insertions, even if vacuous, ensures that the rules are up-to-date.
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r4->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r5->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r6->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(4, r7->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r8->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(4, r9->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(4, m_ruleIndex.getMaxComponentLevel());
}

TEST(testUnification) {
    const RuleInfo* r1 = addRule(":B(?X) :- :A(?X) .");
    const RuleInfo* r2 = addRule(":C(:a) :- :B(?X) .");
    const RuleInfo* r3 = addRule(":C(:b) :- :A(?X) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, m_ruleIndex.getMaxComponentLevel());

    const RuleInfo* r4 = addRule(":D(:b) :- :C(?X) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r4->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, m_ruleIndex.getMaxComponentLevel());

    removeRule(r4);
    m_ruleIndex.propagateDeletions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, m_ruleIndex.getMaxComponentLevel());

    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, m_ruleIndex.getMaxComponentLevel());
}

TEST(testComponentLevel1) {
    const RuleInfo* r1 = addRule(":R(?X,?X) :- :A(?X) .");
    const RuleInfo* r2 = addRule(":C(:a) :- :R(?X,?X) .");
    const RuleInfo* r3 = addRule(":C(:b) :- :A(?X) .");
    const RuleInfo* r4 = addRule(":D(:b) :- :C(?X) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r4->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, m_ruleIndex.getMaxComponentLevel());

    ASSERT_LEVEL(0, ":c", "rdf:type", ":A");
    ASSERT_LEVEL(1, ":c", ":R", ":d");
    ASSERT_LEVEL(2, ":a", "rdf:type", ":C");
    ASSERT_LEVEL(2, ":c", "rdf:type", ":C");
    ASSERT_LEVEL(3, ":b", "rdf:type", ":D");
    ASSERT_LEVEL(0, ":d", "rdf:type", ":D");
}

TEST(testComponentLevel2) {
    const RuleInfo* r1 = addRule("[:b, ?X, ?X] :- [:a, ?X, ?X] .");
    const RuleInfo* r2 = addRule("[:c, ?X, ?X] :- [:b, ?X, ?X] .");
    const RuleInfo* r3 = addRule("[:c, ?X, ?X] :- [:a, ?X, ?X] .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, m_ruleIndex.getMaxComponentLevel());

    ASSERT_LEVEL(0, ":a", ":X", ":Y");
    ASSERT_LEVEL(1, ":b", ":X", ":Y");
    ASSERT_LEVEL(2, ":c", ":X", ":Y");
    ASSERT_LEVEL(0, ":d", ":X", ":Y");
}

TEST(testStraitfied) {
    const RuleInfo* r1 = addRule("[?X, rdf:type, :B] :- [?X, rdf:type, :O], not [?X, rdf:type, :A] .");
    const RuleInfo* r2 = addRule("[?X, rdf:type, :C] :- [?X, rdf:type, :O], not [?X, rdf:type, :B] .");
    const RuleInfo* r3 = addRule("[?X, rdf:type, :D] :- [?X, rdf:type, :O], not [?X, rdf:type, :C] .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(3, m_ruleIndex.getMaxComponentLevel());
}

TEST(testNotStraitfied) {
    const RuleInfo* r1 = addRule("[?X, rdf:type, :B] :- [?X, rdf:type, :O], not [?X, rdf:type, :A] .");
    const RuleInfo* r2 = addRule("[?X, rdf:type, :C] :- [?X, rdf:type, :O], not [?X, rdf:type, :B] .");
    const RuleInfo* r3 = addRule("[?X, rdf:type, :A] :- [?X, rdf:type, :O], not [?X, rdf:type, :C] .");
    m_ruleIndex.propagateInsertions();
    ASSERT_FALSE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, r3->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(1, m_ruleIndex.getMaxComponentLevel());
}

TEST(testMultipleRuleHeads) {
    const RuleInfo* r = addRule("[?Y, rdf:type, :A], [?Y, rdf:type, :B] :- [?X, :R, ?Y], [?X, rdf:type, :A] .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r->getHeadAtomInfo(1).getComponentLevel());
    ASSERT_TRUE(r->isInComponentLevelFilter(1));
    ASSERT_TRUE(r->isInComponentLevelFilter(2));
    ASSERT_EQUAL(2, m_ruleIndex.getMaxComponentLevel());
}

TEST(testAggregates) {
    const RuleInfo* r1 = addRule("[?X, :tempInC, ?TC] :- [?X, :tempInF, ?TF], BIND((?TF - 32) * 5 / 9 AS ?TC) .");
    const RuleInfo* r2 = addRule("[?X, :minTempInC, ?MT] :- [?X, rdf:type, :City], AGGREGATE([?X, :tempInC, ?T] ON ?X BIND MIN(?T) AS ?MT) .");
    m_ruleIndex.propagateInsertions();
    ASSERT_TRUE(m_ruleIndex.isStratified());
    ASSERT_EQUAL(1, m_ruleIndex.getFirstRuleComponentLevel());
    ASSERT_EQUAL(1, r1->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, r2->getHeadAtomInfo(0).getComponentLevel());
    ASSERT_EQUAL(2, m_ruleIndex.getMaxComponentLevel());
}

#endif
