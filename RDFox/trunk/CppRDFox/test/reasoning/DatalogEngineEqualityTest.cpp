// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST DatalogEngineEqualityTest

#include <CppTest/AutoTest.h>

#include "../../src/Common.h"
#include "../../src/util/Vocabulary.h"
#include "../../src/dictionary/Dictionary.h"
#include "../../src/equality/EqualityManager.h"
#include "../querying/DataStoreTest.h"

class DatalogEngineEqualityTest : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", false);
        m_queryParameters.setBoolean("cardinality", true);
    }

public:

    DatalogEngineEqualityTest() : DataStoreTest("par-complex-ww", EQUALITY_AXIOMATIZATION_NO_UNA) {
    }

    ResourceID R(const std::string& iri) {
        std::string decodedIRI;
        m_prefixes.decodeAbbreviatedIRI(iri, decodedIRI);
        return m_dictionary.resolveResource(decodedIRI, D_IRI_REFERENCE);
    }

    void assertSameAs(const char* const fileName, const long lineNumber, const std::string& iri1, const std::string& iri2) {
        const ResourceID resourceID1 = R(iri1);
        const ResourceID resourceID2 = R(iri2);
        const ResourceID canonicalID1 = m_dataStore->getEqualityManager().normalize(resourceID1);
        const ResourceID canonicalID2 = m_dataStore->getEqualityManager().normalize(resourceID2);
        if (canonicalID1 != canonicalID2) {
            std::ostringstream message;
            message << "Resources '" << iri1 << "' and '" << iri2 << "' should be equal but are not." << std::endl;
            throw CppTest::AssertionError(fileName, lineNumber, message.str());
        }
    }

    void assertEquivalent(const char* const fileName, const long lineNumber, const std::vector<std::string>& iris) {
        std::unordered_set<ResourceID> set;
        ResourceID minResourceID = MAX_RESOURCE_ID;
        for (std::vector<std::string>::const_iterator iterator = iris.begin(); iterator != iris.end(); ++iterator) {
            const ResourceID resourceID = R(*iterator);
            if (resourceID < minResourceID)
                minResourceID = resourceID;
            set.insert(resourceID);
        }
        for (std::vector<std::string>::const_iterator iterator = iris.begin(); iterator != iris.end(); ++iterator) {
            const ResourceID resourceID = R(*iterator);
            if (minResourceID != m_dataStore->getEqualityManager().normalize(resourceID)) {
                std::ostringstream message;
                message << "Invalid normal form for IRI '" << *iterator << "'";
                throw CppTest::AssertionError(fileName, lineNumber, message.str());
            }
        }
        ResourceID currentResourceID = minResourceID;
        do {
            std::unordered_set<ResourceID>::iterator iterator = set.find(currentResourceID);
            if (iterator == set.end()) {
                std::ostringstream message;
                message << "Resource ID " << currentResourceID << " should not be in the equivalence class.";
                throw CppTest::AssertionError(fileName, lineNumber, message.str());
            }
            set.erase(iterator);
            currentResourceID = m_dataStore->getEqualityManager().getNextEqual(currentResourceID);
        } while (currentResourceID != INVALID_RESOURCE_ID);
        if (!set.empty()) {
            std::ostringstream message;
            message << "The following resource IDs occur do not occur in the equivalence class: ";
            bool first = true;
            for (std::unordered_set<ResourceID>::iterator iterator = set.begin(); iterator != set.end(); ++iterator) {
                if (first)
                    first = false;
                else
                    message << ", ";
                message << *iterator;
            }
            throw CppTest::AssertionError(fileName, lineNumber, message.str());
        }
    }

};

#define ASSERT_SAME_AS(iri1, iri2) \
    assertSameAs(__FILE__, __LINE__, iri1, iri2)

#define ASSERT_EQUIVALENT(iris) \
    assertEquivalent(__FILE__, __LINE__, iris)

always_inline std::vector<std::string> IRIs() {
    return std::vector<std::string>();
}

always_inline std::vector<std::string> operator<<(const std::vector<std::string>& iris, std::string iri) {
    std::vector<std::string> result(iris);
    result.push_back(iri);
    return result;
}

always_inline std::vector<std::string> operator<<(std::vector<std::string>&& iris, std::string iri) {
    std::vector<std::string> result(iris);
    result.push_back(iri);
    return result;
}

TEST(testBasic) {
    addTriples(
        ":i1 :R :a1 . "
        ":i2 :R :a2 . "
        ":i3 :R :a3 . "

        ":a1 rdf:type :A ."
        ":a2 rdf:type :A . "
        ":a3 rdf:type :A . "

        ":i1 :S :b1 . "
        ":i2 :S :b2 . "
        ":i3 :S :b3 . "

        ":b1 rdf:type :B . "
        ":b2 rdf:type :B . "
        ":b3 rdf:type :B . "

        ":a1 rdf:type :D . "
        ":b2 rdf:type :D . "
    );

    addRules(
        "[?Y1,owl:sameAs,?Y2] :- :R(?X,?Y1), :S(?X,?Y2) . "
        ":C(?X) :- :A(?X), :B(?X) . "
        ":E(?X) :- :C(?X), :D(?X) . "
    );
    applyRules();

    ASSERT_SAME_AS(":a1", ":b1");
    ASSERT_EQUIVALENT(IRIs() << ":a1" << ":b1");
    ASSERT_SAME_AS(":a2", ":b2");
    ASSERT_EQUIVALENT(IRIs() << ":a2" << ":b2");
    ASSERT_SAME_AS(":a3", ":b3");
    ASSERT_EQUIVALENT(IRIs() << ":a3" << ":b3");

    // First test query answers without equality expansion.
    // All constants will be normalized to the smaller ones, so the answers will be as below.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :a1 . "
        ":i2 :a2 . "
        ":i3 :a3 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i1 :a1 . "
        ":i2 :a2 . "
        ":i3 :a3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":a1 . "
        ":a2 . "
        ":a3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":a1 . "
        ":a2 . "
        ":a3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :C }",
        ":a1 . "
        ":a2 . "
        ":a3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :D }",
        ":a1 . "
        ":a2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :E }",
        ":a1 . "
        ":a2 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":a1 :a1 . "
        ":a2 :a2 . "
        ":a3 :a3 . "
        ":i1 :i1 . "
        ":i2 :i2 . "
        ":i3 :i3 . "
        ":A :A . "
        ":B :B . "
        ":C :C . "
        ":D :D . "
        ":E :E . "
        ":R :R . "
        ":S :S . "
        "rdf:type rdf:type . "
        "owl:sameAs owl:sameAs . "
    );
    ASSERT_QUERY("SELECT ?Y WHERE { :a1 owl:sameAs ?Y }",
        ":a1 . "
    );

    // Now enable equality expansion and check the answers again.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :a1 . "
        ":i1 :b1 . "
        ":i2 :a2 . "
        ":i2 :b2 . "
        ":i3 :a3 . "
        ":i3 :b3 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i1 :a1 . "
        ":i1 :b1 . "
        ":i2 :a2 . "
        ":i2 :b2 . "
        ":i3 :a3 . "
        ":i3 :b3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":a1 . "
        ":b1 . "
        ":a2 . "
        ":b2 . "
        ":a3 . "
        ":b3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":a1 . "
        ":b1 . "
        ":a2 . "
        ":b2 . "
        ":a3 . "
        ":b3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :C }",
        ":a1 . "
        ":b1 . "
        ":a2 . "
        ":b2 . "
        ":a3 . "
        ":b3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :D }",
        ":a1 . "
        ":b1 . "
        ":a2 . "
        ":b2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :E }",
        ":a1 . "
        ":b1 . "
        ":a2 . "
        ":b2 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":a1 :a1 . "
        ":a1 :b1 . "
        ":b1 :a1 . "
        ":b1 :b1 . "

        ":a2 :a2 . "
        ":a2 :b2 . "
        ":b2 :a2 . "
        ":b2 :b2 . "

        ":a3 :a3 . "
        ":a3 :b3 . "
        ":b3 :a3 . "
        ":b3 :b3 . "

        ":i1 :i1 . "
        ":i2 :i2 . "
        ":i3 :i3 . "

        ":A :A . "
        ":B :B . "
        ":C :C . "
        ":D :D . "
        ":E :E . "
        ":R :R . "
        ":S :S . "
        "rdf:type rdf:type . "
        "owl:sameAs owl:sameAs . "
    );
    ASSERT_QUERY("SELECT ?Y WHERE { :a1 owl:sameAs ?Y }",
        ":a1 . "
        ":b1 . "
    );
}

TEST(testMulipleEqualities) {
    addTriples(
        ":a :R :b1 . "
        ":a :R :b2 . "
        ":a :R :b3 . "
        ":a :R :b4 . "
        ":a :R :b5 . "

        ":b1 rdf:type :A . "
    );

    addRules(
        "[?Y1,owl:sameAs,?Y2] :- :R(?X,?Y1), :R(?X,?Y2) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":b1 . "
        ":b2 . "
        ":b3 . "
        ":b4 . "
        ":b5 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":a :a . "

        ":b1 :b1 . "
        ":b1 :b2 . "
        ":b1 :b3 . "
        ":b1 :b4 . "
        ":b1 :b5 . "

        ":b2 :b1 . "
        ":b2 :b2 . "
        ":b2 :b3 . "
        ":b2 :b4 . "
        ":b2 :b5 . "

        ":b3 :b1 . "
        ":b3 :b2 . "
        ":b3 :b3 . "
        ":b3 :b4 . "
        ":b3 :b5 . "

        ":b4 :b1 . "
        ":b4 :b2 . "
        ":b4 :b3 . "
        ":b4 :b4 . "
        ":b4 :b5 . "

        ":b5 :b1 . "
        ":b5 :b2 . "
        ":b5 :b3 . "
        ":b5 :b4 . "
        ":b5 :b5 . "

        ":A :A . "
        ":R :R . "
        "rdf:type rdf:type . "
        "owl:sameAs owl:sameAs . "
    );
}

TEST(testRecursiveEqualities) {
    addTriples(
        ":a :R :b1 . "
        ":a :R :b2 . "
        ":a :R :b3 . "
        ":a :R :b4 . "
        ":a :R :b5 . "

        ":b1 rdf:type :A . "
        ":b5 rdf:type :B . "
    );

    addRules(
        "[?Y1,owl:sameAs,?Y2] :- :A(?Y1), :R(?X,?Y1), :R(?X,?Y2) . "
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":b1 . "
        ":b2 . "
        ":b3 . "
        ":b4 . "
        ":b5 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":b1 . "
        ":b2 . "
        ":b3 . "
        ":b4 . "
        ":b5 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :C }",
        ":b1 . "
        ":b2 . "
        ":b3 . "
        ":b4 . "
        ":b5 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":a :a . "

        ":b1 :b1 . "
        ":b1 :b2 . "
        ":b1 :b3 . "
        ":b1 :b4 . "
        ":b1 :b5 . "

        ":b2 :b1 . "
        ":b2 :b2 . "
        ":b2 :b3 . "
        ":b2 :b4 . "
        ":b2 :b5 . "

        ":b3 :b1 . "
        ":b3 :b2 . "
        ":b3 :b3 . "
        ":b3 :b4 . "
        ":b3 :b5 . "

        ":b4 :b1 . "
        ":b4 :b2 . "
        ":b4 :b3 . "
        ":b4 :b4 . "
        ":b4 :b5 . "

        ":b5 :b1 . "
        ":b5 :b2 . "
        ":b5 :b3 . "
        ":b5 :b4 . "
        ":b5 :b5 . "

        ":A :A . "
        ":B :B . "
        ":C :C . "
        ":R :R . "
        "rdf:type rdf:type . "
        "owl:sameAs owl:sameAs . "
    );
}

TEST(testEqualityInData) {
    addTriples(
        ":a :R :b1 . "
        ":a :R :b2 . "
        ":a :R :b3 . "
        ":a :R :b4 . "
        ":a :R :b5 . "

        ":b1 rdf:type :A . "

        ":b1 owl:sameAs :b2 . "
        ":b2 owl:sameAs :b3 . "
        ":b3 owl:sameAs :b4 . "
        ":b4 owl:sameAs :b5 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":b1 . "
    );
    // The following query returns only what is explicitly available in the data.
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":b1 :b2 . "
        ":b2 :b3 . "
        ":b3 :b4 . "
        ":b4 :b5 . "
    );

    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":b1 . "
        ":b2 . "
        ":b3 . "
        ":b4 . "
        ":b5 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":a :a . "

        ":b1 :b1 . "
        ":b1 :b2 . "
        ":b1 :b3 . "
        ":b1 :b4 . "
        ":b1 :b5 . "

        ":b2 :b1 . "
        ":b2 :b2 . "
        ":b2 :b3 . "
        ":b2 :b4 . "
        ":b2 :b5 . "

        ":b3 :b1 . "
        ":b3 :b2 . "
        ":b3 :b3 . "
        ":b3 :b4 . "
        ":b3 :b5 . "

        ":b4 :b1 . "
        ":b4 :b2 . "
        ":b4 :b3 . "
        ":b4 :b4 . "
        ":b4 :b5 . "

        ":b5 :b1 . "
        ":b5 :b2 . "
        ":b5 :b3 . "
        ":b5 :b4 . "
        ":b5 :b5 . "

        ":A :A . "
        ":R :R . "
        "rdf:type rdf:type . "
        "owl:sameAs owl:sameAs . "
    );
}

TEST(testConstantsInRules) {
    addTriples(
        ":a1 :R :a2 . "
        ":a2 :R :a3 . "
        ":a3 :R :a4 . "
        ":a4 :R :a5 . "
        ":a5 :R :a6 . "

        ":b1 :S :b2 . "
        ":b2 :S :b3 . "
        ":b3 :S :b4 . "
        ":b4 :S :b5 . "
        ":b5 :S :b6 . "

        ":a1 rdf:type :A . "
        ":a1 owl:sameAs :b1 . "
    );

    addRules(
        "owl:sameAs(?Y1,?Y2) :- :A(?X), :R(?X,?Y1), :S(?X,?Y2) . "
        ":A(:b2) :- :R(:b1,:a2), :S(:a1,:b2) . "
        ":A(:b3) :- :R(:b2,:a3), :S(:a2,:b3) . "
        ":A(:b4) :- :R(:b3,:a4), :S(:a3,:b4) . "
        ":A(:b5) :- :R(:b4,:a5), :S(:a4,:b5) . "
        ":A(:b6) :- :R(:b5,:a6), :S(:a5,:b6) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A }",
        ":a1 . "
        ":a2 . "
        ":a3 . "
        ":a4 . "
        ":a5 . "
        ":a6 . "

        ":b1 . "
        ":b2 . "
        ":b3 . "
        ":b4 . "
        ":b5 . "
        ":b6 . "
    );

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":a1 :a2 . "
        ":a1 :b2 . "
        ":b1 :a2 . "
        ":b1 :b2 . "

        ":a2 :a3 . "
        ":a2 :b3 . "
        ":b2 :a3 . "
        ":b2 :b3 . "

        ":a3 :a4 . "
        ":a3 :b4 . "
        ":b3 :a4 . "
        ":b3 :b4 . "

        ":a4 :a5 . "
        ":a4 :b5 . "
        ":b4 :a5 . "
        ":b4 :b5 . "

        ":a5 :a6 . "
        ":a5 :b6 . "
        ":b5 :a6 . "
        ":b5 :b6 . "
    );

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":a1 :a2 . "
        ":a1 :b2 . "
        ":b1 :a2 . "
        ":b1 :b2 . "

        ":a2 :a3 . "
        ":a2 :b3 . "
        ":b2 :a3 . "
        ":b2 :b3 . "

        ":a3 :a4 . "
        ":a3 :b4 . "
        ":b3 :a4 . "
        ":b3 :b4 . "

        ":a4 :a5 . "
        ":a4 :b5 . "
        ":b4 :a5 . "
        ":b4 :b5 . "

        ":a5 :a6 . "
        ":a5 :b6 . "
        ":b5 :a6 . "
        ":b5 :b6 . "
    );

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":a1 :a1 . "
        ":a1 :b1 . "
        ":b1 :a1 . "
        ":b1 :b1 . "

        ":a2 :a2 . "
        ":a2 :b2 . "
        ":b2 :a2 . "
        ":b2 :b2 . "

        ":a3 :a3 . "
        ":a3 :b3 . "
        ":b3 :a3 . "
        ":b3 :b3 . "

        ":a4 :a4 . "
        ":a4 :b4 . "
        ":b4 :a4 . "
        ":b4 :b4 . "

        ":a5 :a5 . "
        ":a5 :b5 . "
        ":b5 :a5 . "
        ":b5 :b5 . "

        ":a6 :a6 . "
        ":a6 :b6 . "
        ":b6 :a6 . "
        ":b6 :b6 . "

        ":A :A . "
        ":R :R . "
        ":S :S . "
        "rdf:type rdf:type . "
        "owl:sameAs owl:sameAs . "
    );
}

TEST(testSameAsInRuleBodies) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :B . "
    );
    addRules(
        ":C(?X) :- owl:sameAs(?X,?X) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:sameAs rdf:type :C . "

        "rdf:type owl:sameAs rdf:type . "
        "rdf:type rdf:type :C . "

        ":A owl:sameAs :A . "
        ":A rdf:type :C . "

        ":B owl:sameAs :B . "
        ":B rdf:type :C . "

        ":C owl:sameAs :C . "
        ":C rdf:type :C . "

        ":a rdf:type :A . "
        ":a owl:sameAs :a . "
        ":a rdf:type :C . "

        ":b rdf:type :B . "
        ":b owl:sameAs :b . "
        ":b rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:sameAs rdf:type :C . "

        "rdf:type owl:sameAs rdf:type . "
        "rdf:type rdf:type :C . "

        ":A owl:sameAs :A . "
        ":A rdf:type :C . "

        ":B owl:sameAs :B . "
        ":B rdf:type :C . "

        ":C owl:sameAs :C . "
        ":C rdf:type :C . "

        ":a rdf:type :A . "
        ":a owl:sameAs :a . "
        ":a rdf:type :C . "

        ":b rdf:type :B . "
        ":b owl:sameAs :b . "
        ":b rdf:type :C . "
    );
}

TEST(testConstantsInRuleHeadBug) {
    addTriples(
        ":c rdf:type :C . "
        ":b owl:sameAs :c . "
        ":a rdf:type :A . "
    );

    addRules(
        ":R(?X,:b) :- :A(?X) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":a :b . "
        ":a :c . "
    );
}

TEST(testMergedConstantsInQueries) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":a owl:sameAs :b . "
    );

    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { :a rdf:type ?X }",
        ":A . "
        ":B . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { :b rdf:type ?X }",
        ":A . "
        ":B . "
    );
}

TEST(testDifferentFrom) {
    addTriples(
        ":a :R :b1 . "
        ":a :R :b2 . "
        ":b1 owl:differentFrom :b2 . "
    );

    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:differentFrom ?Y }",
        ":b1 :b1 . "
        ":b1 :b2 . "
        ":b2 :b1 . "
        ":b2 :b2 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type owl:Nothing }",
        ":b1 . "
        ":b2 . "
    );
}

TEST(testSameAsEqualDifferentFromCornerCase) {
    addTriples(
        ":a rdf:type :A . "
        "owl:sameAs owl:sameAs owl:differentFrom . "
    );

    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type owl:Nothing }",
        ":a . "
        "rdf:type . "
        ":A . "
        "owl:sameAs . "
        "owl:differentFrom . "
        "owl:Nothing . "
    );
}

TEST(testEmpty) {
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
}

TEST(testSameAsOnly) {
    addTriples(
        ":a rdf:type :A . "
    );
    addRules(
        "owl:sameAs(:b,:c) :- :A(?X) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a owl:sameAs :a . "

        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
    );
}

TEST(testLiteralsUNA1) {
    addTriples(
        ":a :R 1 . "
        ":a :R 2 . "
    );

    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type owl:Nothing }",
        "1 . "
        "2 . "
    );
}

TEST(testLiteralsUNA2) {
    addTriples(
        ":a :R 1 . "
    );

    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type owl:Nothing }",
        ""
    );
}

TEST(testAbstractConcreteSeparation) {
    addTriples(
        ":a :R :b . "
        ":a :R 2 . "
    );

    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type owl:Nothing }",
        ":b . "
        "2 . "
    );
}

TEST(testConstantsInQueryHeadAndBody) {
    addTriples(
        ":a :R :b . "
        ":b owl:sameAs :c . "
        ":a owl:sameAs :d . "
    );

    applyRules();

    ASSERT_QUERY("SELECT ?X :c WHERE { ?X :R :c }",
        ":a :c . "
        ":d :c . "
    );

    m_queryParameters.setString("domain", "IDBrep");

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":a :b . "
    );
    ASSERT_QUERY("SELECT ?X :c WHERE { ?X :R :c }",
        ":a :c . "
    );
}

TEST(testProjectedConstants1) {
    addTriples(
        ":a :R :i1 . "
        ":a :R :i2 . "
        ":a :R :i3 . "
        ":i1 owl:sameAs :i2 . "
        ":i2 owl:sameAs :i3 . "
    );

    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y }",
        ":a * 3 . "
    );
    ASSERT_QUERY("SELECT ?X ?X WHERE { ?X :R ?Y }",
        ":a :a * 3 . "
    );
    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . ?Z :R ?Y }",
        ":a :a * 3 . "
    );
}

TEST(testProjectedConstants2) {
    addTriples(
        ":a :R :b . "
        ":c :S :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :c . "
    );

    applyRules();

    ASSERT_QUERY("SELECT WHERE { :a :R :a }",
        " . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R :a }",
        ":a . "
        ":b . "
        ":c . "
    );
    ASSERT_QUERY("SELECT ?X ?X WHERE { ?X :R :a }",
        ":a :a . "
        ":b :b . "
        ":c :c . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y }",
        ":a * 3 . "
        ":b * 3 . "
        ":c * 3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?X }",
        ":a . "
        ":b . "
        ":c . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?X . ?X :R ?Y }",
        ":a * 3 . "
        ":b * 3 . "
        ":c * 3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . ?Y :S ?X }",
        ":a * 3 . "
        ":b * 3 . "
        ":c * 3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . ?Y :S :a }",
        ":a * 3 . "
        ":b * 3 . "
        ":c * 3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R :b . :b :S :a }",
        ":a . "
        ":b . "
        ":c . "
    );
}

TEST(testProjectedConstantsBushyJoins) {
    addTriples(
        ":a1 :R :b1 . "
        ":b1 :S :c1 . "
        ":c1 :T :d1 . "
        ":d1 :U :e1 . "
        ":a1 owl:sameAs :a2 . "
        ":b1 owl:sameAs :b2 . "
        ":c1 owl:sameAs :c2 . "
        ":d1 owl:sameAs :d2 . "
        ":e1 owl:sameAs :e2 . "
    );

    applyRules();

    ASSERT_QUERY("SELECT ?X1 WHERE { ?X1 :R ?X2 . ?X2 :S ?X3 . ?X3 :T ?X4 . ?X4 :U ?X5 }",
        ":a1 * 16 . "
        ":a2 * 16 . "
    );

    m_queryParameters.setBoolean("bushy", "true");

    ASSERT_QUERY("SELECT ?X1 WHERE { ?X1 :R ?X2 . ?X2 :S ?X3 . ?X3 :T ?X4 . ?X4 :U ?X5 }",
        ":a1 * 16 . "
        ":a2 * 16 . "
    );
}

TEST(testClearRulesAndMakeFactsExplict) {
    addTriples(
        ":a :R :b . "
        ":a :R :c . "
        ":b rdf:type :A . "
        ":c rdf:type :B . "
    );
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    applyRules();

    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":a :R :c . "
        ":b rdf:type :A . "
        ":c rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :B . "
        ":b rdf:type :C . "
        ":c rdf:type :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
    m_queryParameters.setString("domain", "EDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":a :R :c . "
        ":b rdf:type :A . "
        ":c rdf:type :B . "
    );

    clearRulesAndMakeFactsExplicit();

    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":a :R :c . "
        ":b rdf:type :A . "
        ":c rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :B . "
        ":b rdf:type :C . "
        ":c rdf:type :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
    m_queryParameters.setString("domain", "EDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :c . "
        ":b owl:sameAs :b . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
}

TEST(testOptionalProjection) {
    addTriples(
        ":a rdf:type :A . "
        ":a :R :b . "
        ":a :R :c . "
        ":b owl:sameAs :c . "
        ":R owl:sameAs :S . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :A OPTIONAL { ?X :R ?Y } }",
        ":a * 2 . "
    );
}

TEST(testUnionProjection) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":a1 owl:sameAs :a1p . "

        ":c1 :S :d1 . "
        ":c2 :S :d2 . "
        ":c1 owl:sameAs :c1p . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?Y ?Z WHERE { { ?X :R ?Y }  UNION { ?X :S ?Z } }",
        ":b1   UNDEF * 2 ."
        ":b2   UNDEF     ."
        "UNDEF :d1   * 2 ."
        "UNDEF :d2       ."
    );
}

TEST(testSubqueryProjection) {
    addTriples(
        ":a :R :c . "

        ":d :T :f . "

        ":R owl:sameAs :S . "

        ":a owl:sameAs :b . "
        ":c owl:sameAs :d . "
        ":e owl:sameAs :f . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { SELECT ?X ?Y WHERE { ?X :R ?Y . ?Y :T ?Z } }",
        ":a * 4 . "
        ":b * 4 . "
    );
}

TEST(testPivotlessRules) {
    addTriples(
        ":a rdf:type :A . "
        ":a owl:sameAs :b . "
    );
    addRules(
        ":B(:b) :- . "
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    applyRules();

    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
    );
}

TEST(testMultipleRuleHeads) {
    addTriples(
        ":c :T :d . "
        ":e :T :f . "
        ":g :U :a . "
        ":g :U :b . "
    );
    addRules(
        ":R(:a,?X), :S(:b,?Y) :- :T(?X,?Y) . "
        "owl:sameAs(?Y1,?Y2) :- :U(?X,?Y1), :U(?X,?Y2) . "
    );
    applyRules();

    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":T owl:sameAs :T . "
        ":U owl:sameAs :U . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "
        ":e owl:sameAs :e . "
        ":f owl:sameAs :f . "
        ":g owl:sameAs :g . "

        ":a :R :c . "
        ":b :R :c . "
        ":a :R :e . "
        ":b :R :e . "
        ":a :S :d . "
        ":b :S :d . "
        ":a :S :f . "
        ":b :S :f . "
        ":c :T :d . "
        ":e :T :f . "
        ":g :U :a . "
        ":g :U :b . "
    );
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":T owl:sameAs :T . "
        ":U owl:sameAs :U . "

        ":a owl:sameAs :a . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "
        ":e owl:sameAs :e . "
        ":f owl:sameAs :f . "
        ":g owl:sameAs :g . "

        ":a :R :c . "
        ":a :R :e . "
        ":a :S :d . "
        ":a :S :f . "
        ":c :T :d . "
        ":e :T :f . "
        ":g :U :a . "
    );
}

TEST(testChaseBug) {
    addTriples(
        "_:row1 <s:0> \"alpha1\" . "
        "_:row1 <s:1> \"beta\" . "
        "_:row1 <s:2> \"gamma\" . "
        "_:row2 <s:0> \"alpha2\" . "
        "_:row2 <s:1> \"beta\" . "
        "_:row2 <s:2> \"omega\" . "
        "_:row3 <s:0> \"gamma\" . "
        "_:row3 <s:1> \"alpha1\" . "
        "_:row3 <s:2> \"beta\" . "
        "_:row4 <s:0> \"omega\" . "
        "_:row4 <s:1> \"alpha2\" . "
        "_:row4 <s:2> \"psi\" . "
    );
    addRules(
        "[?TP2, <t1:0>, ?a], [?TP2, <t1:1>, ?b], [?TP2, <t1:2>, ?c] :- [?TP1, <s:0>, ?a], [?TP1, <s:1>, ?b], [?TP1, <s:2>, ?c], BIND(SKOLEM(\"t1\",?a,?b,?c) AS ?TP2) . "
        "[?TP2, <t1:0>, ?c], [?TP2, <t1:1>, ?a], [?TP2, <t1:2>, ?C] :- [?TP1, <s:0>, ?a], [?TP1, <s:1>, ?b], [?TP1, <s:2>, ?c], BIND(SKOLEM(\"ex1\",?a,?c) AS ?C), BIND(SKOLEM(\"t1\",?c,?a,?C) AS ?TP2) . "
        "[?a, <t2>, ?b] :- [?TP1, <t1:0>, ?a], [?TP1, <t1:1>, ?b], [?TP1, <t1:2>, ?c] . "
        "[?TP1, <t3:0>, ?a], [?TP1, <t3:1>, ?b], [?TP1, <t3:2>, ?C] :- [?a, <t2>, ?b], BIND(SKOLEM(\"ex2\",?b,?a) AS ?C), BIND(SKOLEM(\"t3\",?a,?b,?C) AS ?TP1) . "
        "[?C1, <http://www.w3.org/2002/07/owl#sameAs>, ?C2] :- [?TP1, <t1:0>, ?a], [?TP1, <t1:1>, ?b], [?TP1, <t1:2>, ?C1], [?TP2, <t1:0>, ?a], [?TP2, <t1:1>, ?b], [?TP2, <t1:2>, ?C2] . "
    );
    applyRules();

    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X0 ?X1 ?X2 WHERE { ?TP0 <t3:0> ?X0 . ?TP0 <t3:1> ?X1 . ?TP0 <t3:2> ?X2 }",
        "\"alpha1\" \"beta\" _:ex2_11_9 . "
        "\"alpha1\" _:ex1_9_13 _:ex2_11_9 . "
        "\"alpha2\" \"beta\" _:ex2_11_15 . "
        "\"alpha2\" _:ex1_9_13 _:ex2_11_15 . "
        "\"beta\" \"gamma\" _:ex2_13_11 . "
        "_:ex1_9_13 \"gamma\" _:ex2_13_11 . "
        "\"gamma\" \"alpha1\" _:ex2_9_13 . "
        "\"omega\" \"alpha2\" _:ex2_15_16 . "
        "\"psi\" \"omega\" _:ex2_16_19 . "
        "_:ex1_15_16 \"omega\" _:ex2_16_19 . "
    );
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X0 ?X1 ?X2 WHERE { ?TP0 <t3:0> ?X0 . ?TP0 <t3:1> ?X1 . ?TP0 <t3:2> ?X2 }",
        "\"alpha1\" \"beta\" _:ex2_11_9 . "
        "\"alpha2\" \"beta\" _:ex2_11_15 . "
        "\"beta\" \"gamma\" _:ex2_13_11 . "
        "\"gamma\" \"alpha1\" _:ex2_9_13 . "
        "\"omega\" \"alpha2\" _:ex2_15_16 . "
        "\"psi\" \"omega\" _:ex2_16_19 . "
    );
}

#endif
