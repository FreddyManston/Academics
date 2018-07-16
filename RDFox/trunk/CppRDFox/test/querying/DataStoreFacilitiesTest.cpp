// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME      DataStoreFacilitiesTest
#define FIXTURE_NAME    DataStoreTest

#include <CppTest/AutoTest.h>

#include "../../src/util/File.h"
#include "DataStoreTest.h"

TEST(testSaveLoad) {
    const char* const text =
        ":i1 a :A1 ."
        ":i1 a :A2 ."
        ":i2 a :A2 ."
        ":i3 a :A1 ."
        ":i3 a :A3 ."
        ":i1 :prop :i2 ."
        ":i2 :prop :i2 ."
        ":i1 :dprop \"text1\" ."
        ":i1 :dprop \"text2\" ."
        ":i2 :prop _:bn ."
        "_:bn :prop _:bn ."
    ;

    addTriples(text);

    ASSERT_QUERY("SELECT ?S ?P ?O WHERE { ?S ?P ?O }", text);

    // Formatted test
    File formattedFile;
    formattedFile.open("TestStore.fmt", File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, true, true, false, true);
    File::OutputStreamType formattedOutputStream(formattedFile);
    m_dataStore->saveFormatted(formattedOutputStream);

    m_dataStore->initialize();
    ASSERT_QUERY("SELECT ?S ?P ?O WHERE { ?S ?P ?O }", "");

    formattedFile.rewind();
    File::InputStreamType formattedInputStream(formattedFile);
    m_dataStore->loadFormatted(formattedInputStream, false, false);
    ASSERT_QUERY("SELECT ?S ?P ?O WHERE { ?S ?P ?O }", text);
    formattedFile.close();

    // Unformatted test
    File unformattedFile;
    unformattedFile.open("TestStore.ufmt", File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, true, true, false, true);
    File::OutputStreamType unformattedOutputStream(unformattedFile);
    m_dataStore->saveUnformatted(unformattedOutputStream);

    m_dataStore->initialize();
    ASSERT_QUERY("SELECT ?S ?P ?O WHERE { ?S ?P ?O }", "");

    unformattedFile.rewind();
    File::InputStreamType unformattedInputStream(unformattedFile);
    m_dataStore->loadUnformatted(unformattedInputStream, false, 1);
    ASSERT_QUERY("SELECT ?S ?P ?O WHERE { ?S ?P ?O }", text);
    unformattedFile.close();
}

TEST(testReindex) {
    addRules(
        ":B(?X) :- :A(?X) . "
    );
    addTriples(
        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY(
        "SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
        ":i1 rdf:type :B . "
        ":i2 rdf:type :B . "
    );
    m_dataStore->reindex(false);
    ASSERT_QUERY(
        "SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
        ":i1 rdf:type :B . "
        ":i2 rdf:type :B . "
    );
    m_dataStore->reindex(true);
    ASSERT_QUERY(
        "SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
    );
}

#endif
