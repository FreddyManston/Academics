// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   DatalogParserTest

#include <CppTest/AutoTest.h>

#include "../../src/RDFStoreException.h"
#include "../../src/formats/datalog/DatalogParser.h"
#include "../../src/formats/sources/MemorySource.h"
#include "../../src/util/InputImporter.h"
#include "../logic/AbstractLogicTest.h"

class DatalogParserTest : public AbstractLogicTest {

protected:

    DatalogProgram m_datalogProgram;
    std::vector<Atom> m_facts;

    DatalogParserTest() : m_datalogProgram() {
    }

    void printProgram(std::vector<Rule>& rules, Prefixes& prefixes) const {
        std::cout << std::endl;
        for (size_t ruleIndex = 0; ruleIndex < rules.size(); ruleIndex++)
            std::cout << rules[ruleIndex]->toString(prefixes) << std::endl;
        std::cout << std::endl;
    }

    void parse(const char* const programText) {
        DatalogProgramInputImporter datalogProgramInputImporter(m_factory, m_datalogProgram, m_facts, nullptr);
        MemorySource memorySource(programText, ::strlen(programText));
        Prefixes prefixes;
        DatalogParser parser(prefixes);
        parser.bind(memorySource);
        parser.parse(m_factory, datalogProgramInputImporter);
        parser.unbind();
    }

};

TEST(testFilterBind) {
    parse(
        "PREFIX p: <a#>\n"
        "p:A(?X) :- FILTER ?X < ?Y , BIND (STRLEN(?X) AS ?Z).\n"
    );
    ASSERT_EQUAL(1, m_datalogProgram.size());
    ASSERT_EQUAL(m_datalogProgram[0],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A"),
            LA() << FLT(
                BBFC("internal$less-than", V("X"), V("Y"))
            ) << BND(
                UBFC("STRLEN", V("X")),
                V("Z")
            )
        )
    );
}

TEST(testRDFAtoms) {
    parse(
        "PREFIX p: <a#>\n"
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "[?X,rdf:type,p:C] :- [?X,p:R,?Y], FILTER ?X < ?Y, BIND (STRLEN(?X) AS ?Z) .\n"
    );
    ASSERT_EQUAL(1, m_datalogProgram.size());
    ASSERT_EQUAL(m_datalogProgram[0],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#C"),
            LA() << RA("?X", "a#R", "?Y") <<
            FLT(
                BBFC("internal$less-than", V("X"), V("Y"))
            ) << BND(
                UBFC("STRLEN", V("X")), V("Z")
            )
        )
    );
}

TEST(testNegation) {
    parse(
        "PREFIX p: <a#>\n"
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "[?X,rdf:type,p:C] :- [?X,rdf:type,p:A], NOT p:D(?X) .\n"
        "[?X,rdf:type,p:C] :- [?X,rdf:type,p:A], NOT EXISTS ?Y IN p:R(?X,?Y) .\n"
        "[?X,rdf:type,p:C] :- [?X,rdf:type,p:A], NOT EXIST ?Y, ?Z IN(p:R(?X,?Y), p:S(?Y,?Z)) .\n"
    );
    ASSERT_EQUAL(3, m_datalogProgram.size());
    ASSERT_EQUAL(m_datalogProgram[0],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#C"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A") <<
            NOT(
                RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#D")
            )
        )
    );
    ASSERT_EQUAL(m_datalogProgram[1],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#C"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A") <<
            NOT(
                VA() << V("Y"),
                AFA() << RA("?X", "a#R", "?Y")
            )
        )
    );
    ASSERT_EQUAL(m_datalogProgram[2],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#C"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A") <<
            NOT(
                VA() << V("Y") << V("Z"),
                AFA() << RA("?X", "a#R", "?Y") << RA("?Y", "a#S", "?Z")
            )
        )
    );
}

TEST(testAggregate) {
    parse(
        "PREFIX : <a#>\n"
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "[?X,:minT,?MT], [?X,:distCount,?TC] :- [?X,rdf:type,:City], AGGREGATE([?X,:tempReading,?R], [?R,:value,?T] ON ?X BIND MIN(?T) AS ?MT BIND :myCount(DISTINCT ?T) AS ?TC) .\n"
    );
    ASSERT_EQUAL(1, m_datalogProgram.size());
    ASSERT_EQUAL(m_datalogProgram[0],
        R(
            AA() << RA("?X", "a#minT", "?MT") << RA("?X", "a#distCount", "?TC"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#City") << AGG(
                AFA() << RA("?X", "a#tempReading", "?R") << RA("?R", "a#value", "?T"), false, VA() << V("X"),
                AGGBNDA() << AGGBND("MIN", false, BEA() << V("T"), V("MT")) << AGGBND("a#myCount", true, BEA() << V("T"), V("TC"))
            )
        )
    );
}

TEST(testNotExists) {
    parse(
        "PREFIX : <a#>\n"
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "[?X,rdf:type,:A] :- [?X,rdf:type,:B], FILTER(NOT EXISTS { [?X,rdf:type,:A] }) .\n"
        "[?X,rdf:type,:A] :- [?X,rdf:type,:B], FILTER(NOT EXISTS { [?X,rdf:type,:A], [?X,rdf:type,:C] }) .\n"
        "[?X,rdf:type,:A] :- [?X,rdf:type,:B], FILTER(NOT EXISTS { SELECT ?X WHERE { [?X,:R,?Y] } }) .\n"
        "[?X,rdf:type,:A] :- [?X,rdf:type,:B], FILTER(NOT EXISTS { SELECT ?X WHERE { [?X,:R,?Y], [?X,:S,?Y] } }) .\n"
    );
    ASSERT_EQUAL(4, m_datalogProgram.size());
    ASSERT_EQUAL(m_datalogProgram[0],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#B") <<
            FLT(
                NOT_EXISTS(
                    RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A")
                )
            )
        )
    );
    ASSERT_EQUAL(m_datalogProgram[1],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#B") <<
            FLT(
                NOT_EXISTS(
                    AND(
                        FA() <<
                        RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A") <<
                        RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#C")
                    )
                )
            )
        )
    );
    ASSERT_EQUAL(m_datalogProgram[2],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#B") <<
            FLT(
                NOT_EXISTS(
                    Q(false, TA() << V("X"),
                        RA("?X", "a#R", "?Y")
                    )
                )
            )
        )
    );
    ASSERT_EQUAL(m_datalogProgram[3],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A"),
            LA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#B") <<
            FLT(
                NOT_EXISTS(
                    Q(false, TA() << V("X"),
                        AND(
                            FA() << RA("?X", "a#R", "?Y") << RA("?X", "a#S", "?Y")
                        )
                    )
                )
            )
        )
    );
}

TEST(testDatalog) {
    parse(
        "PREFIX p: <a#>\n"
        "p:A(?X) :- p:R(?X,?Y) .\n"
        "p:R(?X,?Z) :- p:R(?X,?Y) , p:R(?Y,?Z) .\n"
        "p:A(?X), p:R(?X,?Y) :- p:S(?X,?Y) .\n"
    );
    ASSERT_EQUAL(3, m_datalogProgram.size());
    ASSERT_EQUAL(m_datalogProgram[0],
        R(
            RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A"),
            LA() << RA("?X", "a#R", "?Y")
        )
    );
    ASSERT_EQUAL(m_datalogProgram[1],
        R(
            RA("?X", "a#R", "?Z"),
            LA() << RA("?X", "a#R", "?Y") << RA("?Y", "a#R", "?Z")
        )
    );
    ASSERT_EQUAL(m_datalogProgram[2],
        R(
            AA() << RA("?X", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a#A") << RA("?X", "a#R", "?Y"),
            LA() << RA("?X", "a#S", "?Y")
        )
    );
}

TEST(testLargerDatalog) {
    parse(
        "PREFIX a: <http://www.ihtsdo.org/snomedct.owl#> "
        "a:Atropine(?X) :- a:Atropine_sulfate(?X) ."
        "a:Order_Onygenales(?X) :- a:Family_Arthrodermataceae(?X) ."
        "a:Protein-tyrosine_kinase_inhibitor(?X) :- a:Sunitinib(?X) ."
        "a:Region_of_prostate(?X) :- a:Surface_of_prostate(?X) ."
        "a:Beta-hemolytic_streptococcus(?X) :- a:Beta-hemolytic_Streptococcus_group_A(?X) ."
        "a:Hematologic_neoplasm_of_uncertain_behavior(?X) :- a:Mast_cell_nevus(?X) ."
        "a:Mast_cell_neoplasm(?X) :- a:Mast_cell_nevus(?X) ."
        "a:Neoplasm_of_uncertain_behavior_of_skin(?X) :- a:Mast_cell_nevus(?X) ."
        "a:Mast_cell_nevus(?X) :- a:Hematologic_neoplasm_of_uncertain_behavior(?X) , a:Mast_cell_neoplasm(?X) , a:Neoplasm_of_uncertain_behavior_of_skin(?X) , a:RoleGroup(?X,?X1) , a:Associated_morphology(?X1,?X3) , a:Mastocytoma(?X3) , a:Finding_site(?X1,?X2) , a:Skin_structure(?X2) ."
        "a:Skin_of_part_of_hand(?X) :- a:Skin_of_surface_of_hand(?X) ."
        "a:Stages(?X) :- a:Stage_2(?X) ."
        "a:Innominate_bone_structure(?X) :- a:Bone_structure_of_pubis(?X) ."
        "a:Neck_structure(?X) :- a:Cervical_nervous_structure(?X) ."
        "a:Isotope_static_scan_of_bone_marrow(?X) :- a:Radioisotope_scan_of_bone_marrow(?X) ."
        "a:Radioisotope_scan_of_bone_marrow(?X) :- a:Isotope_static_scan_of_bone_marrow(?X) , a:RoleGroup(?X,?X1) , a:Method(?X1,?X4) , a:Radionuclide_imaging_-_action(?X4) , a:Procedure_site_-_Direct(?X1,?X3) , a:Bone_marrow_structure(?X3) , a:Using_substance(?X1,?X2) , a:Radioactive_isotope(?X2) ."
        "a:Bacterial_sepsis_of_newborn(?X) :- a:Sepsis_of_newborn_due_to_Staphylococcus_aureus(?X) ."
        "a:Infection_due_to_Staphylococcus_aureus(?X) :- a:Sepsis_of_newborn_due_to_Staphylococcus_aureus(?X) ."
        "a:Sepsis_of_newborn_due_to_Staphylococcus_aureus(?X) :- a:Bacterial_sepsis_of_newborn(?X) , a:Infection_due_to_Staphylococcus_aureus(?X) , a:RoleGroup(?X,?X5) , a:Causative_agent(?X5,?X6) , a:Staphylococcus_aureus(?X6) , a:RoleGroup(?X,?X3) , a:Finding_site(?X3,?X4) , a:Anatomical_structure(?X4) , a:RoleGroup(?X,?X1) , a:Occurrence(?X1,?X2) , a:Neonatal(?X2) ."
        "a:Benign_neoplasm_of_female_breast(?X) :- a:Benign_neoplasm_of_nipple_of_female_breast(?X) ."
        "a:Neoplasm_of_nipple_of_female_breast(?X) :- a:Benign_neoplasm_of_nipple_of_female_breast(?X) ."
        "a:Benign_neoplasm_of_nipple_of_female_breast(?X) :- a:Benign_neoplasm_of_female_breast(?X) , a:Neoplasm_of_nipple_of_female_breast(?X) , a:RoleGroup(?X,?X1) , a:Associated_morphology(?X1,?X4) , a:Neoplasm_benign(?X4) , a:Finding_site(?X1,?X3) , a:Female_mammary_gland_structure(?X3) , a:Finding_site(?X1,?X2) , a:Nipple_structure(?X2) ."
    );
    ASSERT_EQUAL(21, m_datalogProgram.size());
}

#endif
