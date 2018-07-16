// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   TurtleTokenizerTest

#include <CppTest/AutoTest.h>

#include "../../src/formats/sources/MemorySource.h"
#include "../../src/formats/turtle/TurtleTokenizer.h"

class TurtleTokenizerTest {

protected:

    std::string m_data;
    std::unique_ptr<MemorySource> m_memorySource;
    TurtleTokenizer m_tokenizer;

    void startTokenizer(const std::string& string) {
        m_data = string;
        m_memorySource.reset(new MemorySource(m_data.c_str(), m_data.length()));
        m_tokenizer.initialize(*m_memorySource);
        ASSERT_TRUE(m_tokenizer.isNoToken());
        m_tokenizer.nextToken();
    }

    void assertToken(const char * const fileName, const long lineNumber, const TurtleTokenizer::TokenType tokenType, const std::string& token) {
        CppTest::assertEqual(tokenType, m_tokenizer.getTokenType(), fileName, lineNumber);
        std::string tokenValue;
        m_tokenizer.getToken(tokenValue);
        CppTest::assertEqual(token, tokenValue, fileName, lineNumber);
        m_tokenizer.nextToken();
    }

    void assertToken2(const char * const fileName, const long lineNumber, const TurtleTokenizer::TokenType tokenType, const std::string& token, const size_t tokenStartLine, const size_t tokenStartColumn) {
        CppTest::assertEqual(tokenType, m_tokenizer.getTokenType(), fileName, lineNumber);
        std::string tokenValue;
        m_tokenizer.getToken(tokenValue);
        CppTest::assertEqual(token, tokenValue, fileName, lineNumber);
        CppTest::assertEqual(tokenStartLine, m_tokenizer.getTokenStartLine(), fileName, lineNumber);
        CppTest::assertEqual(tokenStartColumn, m_tokenizer.getTokenStartColumn(), fileName, lineNumber);
        m_tokenizer.nextToken();
    }

public:

    TurtleTokenizerTest() : m_data(), m_memorySource(), m_tokenizer() {
    }

};

#define ASSERT_TOKEN(tokenType, token) \
    assertToken(__FILE__, __LINE__, tokenType, token)

#define ASSERT_TOKEN2(tokenType, token, tokenStartLine, tokenStartColumn) \
    assertToken2(__FILE__, __LINE__, tokenType, token, tokenStartLine, tokenStartColumn)

TEST(testBasic) {
    startTokenizer("abc \"qstring\\\"quote\" d12  123 <URI://hp> a:gg\ta:tt:ee\t: ns:-_:abc \n@base :ln-gg tr \"ds\"^^<dt> \"kk\"^^rdf:blah");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "qstring\"quote");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "d12");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, "123");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_IRI, "URI://hp");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_LN, "a:gg");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_LN, "a:tt:ee");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_NS, ":");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_NS, "ns:");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "-");
    ASSERT_TOKEN(TurtleTokenizer::BLANK_NODE, "_:abc");
    ASSERT_TOKEN(TurtleTokenizer::LANGUAGE_TAG, "@base");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_LN, ":ln-gg");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "tr");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "ds");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "^^");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_IRI, "dt");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "kk");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "^^");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_LN, "rdf:blah");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
    m_tokenizer.nextToken();
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testPunctuation) {
    startTokenizer("abc.;abc.a,def .\"gre\";@blk-ab--:-a");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ".");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ";");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc.a");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ",");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "def");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ".");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "gre");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ";");
    ASSERT_TOKEN(TurtleTokenizer::LANGUAGE_TAG, "@blk-ab");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "-");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "-");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ":-");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "a");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testComment) {
    startTokenizer("abc # comment to eoln\nmore # another comment\n\rand more # sdfsdf");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "more");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "and");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "more");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testEmpty1) {
    startTokenizer("");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testEmpty2) {
    startTokenizer("   ");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testEmpty3) {
    startTokenizer("# sdfsdf");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testPositionTracking) {
    startTokenizer("abc  def\r\nghi\nqwe\r\n\r\n\r\n yui");
    ASSERT_TOKEN2(TurtleTokenizer::SYMBOL, "abc", 1, 1);
    ASSERT_TOKEN2(TurtleTokenizer::SYMBOL, "def", 1, 6);
    ASSERT_TOKEN2(TurtleTokenizer::SYMBOL, "ghi", 2, 1);
    ASSERT_TOKEN2(TurtleTokenizer::SYMBOL, "qwe", 3, 1);
    ASSERT_TOKEN2(TurtleTokenizer::SYMBOL, "yui", 6, 2);
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testEOLInString) {
    startTokenizer("\"ab\ncd\"");
    ASSERT_EQUAL(TurtleTokenizer::ERROR_TOKEN, m_tokenizer.getTokenType());
}

TEST(testOtherQuoteInString1) {
    startTokenizer("\"ab'cd\"");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "ab'cd");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testOtherQuoteInString2) {
    startTokenizer("'ab\"cd'");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "ab\"cd");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testLongString1) {
    startTokenizer("    \"\"\"long string\"\"\"");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "long string");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testLongString2) {
    startTokenizer("\"\"\"\"");
    ASSERT_EQUAL(TurtleTokenizer::ERROR_TOKEN, m_tokenizer.getTokenType());
}

TEST(testLongString3) {
    startTokenizer("\"\"\"sdf sdf\"");
    ASSERT_EQUAL(TurtleTokenizer::ERROR_TOKEN, m_tokenizer.getTokenType());
}

TEST(testLongString4) {
    startTokenizer("\"\"\"sdf\"\"\n\" '''sdf\"\"\"abc");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "sdf\"\"\n\" '''sdf");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testConsecutiveStrings1) {
    startTokenizer("\"\" \"\"");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testConsecutiveStrings2) {
    startTokenizer("''''''''''''");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testStringSymbolString) {
    startTokenizer("\"\"a\"\"");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "a");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testWithTrailingWS) {
    startTokenizer("abc  ");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testEmpty) {
    startTokenizer("  ");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testIRIvsLessThan) {
    startTokenizer("<=abc=><= <abc <=def");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_IRI, "=abc=");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "<=");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "<");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "<=");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "def");
}

TEST(testNumbers) {
    startTokenizer("..0.0E+3 3. 4 5E+");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ".");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, ".0");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, ".0E+3");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, "3");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ".");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, "4");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, "5");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "E");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "+");
}

TEST(testUnicodeInString) {
    startTokenizer("\xC3\x81guila ol\xC3\xA9 \"tr\\u00E4nen\\u00fCberstr\\U000000f6mt\"");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "\xC3\x81guila");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "ol\xC3\xA9");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_STRING, "tr""\xC3\xA4""nen\xC3\xBC""berstr""\xC3\xB6""mt");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testUnicodeInQuotedIRI) {
    startTokenizer("<\xC3\x81guila-ol\xC3\xA9>");
    ASSERT_TOKEN(TurtleTokenizer::QUOTED_IRI, "\xC3\x81guila-ol\xC3\xA9");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testVariables) {
    startTokenizer("abc$var1-$var2");
    ASSERT_TOKEN(TurtleTokenizer::SYMBOL, "abc");
    ASSERT_TOKEN(TurtleTokenizer::VARIABLE, "$var1");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "-");
    ASSERT_TOKEN(TurtleTokenizer::VARIABLE, "$var2");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testBlankNodes) {
    startTokenizer("abc_:def _:bn:");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_LN, "abc_:def");
    ASSERT_TOKEN(TurtleTokenizer::BLANK_NODE, "_:bn");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_NS, ":");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testEscapedSymbols) {
    startTokenizer(":\\*%Ab\\=.");
    ASSERT_TOKEN(TurtleTokenizer::PNAME_LN, ":\\*%Ab\\=");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, ".");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

TEST(testLanguageTag) {
    startTokenizer("@ab12 @ab-ab12-");
    ASSERT_TOKEN(TurtleTokenizer::LANGUAGE_TAG, "@ab");
    ASSERT_TOKEN(TurtleTokenizer::NUMBER, "12");
    ASSERT_TOKEN(TurtleTokenizer::LANGUAGE_TAG, "@ab-ab12");
    ASSERT_TOKEN(TurtleTokenizer::NON_SYMBOL, "-");
    ASSERT_EQUAL(TurtleTokenizer::EOF_TOKEN, m_tokenizer.getTokenType());
}

#endif
