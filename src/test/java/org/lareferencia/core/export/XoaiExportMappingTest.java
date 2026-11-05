package org.lareferencia.core.export;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class XoaiExportMappingTest {
    static final String XML = """
            <metadata xmlns="http://www.lyncode.com/xoai">
              <element name="dc"><element name="title">
                <element name="none"><field name="value"> Title </field><field name="language">en</field></element>
                <element name="alternative"><field name="value">Outro</field><field name="language">pt</field></element>
              </element><element name="identifier"><field name="value">parent</field>
                <element name="dark"><field name="value">ark:/123</field></element>
                <element name="uri"><field name="value">ark:/123</field></element>
              </element><element name="count"><field name="value">123456789012345678901234567890</field></element>
              <element name="flag"><field name="value">true</field></element></element>
            </metadata>
            """;
    static String spec(String fields) { return "{\"specVersion\":1,\"name\":\"test\",\"version\":\"1\",\"inputSchema\":\"xoai\",\"fields\":" + fields + "}"; }
    static XoaiExportMapping mapping(String fields) throws Exception { return new XoaiExportMapping(ExportJson.JSON.readTree(spec(fields))); }
    @Test void groupedOccurrencesKeepLanguagesAndWildcardExcludesParent() throws Exception {
        var mapping = mapping("""
                {"titles":{"forEach":"dc.title.*","fields":{
                   "text":{"select":":value","type":"string","many":false,"normalize":["trim"]},
                   "language":{"select":":language","type":"string","many":false}}},
                 "ids":{"select":"dc.identifier.*","type":"string","unique":true},
                 "dark":{"select":"dc.identifier.dark","type":"string","many":false},
                 "missing":{"select":"dc.missing","type":"string"},
                 "omitted":{"select":"dc.missing","type":"string","many":false},
                 "number":{"select":"dc.count","type":"integer","many":false},
                 "flag":{"select":"dc.flag","type":"boolean","many":false}}
                """);
        var data = mapping.project(XML);
        assertEquals("Title", data.at("/titles/0/text").asText());
        assertEquals("pt", data.at("/titles/1/language").asText());
        assertEquals(1, data.path("ids").size());
        assertEquals("ark:/123", data.path("dark").asText());
        assertEquals(0, data.path("missing").size()); assertFalse(data.has("omitted"));
        assertEquals("123456789012345678901234567890", data.path("number").asText());
        assertTrue(data.path("flag").asBoolean());
        assertEquals(data, mapping.project(XML.replace(" xmlns=\"http://www.lyncode.com/xoai\"", "")));
    }
    @Test void invalidMappingsAndRecordErrorsAreDistinct() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> mapping("{\"x\":{\"select\":\"dc.'bad\",\"type\":\"string\"}}"));
        assertThrows(IllegalArgumentException.class, () -> mapping("{\"x\":{\"select\":\"dc.title\",\"type\":\"decimal\"}}"));
        assertThrows(IllegalArgumentException.class, () -> mapping("{\"x\":{\"select\":\"dc.title\",\"type\":\"string\",\"typo\":1}}"));
        var scalar = mapping("{\"x\":{\"select\":\"dc.title.*\",\"type\":\"string\",\"many\":false}}");
        assertThrows(IllegalArgumentException.class, () -> scalar.project(XML));
        assertThrows(IllegalArgumentException.class, () -> mapping("{\"x\":{\"select\":\"dc.title.*\",\"type\":\"boolean\"}}").project(XML));
        assertThrows(Exception.class, () -> scalar.project("<!DOCTYPE metadata [<!ENTITY e SYSTEM 'file:///etc/passwd'>]><metadata>&e;</metadata>"));
        assertThrows(IllegalArgumentException.class, () -> scalar.project("<record/>"));
        assertThrows(IllegalArgumentException.class, () -> mapping("{\"x\":{\"select\":\"dc.title\",\"type\":\"string\",\"maxLength\":-1}}"));
    }

    @Test void maxLengthSkipsOversizedValuesAfterNormalizationAndCountsUnicodeCodePoints() throws Exception {
        var projection = mapping("""
            {"rights":{"select":"dc.rights.*","type":"string","normalize":["trim"],"maxLength":8}}
            """);
        String xml = """
            <metadata xmlns="http://www.lyncode.com/xoai"><element name="dc"><element name="rights">
            <element name="license"><field name="value">  CC BY  </field></element>
            <element name="text"><field name="value">This is a very long copyright text</field></element>
            <element name="unicode"><field name="value">😀😀😀😀😀😀😀😀😀</field></element>
            </element></element></metadata>
            """;
        var rights = projection.project(xml).path("rights");
        assertEquals(1, rights.size());
        assertEquals("CC BY", rights.get(0).asText());
    }
    @Test void sequentialOccurrencesKeepAttributesWithTheirOwnValue() throws Exception {
        var mapping = mapping("""
            {"titles":{"forEach":"dc.title.*","occurrenceStart":"value","fields":{
              "value":{"select":":value","type":"string","many":false},
              "language":{"select":":language","type":"string","many":false},
              "scheme":{"select":":scheme","type":"string","many":false}}}}
            """);
        String xml = """
            <metadata xmlns="http://www.lyncode.com/xoai"><element name="dc"><element name="title"><element name="none">
            <field name="value">Español</field><field name="language">spa</field><field name="scheme">custom</field>
            <field name="value">English</field><field name="language">eng</field><field name="value"></field>
            </element></element></element></metadata>
            """;
        var data = mapping.project(xml);
        assertEquals(3, data.path("titles").size());
        assertEquals("spa", data.at("/titles/0/language").asText());
        assertEquals("custom", data.at("/titles/0/scheme").asText());
        assertEquals("eng", data.at("/titles/1/language").asText());
        assertFalse(data.path("titles").get(1).has("scheme"));
        assertEquals("", data.at("/titles/2/value").asText());
        assertFalse(data.path("titles").get(2).has("language"));
        assertThrows(IllegalArgumentException.class, () -> mapping.project(xml.replace(
            "<field name=\"value\">Español</field>", "<field name=\"language\">unknown</field>")));
        assertThrows(IllegalArgumentException.class, () -> mapping.project(xml.replace(
            "<field name=\"language\">eng</field>", "<field name=\"language\">eng</field><field name=\"language\">spa</field>")));
        assertThrows(IllegalArgumentException.class, () -> mapping("""
            {"x":{"forEach":"dc.title.*","occurrenceStart":"value","fields":{
               "bad":{"select":"nested:value","type":"string"}}}}
            """));
    }

    @Test void mergedSelectorsPreserveOrderContextAndDoNotDuplicateOverlaps() throws Exception {
        var projection = mapping("""
            {"titles":{"forEach":["dc.title.*","dc.title.none","dcterms.title.*"],
              "occurrenceStart":"value","includeContext":["schema","qualifier","path"],"fields":{
              "value":{"select":":value","type":"string","many":false},
              "language":{"select":":language","type":"string","many":false}}}}
            """);
        String xml = """
            <metadata xmlns="http://www.lyncode.com/xoai">
             <element name="dcterms"><element name="title"><element name="alternative">
              <field name="value">Alternative first</field></element></element></element>
             <element name="dc"><element name="title"><element name="none">
              <field name="value">Español</field><field name="language">spa</field>
              <field name="value">English</field><field name="language">eng</field>
             </element></element></element>
            </metadata>
            """;
        var titles = projection.project(xml).path("titles");
        assertEquals(3, titles.size());
        assertEquals("dcterms", titles.get(0).path("schema").asText());
        assertEquals("alternative", titles.get(0).path("qualifier").asText());
        assertEquals("dc.title.none", titles.get(1).path("path").asText());
        assertFalse(titles.get(1).has("qualifier"));
        assertEquals("eng", titles.get(2).path("language").asText());
        assertEquals("dc", titles.get(2).path("schema").asText());
    }

    @Test void invalidContextConfigurationsAreRejected() throws Exception {
        for (String rule : java.util.List.of(
                "{\"forEach\":[],\"fields\":{}}",
                "{\"forEach\":[\"dc.title.*\",\"dc.title.*\"],\"fields\":{}}",
                "{\"forEach\":\"dc.title.*\",\"includeContext\":[\"unknown\"],\"fields\":{}}",
                "{\"forEach\":\"dc.title.*\",\"includeContext\":[\"schema\"],\"fields\":{\"schema\":{\"select\":\":value\",\"type\":\"string\"}}}")) {
            assertThrows(IllegalArgumentException.class, () -> mapping("{\"x\":" + rule + "}"));
        }
    }

    @Test void canonicalHashSortsObjectsButPreservesArrays() throws Exception {
        var a = ExportJson.JSON.readTree("{\"b\":[1,2],\"a\":{\"y\":1,\"x\":2}}");
        var b = ExportJson.JSON.readTree("{ \"a\": {\"x\":2,\"y\":1}, \"b\":[1,2] }");
        assertEquals(XoaiExportMapping.canonicalHash(a), XoaiExportMapping.canonicalHash(b));
        assertNotEquals(XoaiExportMapping.canonicalHash(a), XoaiExportMapping.canonicalHash(ExportJson.JSON.readTree("{\"b\":[2,1],\"a\":{\"y\":1,\"x\":2}}")));
    }
}
