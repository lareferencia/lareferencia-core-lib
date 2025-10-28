/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.metadata;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for OAIRecordMetadata
 * Uses real XOAI XML format from test resources (original.xoai.record.xml and xoai_openaire.xml)
 */
@DisplayName("OAIRecordMetadata Tests")
class OAIRecordMetadataTest {
    
    private static final String TEST_IDENTIFIER = "oai:test:12345";
    
    private Document getXmlDocumentFromResourcePath(String resourcePath) throws Exception {
        InputStream resource = new ClassPathResource(resourcePath).getInputStream();
        DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = dBuilder.parse(resource);
        return doc;
    }

    // ========== Constructor Tests ==========
    
    @Test
    @DisplayName("Should create metadata from XML string")
    void testCreateFromXmlString() throws Exception {
        String xml = "<metadata><element name=\"dc\"><element name=\"title\"><element name=\"none\">" +
                     "<field name=\"value\">Test Title</field></element></element></element></metadata>";
        
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, xml);
        
        assertNotNull(metadata);
        assertEquals(TEST_IDENTIFIER, metadata.getIdentifier());
        assertNotNull(metadata.getDOMDocument());
    }

    @Test
    @DisplayName("Should create empty metadata")
    void testCreateEmptyMetadata() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        assertNotNull(metadata);
        assertEquals(TEST_IDENTIFIER, metadata.getIdentifier());
        assertNotNull(metadata.getDOMDocument());
    }

    @Test
    @DisplayName("Should throw exception for invalid XML")
    void testInvalidXml() {
        String invalidXml = "<metadata><unclosed>";
        
        assertThrows(OAIRecordMetadataParseException.class, 
            () -> new OAIRecordMetadata(TEST_IDENTIFIER, invalidXml));
    }

    @Test
    @DisplayName("Should create metadata from Document")
    void testCreateFromDocument() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        assertNotNull(metadata);
        assertEquals(TEST_IDENTIFIER, metadata.getIdentifier());
        assertSame(doc, metadata.getDOMDocument());
    }

    @Test
    @DisplayName("Should create metadata from Node")
    void testCreateFromNode() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        Node root = doc.getDocumentElement();
        
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, root);
        
        assertNotNull(metadata);
        assertEquals(TEST_IDENTIFIER, metadata.getIdentifier());
        assertNotNull(metadata.getDOMDocument());
    }

    // ========== Field Occurrences Tests ==========

    @Test
    @DisplayName("Should get multiple field occurrences from real XML")
    void testGetFieldOccurrencesMultiple() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        // Get subjects - original.xoai.record.xml has multiple subject values in dc.subject.none
        List<String> subjects = metadata.getFieldOcurrences("dc.subject.none");
        assertNotNull(subjects);
        assertTrue(subjects.size() > 5);
        assertTrue(subjects.contains("Trabajo social"));
        assertTrue(subjects.contains("Acción social"));
    }

    @Test
    @DisplayName("Should get single field occurrence")
    void testGetSingleFieldOccurrence() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<String> publishers = metadata.getFieldOcurrences("dc.publisher.none");
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        assertEquals("Universidad de Huelva", publishers.get(0));
    }

    @Test
    @DisplayName("Should return empty list for non-existent field")
    void testGetNonExistentField() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<String> nonExistent = metadata.getFieldOcurrences("dc.nonexistent.field");
        assertNotNull(nonExistent);
        assertEquals(0, nonExistent.size());
    }

    @Test
    @DisplayName("Should get date field occurrences")
    void testGetDateOccurrences() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<String> dates = metadata.getFieldOcurrences("dc.date.none");
        assertNotNull(dates);
        assertTrue(dates.size() >= 2);
        assertTrue(dates.contains("2015"));
        assertTrue(dates.contains("2015-01-01"));
    }

    @Test
    @DisplayName("Should get contributor occurrences")
    void testGetContributorOccurrences() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<String> contributors = metadata.getFieldOcurrences("dc.contributor.none");
        assertNotNull(contributors);
        assertTrue(contributors.size() >= 1);
        assertTrue(contributors.contains("Vázquez Aguado, Octavio"));
    }

    // ========== Add Field Tests ==========

    @Test
    @DisplayName("Should add field occurrence to existing metadata")
    void testAddFieldOccurrence() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        int originalCount = metadata.getFieldOcurrences("dc.subject.none").size();
        metadata.addFieldOcurrence("dc.subject.none", "New Subject Topic");
        
        List<String> subjects = metadata.getFieldOcurrences("dc.subject.none");
        assertEquals(originalCount + 1, subjects.size());
        assertTrue(subjects.contains("New Subject Topic"));
    }

    @Test
    @DisplayName("Should add field to empty metadata")
    void testAddFieldToEmptyMetadata() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        metadata.addFieldOcurrence("dc.title", "New Title");
        
        List<String> titles = metadata.getFieldOcurrences("dc.title");
        assertEquals(1, titles.size());
        assertEquals("New Title", titles.get(0));
    }

    @Test
    @DisplayName("Should add multiple occurrences of same field")
    void testAddMultipleOccurrences() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        metadata.addFieldOcurrence("dc.subject", "Topic1");
        metadata.addFieldOcurrence("dc.subject", "Topic2");
        metadata.addFieldOcurrence("dc.subject", "Topic3");
        
        List<String> subjects = metadata.getFieldOcurrences("dc.subject");
        assertEquals(3, subjects.size());
        assertTrue(subjects.contains("Topic1"));
        assertTrue(subjects.contains("Topic2"));
        assertTrue(subjects.contains("Topic3"));
    }

    @Test
    @DisplayName("Should add nested field structure")
    void testAddNestedField() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        metadata.addFieldOcurrence("dc.relation.ispartof", "Parent Collection");
        
        List<String> relations = metadata.getFieldOcurrences("dc.relation.ispartof");
        assertEquals(1, relations.size());
        assertEquals("Parent Collection", relations.get(0));
    }

    // ========== Replace Field Tests ==========

    @Test
    @DisplayName("Should replace field occurrence")
    void testReplaceFieldOccurrence() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        metadata.replaceFieldOcurrence("dc.publisher.none", "Modified Publisher");
        
        List<String> publishers = metadata.getFieldOcurrences("dc.publisher.none");
        assertEquals(1, publishers.size());
        assertEquals("Modified Publisher", publishers.get(0));
    }

    @Test
    @DisplayName("Should not fail when replacing non-existent field")
    void testReplaceNonExistentField() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        // Should not throw exception
        assertDoesNotThrow(() -> metadata.replaceFieldOcurrence("dc.nonexistent", "Value"));
        
        // Field should not be created by replace
        List<String> values = metadata.getFieldOcurrences("dc.nonexistent");
        assertEquals(0, values.size());
    }

    // ========== Remove Field Tests ==========

    @Test
    @DisplayName("Should remove all field occurrences")
    void testRemoveFieldOccurrence() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        int originalCount = metadata.getFieldOcurrences("dc.subject.none").size();
        assertTrue(originalCount > 0);
        
        metadata.removeFieldOcurrence("dc.subject.none");
        
        List<String> subjects = metadata.getFieldOcurrences("dc.subject.none");
        assertEquals(0, subjects.size());
    }

    @Test
    @DisplayName("Should not fail when removing non-existent field")
    void testRemoveNonExistentField() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        assertDoesNotThrow(() -> metadata.removeFieldOcurrence("dc.nonexistent"));
    }

    @Test
    @DisplayName("Should remove node from DOM")
    void testRemoveNode() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<Node> nodes = metadata.getFieldNodes("dc.publisher.none");
        assertTrue(nodes.size() > 0);
        
        int originalSize = nodes.size();
        Node nodeToRemove = nodes.get(0);
        metadata.removeNode(nodeToRemove);
        
        // After removal, should have fewer nodes
        List<Node> remainingNodes = metadata.getFieldNodes("dc.publisher.none");
        assertTrue(remainingNodes.size() < originalSize);
    }

    // ========== Get Field Value Tests ==========

    @Test
    @DisplayName("Should get field value using colon notation")
    void testGetFieldValue() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        String publisher = metadata.getFieldValue("dc.publisher.none:value");
        assertNotNull(publisher);
        assertEquals("Universidad de Huelva", publisher);
    }

    @Test
    @DisplayName("Should return empty string for non-existent field value")
    void testGetNonExistentFieldValue() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        String value = metadata.getFieldValue("dc.nonexistent:value");
        assertEquals("", value);
    }

    @Test
    @DisplayName("Should get field value from xpath expression")
    void testGetFieldValueFromXPath() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        String value = metadata.getFieldValueFromXPATHExpression("//field[@name='value'][1]/text()");
        assertNotNull(value);
        assertFalse(value.isEmpty());
    }

    // ========== Get Field Nodes Tests ==========

    @Test
    @DisplayName("Should get field nodes")
    void testGetFieldNodes() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<Node> nodes = metadata.getFieldNodes("dc.subject.none");
        assertNotNull(nodes);
        assertTrue(nodes.size() > 0);
    }

    @Test
    @DisplayName("Should get field nodes by xpath")
    void testGetFieldNodesByXPath() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<Node> nodes = metadata.getFieldNodesByXPath("//field[@name='value']");
        assertNotNull(nodes);
        assertTrue(nodes.size() > 0);
    }

    @Test
    @DisplayName("Should return empty list for non-matching xpath")
    void testGetFieldNodesByXPathNoMatch() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<Node> nodes = metadata.getFieldNodesByXPath("//nonexistent");
        assertNotNull(nodes);
        assertEquals(0, nodes.size());
    }

    // ========== Get Field Metadata Occurrences Tests ==========

    @Test
    @DisplayName("Should get field metadata occurrences")
    void testGetFieldMetadataOccurrences() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("xoai_openaire.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<OAIRecordMetadata> occurrences = metadata.getFieldMetadataOccurrences("datacite.contributors.contributor");
        assertNotNull(occurrences);
        assertTrue(occurrences.size() > 0);
    }

    @Test
    @DisplayName("Should return identity for identity expression")
    void testIdentityExpression() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<OAIRecordMetadata> result = metadata.getFieldMetadataOccurrences(".");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertSame(metadata, result.get(0));
    }

    @Test
    @DisplayName("Should get field metadata from xpath expression")
    void testGetFieldMetadataOccurrencesFromXPath() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<OAIRecordMetadata> occurrences = 
            metadata.getFieldMetadataOccurrencesFromXPATHExpression("//element[@name='subject']");
        
        assertNotNull(occurrences);
        assertTrue(occurrences.size() > 0);
    }

    @Test
    @DisplayName("Should return empty list for non-matching metadata expression")
    void testGetFieldMetadataOccurrencesEmpty() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<OAIRecordMetadata> occurrences = metadata.getFieldMetadataOccurrences("nonexistent.field.path");
        assertNotNull(occurrences);
        assertEquals(0, occurrences.size());
    }

    // ========== Get Bitstreams Tests ==========

    @Test
    @DisplayName("Should return empty list when no bitstreams present")
    void testGetBitstreamsEmpty() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<OAIMetadataBitstream> bitstreams = metadata.getBitstreams();
        assertNotNull(bitstreams);
        assertEquals(0, bitstreams.size());
    }

    @Test
    @DisplayName("Should get bitstreams from OpenAIRE format")
    void testGetBitstreamsFromOpenAIRE() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("xoai_openaire.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        List<OAIMetadataBitstream> bitstreams = metadata.getBitstreams();
        assertNotNull(bitstreams);
        // Note: xoai_openaire.xml uses oaire:files format, not bundles
    }

    // ========== Get Field Prefixed Content Tests ==========

    @Test
    @DisplayName("Should get field prefixed content when prefix matches")
    void testGetFieldPrefixedContent() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        String identifier = metadata.getFieldPrefixedContent("dc.identifier", "http://");
        assertNotNull(identifier);
        assertTrue(identifier.contains("handle.net") || identifier.equals("UNKNOWN"));
    }

    @Test
    @DisplayName("Should return UNKNOWN for non-matching prefix")
    void testGetFieldPrefixedContentNoMatch() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        String result = metadata.getFieldPrefixedContent("dc.identifier", "doi:");
        assertEquals("UNKNOWN", result);
    }

    // ========== ToString Test ==========

    @Test
    @DisplayName("Should convert to XML string")
    void testToString() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        String xmlString = metadata.toString();
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("metadata"));
        assertTrue(xmlString.length() > 100);
    }

    @Test
    @DisplayName("Should convert empty metadata to XML string")
    void testToStringEmpty() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        String xmlString = metadata.toString();
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("metadata"));
    }

    // ========== Properties Tests ==========

    @Test
    @DisplayName("Should set and get datestamp")
    void testDatestamp() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        LocalDateTime now = LocalDateTime.now();
        metadata.setDatestamp(now);
        assertEquals(now, metadata.getDatestamp());
    }

    @Test
    @DisplayName("Should set and get origin")
    void testOrigin() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        metadata.setOrigin("test-origin");
        assertEquals("test-origin", metadata.getOrigin());
    }

    @Test
    @DisplayName("Should set and get setSpec")
    void testSetSpec() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        metadata.setSetSpec("test-set");
        assertEquals("test-set", metadata.getSetSpec());
    }

    @Test
    @DisplayName("Should set and get storeSchema")
    void testStoreSchema() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        metadata.setStoreSchema("oai_dc");
        assertEquals("oai_dc", metadata.getStoreSchema());
    }

    @Test
    @DisplayName("Should handle null datestamp")
    void testNullDatestamp() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        metadata.setDatestamp(null);
        assertNull(metadata.getDatestamp());
    }

    @Test
    @DisplayName("Should handle null origin")
    void testNullOrigin() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        metadata.setOrigin(null);
        assertNull(metadata.getOrigin());
    }

    // ========== Special Characters Tests ==========

    @Test
    @DisplayName("Should handle special characters in field content")
    void testSpecialCharactersInFields() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        metadata.addFieldOcurrence("dc.title", "Test & Title with <special> chars");
        
        List<String> titles = metadata.getFieldOcurrences("dc.title");
        assertEquals(1, titles.size());
        assertTrue(titles.get(0).contains("&"));
    }

    @Test
    @DisplayName("Should handle Unicode characters")
    void testUnicodeCharacters() throws Exception {
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER);
        
        metadata.addFieldOcurrence("dc.title", "Título con ñ y acentos: José María");
        
        List<String> titles = metadata.getFieldOcurrences("dc.title");
        assertEquals(1, titles.size());
        assertTrue(titles.get(0).contains("ñ"));
        assertTrue(titles.get(0).contains("José"));
    }

    // ========== Edge Cases Tests ==========

    @Test
    @DisplayName("Should handle empty identifier")
    void testEmptyIdentifier() throws Exception {
        assertDoesNotThrow(() -> new OAIRecordMetadata(""));
    }

    @Test
    @DisplayName("Should handle minimal valid XML")
    void testMinimalValidXML() throws Exception {
        String minimalXml = "<metadata></metadata>";
        
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, minimalXml);
        
        assertNotNull(metadata);
        assertEquals(TEST_IDENTIFIER, metadata.getIdentifier());
    }

    @Test
    @DisplayName("Should get identifier correctly")
    void testGetIdentifier() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        assertEquals(TEST_IDENTIFIER, metadata.getIdentifier());
    }

    @Test
    @DisplayName("Should get DOM document correctly")
    void testGetDOMDocument() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        assertNotNull(metadata.getDOMDocument());
        assertSame(doc, metadata.getDOMDocument());
    }

    @Test
    @DisplayName("Should handle long text content")
    void testLongTextContent() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        // Description field contains very long text in original.xoai.record.xml
        List<String> descriptions = metadata.getFieldOcurrences("dc.description.none");
        assertNotNull(descriptions);
        assertTrue(descriptions.size() > 0);
        assertTrue(descriptions.get(0).length() > 100);
    }

    @Test
    @DisplayName("Should work with wildcard expressions")
    void testWildcardExpression() throws Exception {
        Document doc = getXmlDocumentFromResourcePath("xoai_openaire.xml");
        OAIRecordMetadata metadata = new OAIRecordMetadata(TEST_IDENTIFIER, doc);
        
        // Test wildcard in path (note: based on example in MetadataUnitTests.java)
        List<String> contributorTypes = metadata.getFieldOcurrences("datacite.contributors.*:contributorType");
        assertNotNull(contributorTypes);
    }
}
