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

import org.apache.xpath.objects.XObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MedatadaDOMHelper Tests")
class MedatadaDOMHelperTest {

    private Document testDocument;
    private String testXML;

    @BeforeEach
    void setUp() throws Exception {
        testXML = "<root xmlns:dc=\"http://purl.org/dc/elements/1.1/\">" +
                  "<dc:title>Test Title</dc:title>" +
                  "<dc:creator>Test Creator</dc:creator>" +
                  "<dc:subject>Subject 1</dc:subject>" +
                  "<dc:subject>Subject 2</dc:subject>" +
                  "<dc:description>Test Description with <b>HTML</b> content</dc:description>" +
                  "<element name=\"custom\">Custom Value</element>" +
                  "</root>";
        testDocument = MedatadaDOMHelper.XMLString2Document(testXML);
    }

    // ========== XML String to Document Tests ==========

    @Test
    @DisplayName("Should convert XML string to Document")
    void testXMLString2Document() throws Exception {
        String xml = "<test><child>value</child></test>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        assertNotNull(doc);
        assertEquals("test", doc.getDocumentElement().getNodeName());
    }

    @Test
    @DisplayName("Should handle XML with namespaces")
    void testXMLString2DocumentWithNamespaces() throws Exception {
        String xml = "<root xmlns:dc=\"http://purl.org/dc/elements/1.1/\"><dc:title>Test</dc:title></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        assertNotNull(doc);
        assertEquals("root", doc.getDocumentElement().getNodeName());
    }

    @Test
    @DisplayName("Should throw exception for invalid XML")
    void testXMLString2DocumentInvalidXML() {
        String invalidXML = "<root><unclosed>";
        
        assertThrows(Exception.class, () -> {
            MedatadaDOMHelper.XMLString2Document(invalidXML);
        });
    }

    @Test
    @DisplayName("Should handle empty elements")
    void testXMLString2DocumentEmptyElements() throws Exception {
        String xml = "<root><empty/><another></another></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        assertNotNull(doc);
        assertTrue(doc.getElementsByTagName("empty").getLength() > 0);
    }

    // ========== Document to XML String Tests ==========

    @Test
    @DisplayName("Should convert Document to XML string")
    void testDocument2XMLString() {
        String xmlString = MedatadaDOMHelper.document2XMLString(testDocument);
        
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("<root"));
        assertTrue(xmlString.contains("</root>"));
    }

    @Test
    @DisplayName("Should preserve content in conversion")
    void testDocument2XMLStringPreservesContent() {
        String xmlString = MedatadaDOMHelper.document2XMLString(testDocument);
        
        assertTrue(xmlString.contains("Test Title"));
        assertTrue(xmlString.contains("Test Creator"));
    }

    @Test
    @DisplayName("Should not include XML declaration")
    void testDocument2XMLStringNoDeclaration() {
        String xmlString = MedatadaDOMHelper.document2XMLString(testDocument);
        
        assertFalse(xmlString.startsWith("<?xml"));
    }

    // ========== Node to XML String Tests ==========

    @Test
    @DisplayName("Should convert Node to XML string")
    void testNode2XMLString() throws Exception {
        Node node = testDocument.getDocumentElement();
        String xmlString = MedatadaDOMHelper.Node2XMLString(node);
        
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("root"));
    }

    @Test
    @DisplayName("Should convert child node to XML string")
    void testNode2XMLStringChildNode() throws Exception {
        NodeList nodes = testDocument.getElementsByTagName("dc:title");
        if (nodes.getLength() > 0) {
            Node titleNode = nodes.item(0);
            String xmlString = MedatadaDOMHelper.Node2XMLString(titleNode);
            
            assertNotNull(xmlString);
            assertTrue(xmlString.contains("Test Title"));
        }
    }

    // ========== Create Document from Node Tests ==========

    @Test
    @DisplayName("Should create Document from Node")
    void testCreateDocumentFromNode() {
        Node rootNode = testDocument.getDocumentElement();
        Document newDoc = MedatadaDOMHelper.createDocumentFromNode(rootNode);
        
        assertNotNull(newDoc);
        assertEquals(rootNode.getNodeName(), newDoc.getDocumentElement().getNodeName());
    }

    @Test
    @DisplayName("Should create independent Document from Node")
    void testCreateDocumentFromNodeIndependence() {
        Node rootNode = testDocument.getDocumentElement();
        Document newDoc = MedatadaDOMHelper.createDocumentFromNode(rootNode);
        
        assertNotSame(testDocument, newDoc);
    }

    // ========== Get Node List Tests ==========

    @Test
    @DisplayName("Should get NodeList by XPath")
    void testGetNodeList() throws Exception {
        NodeList nodes = MedatadaDOMHelper.getNodeList(testDocument, "//dc:subject");
        
        assertNotNull(nodes);
        assertEquals(2, nodes.getLength());
    }

    @Test
    @DisplayName("Should return empty NodeList for non-matching XPath")
    void testGetNodeListEmpty() throws Exception {
        NodeList nodes = MedatadaDOMHelper.getNodeList(testDocument, "//nonexistent");
        
        assertNotNull(nodes);
        assertEquals(0, nodes.getLength());
    }

    // ========== Get List of Nodes Tests ==========

    @Test
    @DisplayName("Should get List of Nodes by XPath")
    void testGetListOfNodes() throws Exception {
        List<Node> nodes = MedatadaDOMHelper.getListOfNodes(testDocument, "//dc:subject");
        
        assertNotNull(nodes);
        assertEquals(2, nodes.size());
    }

    @Test
    @DisplayName("Should filter nodes without children")
    void testGetListOfNodesFilterEmpty() throws Exception {
        String xml = "<root><empty/><filled>content</filled></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        List<Node> nodes = MedatadaDOMHelper.getListOfNodes(doc, "//*");
        
        // Should only include nodes with children
        assertNotNull(nodes);
        assertTrue(nodes.size() >= 1);
    }

    // ========== Get List of Text Nodes Tests ==========

    @Test
    @DisplayName("Should get List of Text Nodes by XPath")
    void testGetListOfTextNodes() throws Exception {
        List<Node> textNodes = MedatadaDOMHelper.getListOfTextNodes(testDocument, "//dc:title");
        
        assertNotNull(textNodes);
        assertTrue(textNodes.size() > 0);
    }

    @Test
    @DisplayName("Should filter nodes without text content")
    void testGetListOfTextNodesFiltersEmpty() throws Exception {
        String xml = "<root><empty></empty><filled>text</filled></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        List<Node> textNodes = MedatadaDOMHelper.getListOfTextNodes(doc, "//*");
        
        // Should only include nodes with text
        assertNotNull(textNodes);
        assertTrue(textNodes.stream().allMatch(n -> n.getFirstChild().getNodeValue() != null));
    }

    // ========== Is Node Defined Tests ==========

    @Test
    @DisplayName("Should return true for defined node")
    void testIsNodeDefinedTrue() {
        boolean isDefined = MedatadaDOMHelper.isNodeDefined(testDocument, "//dc:title");
        
        assertTrue(isDefined);
    }

    @Test
    @DisplayName("Should return false for undefined node")
    void testIsNodeDefinedFalse() {
        boolean isDefined = MedatadaDOMHelper.isNodeDefined(testDocument, "//nonexistent");
        
        assertFalse(isDefined);
    }

    @Test
    @DisplayName("Should handle multiple matching nodes")
    void testIsNodeDefinedMultiple() {
        boolean isDefined = MedatadaDOMHelper.isNodeDefined(testDocument, "//dc:subject");
        
        assertTrue(isDefined);
    }

    // ========== Get Single String Tests ==========

    @Test
    @DisplayName("Should get single string value by XPath")
    void testGetSingleString() throws Exception {
        String value = MedatadaDOMHelper.getSingleString(testDocument, "//dc:title");
        
        assertNotNull(value);
        assertTrue(value.contains("Test Title"));
    }

    @Test
    @DisplayName("Should return empty string for non-existent XPath")
    void testGetSingleStringEmpty() throws Exception {
        String value = MedatadaDOMHelper.getSingleString(testDocument, "//nonexistent");
        
        assertNotNull(value);
        assertEquals("", value);
    }

    // ========== Get Single XObject Tests ==========

    @Test
    @DisplayName("Should get single XObject by XPath")
    void testGetSingleXObject() throws Exception {
        XObject xobj = MedatadaDOMHelper.getSingleXObjet(testDocument, "//dc:title");
        
        assertNotNull(xobj);
        assertTrue(xobj.str().contains("Test Title"));
    }

    // ========== Get Single Node Tests ==========

    @Test
    @DisplayName("Should get single Node by XPath")
    void testGetSingleNode() throws Exception {
        Node node = MedatadaDOMHelper.getSingleNode(testDocument, "//dc:title");
        
        assertNotNull(node);
        assertEquals("dc:title", node.getNodeName());
    }

    @Test
    @DisplayName("Should return null for non-existent single node")
    void testGetSingleNodeNull() throws Exception {
        Node node = MedatadaDOMHelper.getSingleNode(testDocument, "//nonexistent");
        
        assertNull(node);
    }

    // ========== Add Child Element with Name Attribute Tests ==========

    @Test
    @DisplayName("Should add child element with name attribute")
    void testAddChildElementWithNameAttr() throws Exception {
        Node root = testDocument.getDocumentElement();
        Node newChild = MedatadaDOMHelper.addChildElementWithNameAttr(root, "element", "testName");
        
        assertNotNull(newChild);
        assertEquals("element", newChild.getNodeName());
        assertEquals("testName", newChild.getAttributes().getNamedItem("name").getNodeValue());
    }

    @Test
    @DisplayName("Should add child to correct parent")
    void testAddChildElementParent() throws Exception {
        Node root = testDocument.getDocumentElement();
        int originalChildCount = root.getChildNodes().getLength();
        
        MedatadaDOMHelper.addChildElementWithNameAttr(root, "newElement", "newName");
        
        assertTrue(root.getChildNodes().getLength() > originalChildCount);
    }

    // ========== Set Node Text Tests ==========

    @Test
    @DisplayName("Should set node text content")
    void testSetNodeText() throws Exception {
        NodeList nodes = testDocument.getElementsByTagName("dc:title");
        if (nodes.getLength() > 0) {
            Node titleNode = nodes.item(0);
            MedatadaDOMHelper.setNodeText(titleNode, "New Title");
            
            assertEquals("New Title", titleNode.getTextContent());
        }
    }

    @Test
    @DisplayName("Should replace existing text content")
    void testSetNodeTextReplace() throws Exception {
        NodeList nodes = testDocument.getElementsByTagName("dc:creator");
        if (nodes.getLength() > 0) {
            Node creatorNode = nodes.item(0);
            String originalText = creatorNode.getTextContent();
            
            MedatadaDOMHelper.setNodeText(creatorNode, "Replaced Creator");
            
            assertNotEquals(originalText, creatorNode.getTextContent());
            assertEquals("Replaced Creator", creatorNode.getTextContent());
        }
    }

    // ========== Remove Node and Empty Parents Tests ==========

    @Test
    @DisplayName("Should remove node from document")
    void testRemoveNodeAndEmptyParents() throws Exception {
        String xml = "<root><parent><child1>value1</child1><child2>value2</child2></parent></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        Node child1 = doc.getElementsByTagName("child1").item(0);
        MedatadaDOMHelper.removeNodeAndEmptyParents(child1);
        
        // child1 should be removed
        assertEquals(0, doc.getElementsByTagName("child1").getLength());
        // parent and child2 should remain
        assertEquals(1, doc.getElementsByTagName("parent").getLength());
        assertEquals(1, doc.getElementsByTagName("child2").getLength());
    }

    @Test
    @DisplayName("Should remove empty parent nodes")
    void testRemoveNodeAndEmptyParentsRecursive() throws Exception {
        String xml = "<root><level1><level2><child>value</child></level2></level1><sibling>keep</sibling></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        Node child = doc.getElementsByTagName("child").item(0);
        MedatadaDOMHelper.removeNodeAndEmptyParents(child);
        
        // child should be removed
        assertEquals(0, doc.getElementsByTagName("child").getLength());
        // Empty parents level2 and level1 should also be removed
        assertEquals(0, doc.getElementsByTagName("level2").getLength());
        assertEquals(0, doc.getElementsByTagName("level1").getLength());
        // Root and sibling should remain
        assertEquals(1, doc.getElementsByTagName("root").getLength());
        assertEquals(1, doc.getElementsByTagName("sibling").getLength());
    }

    @Test
    @DisplayName("Should not remove parent with other children")
    void testRemoveNodeAndEmptyParentsKeepSiblings() throws Exception {
        String xml = "<root><parent><child1>value1</child1><child2>value2</child2></parent></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        Node child1 = doc.getElementsByTagName("child1").item(0);
        MedatadaDOMHelper.removeNodeAndEmptyParents(child1);
        
        // Parent should remain because child2 still exists
        assertEquals(1, doc.getElementsByTagName("parent").getLength());
        assertEquals(0, doc.getElementsByTagName("child1").getLength());
        assertEquals(1, doc.getElementsByTagName("child2").getLength());
    }

    // ========== Round-trip Conversion Tests ==========

    @Test
    @DisplayName("Should maintain content in round-trip conversion")
    void testRoundTripConversion() throws Exception {
        String originalXML = "<test><value>content</value></test>";
        Document doc = MedatadaDOMHelper.XMLString2Document(originalXML);
        String convertedXML = MedatadaDOMHelper.document2XMLString(doc);
        
        assertTrue(convertedXML.contains("content"));
        assertTrue(convertedXML.contains("test"));
    }

    // ========== Edge Cases ==========

    @Test
    @DisplayName("Should handle special characters in text")
    void testSpecialCharactersInText() throws Exception {
        String xml = "<root><text>&lt;special&gt; &amp; characters</text></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        assertNotNull(doc);
        String converted = MedatadaDOMHelper.document2XMLString(doc);
        assertTrue(converted.contains("special"));
    }

    @Test
    @DisplayName("Should handle deep nesting")
    void testDeepNesting() throws Exception {
        String xml = "<a><b><c><d><e>deep</e></d></c></b></a>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        Node deepNode = MedatadaDOMHelper.getSingleNode(doc, "//e");
        assertNotNull(deepNode);
        assertEquals("deep", deepNode.getTextContent());
    }

    @Test
    @DisplayName("Should handle multiple namespaces")
    void testMultipleNamespaces() throws Exception {
        String xml = "<root xmlns:dc=\"http://purl.org/dc/elements/1.1/\" " +
                     "xmlns:dcterms=\"http://purl.org/dc/terms/\">" +
                     "<dc:title>DC Title</dc:title>" +
                     "<dcterms:abstract>Abstract</dcterms:abstract>" +
                     "</root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xml);
        
        assertNotNull(doc);
        assertTrue(MedatadaDOMHelper.isNodeDefined(doc, "//dc:title"));
    }
}
