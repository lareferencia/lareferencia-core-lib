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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.w3c.dom.Document;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("XsltMDFormatTransformer Tests")
class XsltMDFormatTransformerTest {

    private XsltMDFormatTransformer transformer;
    private XsltMDFormatTransformer transformerWithParams;
    private Document testDocument;

    @BeforeEach
    void setUp() throws Exception {
        // Get test XSLT files
        String xsltPath = new ClassPathResource("test-transform.xsl").getFile().getAbsolutePath();
        String xsltParamPath = new ClassPathResource("test-transform-with-params.xsl").getFile().getAbsolutePath();
        
        // Create transformers
        transformer = new XsltMDFormatTransformer("source", "target", xsltPath);
        transformerWithParams = new XsltMDFormatTransformer("source", "target", xsltParamPath);
        
        // Create test document
        String xml = "<root><title>Test Title</title><content>Test Content</content></root>";
        testDocument = MedatadaDOMHelper.XMLString2Document(xml);
    }

    // ========== Constructor Tests ==========

    @Test
    @DisplayName("Should create transformer with valid XSLT file")
    void testConstructorWithValidXSLT() {
        assertNotNull(transformer);
        assertEquals("source", transformer.getSourceMDFormat());
        assertEquals("target", transformer.getTargetMDFormat());
    }

    @Test
    @DisplayName("Should set source and target formats correctly")
    void testGetSourceAndTargetFormats() {
        assertEquals("source", transformer.getSourceMDFormat());
        assertEquals("target", transformer.getTargetMDFormat());
    }

    // ========== Transform to String Tests ==========

    @Test
    @DisplayName("Should transform document to XML string")
    void testTransformToString() throws Exception {
        String result = transformer.transformToString(testDocument);
        
        assertNotNull(result);
        assertTrue(result.contains("processed"));
        assertTrue(result.contains("Test Title"));
        assertTrue(result.contains("Test Content"));
    }

    @Test
    @DisplayName("Should preserve content during transformation")
    void testTransformToStringPreservesContent() throws Exception {
        String result = transformer.transformToString(testDocument);
        
        assertTrue(result.contains("title"));
        assertTrue(result.contains("content"));
    }

    // ========== Transform to Document Tests ==========

    @Test
    @DisplayName("Should transform document to document")
    void testTransform() throws Exception {
        Document result = transformer.transform(testDocument);
        
        assertNotNull(result);
        assertEquals("processed", result.getDocumentElement().getNodeName());
    }

    @Test
    @DisplayName("Should produce valid DOM document")
    void testTransformProducesValidDocument() throws Exception {
        Document result = transformer.transform(testDocument);
        
        assertNotNull(result.getDocumentElement());
        assertTrue(result.getDocumentElement().hasChildNodes());
    }

    // ========== Parameter Setting Tests ==========

    @Test
    @DisplayName("Should set string parameter")
    void testSetStringParameter() throws Exception {
        transformerWithParams.setParameter("testParam", "customValue");
        String result = transformerWithParams.transformToString(testDocument);
        
        assertNotNull(result);
        assertTrue(result.contains("customValue"));
    }

    @Test
    @DisplayName("Should set list parameter")
    void testSetListParameter() throws Exception {
        List<String> values = Arrays.asList("value1", "value2", "value3");
        transformerWithParams.setParameter("testParam", values);
        
        // Should not throw exception
        Document result = transformerWithParams.transform(testDocument);
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle empty list parameter")
    void testSetEmptyListParameter() throws Exception {
        List<String> emptyList = Arrays.asList();
        transformerWithParams.setParameter("testParam", emptyList);
        
        Document result = transformerWithParams.transform(testDocument);
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle single value in list parameter")
    void testSetSingleValueListParameter() throws Exception {
        List<String> singleValue = Arrays.asList("singleValue");
        transformerWithParams.setParameter("testParam", singleValue);
        
        Document result = transformerWithParams.transform(testDocument);
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle null parameter name gracefully")
    void testSetParameterWithNullName() {
        // Should not throw exception, should log warning and ignore
        assertDoesNotThrow(() -> {
            transformerWithParams.setParameter(null, "value");
        });
    }

    @Test
    @DisplayName("Should handle null parameter value gracefully")
    void testSetParameterWithNullValue() {
        // Should not throw exception, should log warning and ignore
        assertDoesNotThrow(() -> {
            transformerWithParams.setParameter("testParam", (String) null);
        });
    }
    
    @Test
    @DisplayName("Should handle null list parameter gracefully")
    void testSetParameterWithNullList() {
        // Should not throw exception, should log warning and ignore
        assertDoesNotThrow(() -> {
            transformerWithParams.setParameter("testParam", (List<String>) null);
        });
    }

    // ========== Complex Transformation Tests ==========

    @Test
    @DisplayName("Should handle complex XML structure")
    void testComplexXMLTransformation() throws Exception {
        String complexXml = "<root>" +
                           "<metadata>" +
                           "<dc:title xmlns:dc=\"http://purl.org/dc/elements/1.1/\">Complex Title</dc:title>" +
                           "<dc:creator xmlns:dc=\"http://purl.org/dc/elements/1.1/\">Author Name</dc:creator>" +
                           "</metadata>" +
                           "</root>";
        Document complexDoc = MedatadaDOMHelper.XMLString2Document(complexXml);
        
        String result = transformer.transformToString(complexDoc);
        
        assertNotNull(result);
        assertTrue(result.contains("processed"));
    }

    @Test
    @DisplayName("Should handle XML with attributes")
    void testXMLWithAttributes() throws Exception {
        String xmlWithAttrs = "<root id=\"123\" type=\"test\"><item name=\"test\">value</item></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xmlWithAttrs);
        
        Document result = transformer.transform(doc);
        
        assertNotNull(result);
        assertEquals("processed", result.getDocumentElement().getNodeName());
    }

    @Test
    @DisplayName("Should handle empty elements")
    void testEmptyElements() throws Exception {
        String xmlWithEmpty = "<root><empty/><filled>content</filled></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xmlWithEmpty);
        
        String result = transformer.transformToString(doc);
        
        assertNotNull(result);
        assertTrue(result.length() > 0);
    }

    // ========== Round-trip Tests ==========

    @Test
    @DisplayName("Should maintain consistency in multiple transformations")
    void testMultipleTransformations() throws Exception {
        String result1 = transformer.transformToString(testDocument);
        String result2 = transformer.transformToString(testDocument);
        
        // Results should be consistent
        assertNotNull(result1);
        assertNotNull(result2);
        // Structure should be the same
        assertTrue(result1.contains("processed"));
        assertTrue(result2.contains("processed"));
    }

    @Test
    @DisplayName("Should transform and convert back to string")
    void testTransformRoundTrip() throws Exception {
        Document transformed = transformer.transform(testDocument);
        String xmlString = MedatadaDOMHelper.document2XMLString(transformed);
        
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("processed"));
    }

    // ========== Error Handling Tests ==========

    @Test
    @DisplayName("Should throw exception for non-existent XSLT file")
    void testMalformedXSLT() {
        // Constructor should throw IllegalArgumentException for non-existent file
        assertThrows(IllegalArgumentException.class, () -> {
            new XsltMDFormatTransformer("source", "target", "nonexistent.xsl");
        });
    }

    @Test
    @DisplayName("Should throw exception for invalid XSLT file path")
    void testInvalidXSLTPath() {
        assertThrows(IllegalArgumentException.class, () -> {
            new XsltMDFormatTransformer("source", "target", "/invalid/path/file.xsl");
        });
    }

    // ========== Special Characters Tests ==========

    @Test
    @DisplayName("Should handle special characters in content")
    void testSpecialCharacters() throws Exception {
        String xmlWithSpecial = "<root><text>&lt;special&gt; &amp; characters</text></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xmlWithSpecial);
        
        String result = transformer.transformToString(doc);
        
        assertNotNull(result);
        assertTrue(result.contains("special"));
    }

    @Test
    @DisplayName("Should handle unicode characters")
    void testUnicodeCharacters() throws Exception {
        String xmlWithUnicode = "<root><text>Héllo Wörld 你好 مرحبا</text></root>";
        Document doc = MedatadaDOMHelper.XMLString2Document(xmlWithUnicode);
        
        String result = transformer.transformToString(doc);
        
        assertNotNull(result);
        assertTrue(result.length() > 0);
    }
}
