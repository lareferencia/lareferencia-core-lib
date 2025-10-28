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
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MDFormatTransformerService Tests")
class MDFormatTransformerServiceTest {

    private MDFormatTransformerService service;
    private Document testDocument;
    
    @BeforeEach
    void setUp() throws Exception {
        service = new MDFormatTransformerService();
        
        // Create a simple test document
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        testDocument = builder.newDocument();
        testDocument.appendChild(testDocument.createElement("test"));
    }
    
    // Constructor tests
    
    @Test
    @DisplayName("Constructor should initialize empty transformer map")
    void testConstructor() {
        MDFormatTransformerService newService = new MDFormatTransformerService();
        assertNotNull(newService);
        assertTrue(newService.getSourceMetadataFormats().isEmpty());
    }
    
    // setTransformers tests
    
    @Test
    @DisplayName("setTransformers should register single transformer")
    void testSetTransformersSingle() {
        IMDFormatTransformer transformer = new MockTransformer("oai_dc", "xoai");
        
        service.setTransformers(Collections.singletonList(transformer));
        
        List<String> sources = service.getSourceMetadataFormats();
        assertEquals(1, sources.size());
        assertTrue(sources.contains("oai_dc"));
    }
    
    @Test
    @DisplayName("setTransformers should register multiple transformers")
    void testSetTransformersMultiple() {
        IMDFormatTransformer trf1 = new MockTransformer("oai_dc", "xoai");
        IMDFormatTransformer trf2 = new MockTransformer("oai_dc", "datacite");
        IMDFormatTransformer trf3 = new MockTransformer("marc21", "oai_dc");
        
        service.setTransformers(Arrays.asList(trf1, trf2, trf3));
        
        List<String> sources = service.getSourceMetadataFormats();
        assertEquals(2, sources.size());
        assertTrue(sources.contains("oai_dc"));
        assertTrue(sources.contains("marc21"));
    }
    
    @Test
    @DisplayName("setTransformers should handle same source different targets")
    void testSetTransformersSameSourceDifferentTargets() {
        IMDFormatTransformer trf1 = new MockTransformer("oai_dc", "xoai");
        IMDFormatTransformer trf2 = new MockTransformer("oai_dc", "datacite");
        IMDFormatTransformer trf3 = new MockTransformer("oai_dc", "marc21");
        
        service.setTransformers(Arrays.asList(trf1, trf2, trf3));
        
        assertDoesNotThrow(() -> {
            service.getMDTransformer("oai_dc", "xoai");
            service.getMDTransformer("oai_dc", "datacite");
            service.getMDTransformer("oai_dc", "marc21");
        });
    }
    
    @Test
    @DisplayName("setTransformers should replace transformer with same source and target")
    void testSetTransformersReplacement() {
        IMDFormatTransformer trf1 = new MockTransformer("oai_dc", "xoai");
        service.setTransformers(Collections.singletonList(trf1));
        
        IMDFormatTransformer trf2 = new MockTransformer("oai_dc", "xoai");
        service.setTransformers(Collections.singletonList(trf2));
        
        IMDFormatTransformer retrieved = assertDoesNotThrow(() -> 
            service.getMDTransformer("oai_dc", "xoai"));
        assertSame(trf2, retrieved);
    }
    
    @Test
    @DisplayName("setTransformers should handle empty list")
    void testSetTransformersEmpty() {
        service.setTransformers(Collections.emptyList());
        assertTrue(service.getSourceMetadataFormats().isEmpty());
    }
    
    // getSourceMetadataFormats tests
    
    @Test
    @DisplayName("getSourceMetadataFormats should return empty list initially")
    void testGetSourceMetadataFormatsEmpty() {
        List<String> sources = service.getSourceMetadataFormats();
        assertNotNull(sources);
        assertTrue(sources.isEmpty());
    }
    
    @Test
    @DisplayName("getSourceMetadataFormats should return all source formats")
    void testGetSourceMetadataFormatsMultiple() {
        service.setTransformers(Arrays.asList(
            new MockTransformer("oai_dc", "xoai"),
            new MockTransformer("marc21", "oai_dc"),
            new MockTransformer("datacite", "oai_dc")
        ));
        
        List<String> sources = service.getSourceMetadataFormats();
        assertEquals(3, sources.size());
        assertTrue(sources.contains("oai_dc"));
        assertTrue(sources.contains("marc21"));
        assertTrue(sources.contains("datacite"));
    }
    
    @Test
    @DisplayName("getSourceMetadataFormats should return unique sources only")
    void testGetSourceMetadataFormatsUnique() {
        service.setTransformers(Arrays.asList(
            new MockTransformer("oai_dc", "xoai"),
            new MockTransformer("oai_dc", "datacite"),
            new MockTransformer("oai_dc", "marc21")
        ));
        
        List<String> sources = service.getSourceMetadataFormats();
        assertEquals(1, sources.size());
        assertEquals("oai_dc", sources.get(0));
    }
    
    // getMDTransformer tests
    
    @Test
    @DisplayName("getMDTransformer should return registered transformer")
    void testGetMDTransformerSuccess() {
        IMDFormatTransformer transformer = new MockTransformer("oai_dc", "xoai");
        service.setTransformers(Collections.singletonList(transformer));
        
        IMDFormatTransformer retrieved = assertDoesNotThrow(() -> 
            service.getMDTransformer("oai_dc", "xoai"));
        assertSame(transformer, retrieved);
    }
    
    @Test
    @DisplayName("getMDTransformer should throw exception for non-existent source format")
    void testGetMDTransformerNonExistentSource() {
        service.setTransformers(Collections.singletonList(
            new MockTransformer("oai_dc", "xoai")));
        
        MDFormatTranformationException exception = assertThrows(
            MDFormatTranformationException.class,
            () -> service.getMDTransformer("marc21", "xoai")
        );
        assertTrue(exception.getMessage().contains("No existe transformador"));
        assertTrue(exception.getMessage().contains("marc21"));
        assertTrue(exception.getMessage().contains("xoai"));
    }
    
    @Test
    @DisplayName("getMDTransformer should throw exception for non-existent target format")
    void testGetMDTransformerNonExistentTarget() {
        service.setTransformers(Collections.singletonList(
            new MockTransformer("oai_dc", "xoai")));
        
        MDFormatTranformationException exception = assertThrows(
            MDFormatTranformationException.class,
            () -> service.getMDTransformer("oai_dc", "datacite")
        );
        assertTrue(exception.getMessage().contains("No existe transformador"));
        assertTrue(exception.getMessage().contains("oai_dc"));
        assertTrue(exception.getMessage().contains("datacite"));
    }
    
    @Test
    @DisplayName("getMDTransformer should throw exception when no transformers registered")
    void testGetMDTransformerNoTransformers() {
        MDFormatTranformationException exception = assertThrows(
            MDFormatTranformationException.class,
            () -> service.getMDTransformer("oai_dc", "xoai")
        );
        assertTrue(exception.getMessage().contains("No existe transformador"));
    }
    
    @Test
    @DisplayName("getMDTransformer should handle null source gracefully")
    void testGetMDTransformerNullSource() {
        service.setTransformers(Collections.singletonList(
            new MockTransformer("oai_dc", "xoai")));
        
        MDFormatTranformationException exception = assertThrows(
            MDFormatTranformationException.class,
            () -> service.getMDTransformer(null, "xoai")
        );
        assertTrue(exception.getMessage().contains("No existe transformador"));
    }
    
    @Test
    @DisplayName("getMDTransformer should handle null target gracefully")
    void testGetMDTransformerNullTarget() {
        service.setTransformers(Collections.singletonList(
            new MockTransformer("oai_dc", "xoai")));
        
        MDFormatTranformationException exception = assertThrows(
            MDFormatTranformationException.class,
            () -> service.getMDTransformer("oai_dc", null)
        );
        assertTrue(exception.getMessage().contains("No existe transformador"));
    }
    
    // transform tests
    
    @Test
    @DisplayName("transform should delegate to transformer and return Document")
    void testTransformSuccess() throws Exception {
        MockTransformer transformer = new MockTransformer("oai_dc", "xoai");
        service.setTransformers(Collections.singletonList(transformer));
        
        Document result = service.transform("oai_dc", "xoai", testDocument);
        
        assertNotNull(result);
        assertTrue(transformer.transformCalled);
        assertSame(testDocument, transformer.lastDocument);
    }
    
    @Test
    @DisplayName("transform should throw exception when transformer not found")
    void testTransformNoTransformer() {
        assertThrows(MDFormatTranformationException.class,
            () -> service.transform("oai_dc", "xoai", testDocument));
    }
    
    @Test
    @DisplayName("transform should propagate transformer exceptions")
    void testTransformPropagatesException() {
        MockTransformer transformer = new MockTransformer("oai_dc", "xoai");
        transformer.shouldThrowOnTransform = true;
        service.setTransformers(Collections.singletonList(transformer));
        
        assertThrows(MDFormatTranformationException.class,
            () -> service.transform("oai_dc", "xoai", testDocument));
    }
    
    // transformToString tests
    
    @Test
    @DisplayName("transformToString should delegate to transformer and return String")
    void testTransformToStringSuccess() throws Exception {
        MockTransformer transformer = new MockTransformer("oai_dc", "xoai");
        service.setTransformers(Collections.singletonList(transformer));
        
        String result = service.transformToString("oai_dc", "xoai", testDocument);
        
        assertNotNull(result);
        assertEquals("<transformed/>", result);
        assertTrue(transformer.transformToStringCalled);
        assertSame(testDocument, transformer.lastDocument);
    }
    
    @Test
    @DisplayName("transformToString should throw exception when transformer not found")
    void testTransformToStringNoTransformer() {
        assertThrows(MDFormatTranformationException.class,
            () -> service.transformToString("oai_dc", "xoai", testDocument));
    }
    
    @Test
    @DisplayName("transformToString should propagate transformer exceptions")
    void testTransformToStringPropagatesException() {
        MockTransformer transformer = new MockTransformer("oai_dc", "xoai");
        transformer.shouldThrowOnTransformToString = true;
        service.setTransformers(Collections.singletonList(transformer));
        
        assertThrows(MDFormatTranformationException.class,
            () -> service.transformToString("oai_dc", "xoai", testDocument));
    }
    
    // Integration tests
    
    @Test
    @DisplayName("Should handle multiple transformers with different source-target pairs")
    void testMultipleTransformersIntegration() throws Exception {
        MockTransformer trf1 = new MockTransformer("oai_dc", "xoai");
        MockTransformer trf2 = new MockTransformer("oai_dc", "datacite");
        MockTransformer trf3 = new MockTransformer("marc21", "oai_dc");
        
        service.setTransformers(Arrays.asList(trf1, trf2, trf3));
        
        service.transform("oai_dc", "xoai", testDocument);
        assertTrue(trf1.transformCalled);
        assertFalse(trf2.transformCalled);
        assertFalse(trf3.transformCalled);
        
        trf1.transformCalled = false;
        service.transform("oai_dc", "datacite", testDocument);
        assertFalse(trf1.transformCalled);
        assertTrue(trf2.transformCalled);
        assertFalse(trf3.transformCalled);
        
        trf2.transformCalled = false;
        service.transform("marc21", "oai_dc", testDocument);
        assertFalse(trf1.transformCalled);
        assertFalse(trf2.transformCalled);
        assertTrue(trf3.transformCalled);
    }
    
    // Mock transformer class for testing
    
    private static class MockTransformer implements IMDFormatTransformer {
        private final String sourceMDFormat;
        private final String targetMDFormat;
        
        boolean transformCalled = false;
        boolean transformToStringCalled = false;
        boolean shouldThrowOnTransform = false;
        boolean shouldThrowOnTransformToString = false;
        Document lastDocument = null;
        
        public MockTransformer(String source, String target) {
            this.sourceMDFormat = source;
            this.targetMDFormat = target;
        }
        
        @Override
        public String getSourceMDFormat() {
            return sourceMDFormat;
        }
        
        @Override
        public String getTargetMDFormat() {
            return targetMDFormat;
        }
        
        @Override
        public Document transform(Document source) throws MDFormatTranformationException {
            transformCalled = true;
            lastDocument = source;
            if (shouldThrowOnTransform) {
                throw new MDFormatTranformationException("Mock transform error");
            }
            try {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document result = builder.newDocument();
                result.appendChild(result.createElement("transformed"));
                return result;
            } catch (Exception e) {
                throw new MDFormatTranformationException("Error creating mock document", e);
            }
        }
        
        @Override
        public String transformToString(Document source) throws MDFormatTranformationException {
            transformToStringCalled = true;
            lastDocument = source;
            if (shouldThrowOnTransformToString) {
                throw new MDFormatTranformationException("Mock transformToString error");
            }
            return "<transformed/>";
        }
        
        @Override
        public void setParameter(String name, String value) {
            // Not used in these tests
        }
        
        @Override
        public void setParameter(String name, List<String> values) {
            // Not used in these tests
        }
    }
}
