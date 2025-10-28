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
import org.lareferencia.core.metadata.OAIMetadataElement.Type;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("XOAIXPATHHelper Tests")
class XOAIXPATHHelperTest {

    private static final String XPATH_ROOT = "/*[local-name()='metadata']";

    // ========== Root XPATH Tests ==========

    @Test
    @DisplayName("Should return correct root XPATH")
    void testGetRootXPATH() {
        String root = XOAIXPATHHelper.getRootXPATH();
        assertNotNull(root);
        assertEquals(XPATH_ROOT, root);
    }

    // ========== Basic Path Conversion Tests ==========

    @Test
    @DisplayName("Should convert simple path to XPATH")
    void testSimplePathConversion() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.title");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='metadata']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='title']"));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    @Test
    @DisplayName("Should convert nested path to XPATH")
    void testNestedPathConversion() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.subject.none");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='subject']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='none']"));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    @Test
    @DisplayName("Should handle empty path")
    void testEmptyPath() {
        String xpath = XOAIXPATHHelper.getXPATH("");
        assertNotNull(xpath);
        assertTrue(xpath.contains(XPATH_ROOT));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    // ========== Field Separator (Colon) Tests ==========

    @Test
    @DisplayName("Should handle field separator with custom field name")
    void testFieldSeparatorWithCustomField() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.title:language");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='title']"));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='language']"));
        assertFalse(xpath.contains("@name='value']"));
    }

    @Test
    @DisplayName("Should handle field separator in nested path")
    void testFieldSeparatorInNestedPath() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.publisher.none:value");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='publisher']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='none']"));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    @Test
    @DisplayName("Should default to 'value' field when no colon separator")
    void testDefaultValueField() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.type");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    // ========== Wildcard Tests ==========

    @Test
    @DisplayName("Should handle wildcard in path")
    void testWildcardInPath() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.*");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertTrue(xpath.contains("//*[local-name()='element']"));
    }

    @Test
    @DisplayName("Should handle wildcard with field separator")
    void testWildcardWithFieldSeparator() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.*:language");
        assertNotNull(xpath);
        assertTrue(xpath.contains("//*[local-name()='element']"));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='language']"));
    }

    // ========== Dollar Sign ($) Tests ==========

    @Test
    @DisplayName("Should truncate at dollar sign")
    void testDollarSignTruncation() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.$");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertFalse(xpath.contains("/*[local-name()='field'"));
    }

    @Test
    @DisplayName("Should truncate at dollar sign in nested path")
    void testDollarSignInNestedPath() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.subject.$");
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='element' and @name='dc']"));
        assertTrue(xpath.contains("/*[local-name()='element' and @name='subject']"));
        assertFalse(xpath.contains("/*[local-name()='field'"));
    }

    // ========== Include/Exclude Field Nodes Tests ==========

    @Test
    @DisplayName("Should include field nodes when includeFieldNodes is true")
    void testIncludeFieldNodes() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.title", true, true);
        assertNotNull(xpath);
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    @Test
    @DisplayName("Should exclude field nodes when includeFieldNodes is false")
    void testExcludeFieldNodes() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.title", false, true);
        assertNotNull(xpath);
        assertFalse(xpath.contains("/*[local-name()='field'"));
    }

    // ========== Include/Exclude Document Root Tests ==========

    @Test
    @DisplayName("Should include document root when includeDocumentRoot is true")
    void testIncludeDocumentRoot() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.title", true, true);
        assertNotNull(xpath);
        assertTrue(xpath.startsWith(XPATH_ROOT));
    }

    @Test
    @DisplayName("Should exclude document root when includeDocumentRoot is false")
    void testExcludeDocumentRoot() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.title", true, false);
        assertNotNull(xpath);
        assertFalse(xpath.contains(XPATH_ROOT));
        assertTrue(xpath.startsWith("/"));
    }

    // ========== XPath List Tests ==========

    @Test
    @DisplayName("Should return list of metadata elements for simple path")
    void testGetXPATHListSimplePath() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("dc.title");
        assertNotNull(elements);
        assertEquals(3, elements.size()); // dc, title, field(value)
        
        assertEquals("dc", elements.get(0).name);
        assertEquals(Type.element, elements.get(0).type);
        
        assertEquals("title", elements.get(1).name);
        assertEquals(Type.element, elements.get(1).type);
        
        assertEquals("value", elements.get(2).name);
        assertEquals(Type.field, elements.get(2).type);
    }

    @Test
    @DisplayName("Should return list of metadata elements for nested path")
    void testGetXPATHListNestedPath() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("dc.subject.none");
        assertNotNull(elements);
        assertEquals(4, elements.size()); // dc, subject, none, field(value)
        
        assertEquals("dc", elements.get(0).name);
        assertEquals("subject", elements.get(1).name);
        assertEquals("none", elements.get(2).name);
        assertEquals("value", elements.get(3).name);
        assertEquals(Type.field, elements.get(3).type);
    }

    @Test
    @DisplayName("Should return list with custom field name")
    void testGetXPATHListCustomField() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("dc.title:language");
        assertNotNull(elements);
        assertEquals(3, elements.size());
        
        assertEquals("dc", elements.get(0).name);
        assertEquals("title", elements.get(1).name);
        assertEquals("language", elements.get(2).name);
        assertEquals(Type.field, elements.get(2).type);
    }

    @Test
    @DisplayName("Should handle wildcard in list")
    void testGetXPATHListWithWildcard() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("dc.*");
        assertNotNull(elements);
        assertEquals(3, elements.size());
        
        assertEquals("dc", elements.get(0).name);
        assertEquals("*", elements.get(1).name);
        assertEquals(Type.element, elements.get(1).type);
        assertTrue(elements.get(1).xpath.contains("//*[local-name()='element']"));
    }

    @Test
    @DisplayName("Should handle dollar sign in list")
    void testGetXPATHListWithDollarSign() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("dc.subject.$");
        assertNotNull(elements);
        // Dollar sign truncates, so we only get dc, subject, and the $ element
        assertEquals(3, elements.size());
        
        assertEquals("dc", elements.get(0).name);
        assertEquals("subject", elements.get(1).name);
        assertEquals("$", elements.get(2).name);
    }

    @Test
    @DisplayName("Should return list with one element for empty path")
    void testGetXPATHListEmptyPath() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("");
        assertNotNull(elements);
        assertEquals(1, elements.size()); // Only field(value)
        
        assertEquals("value", elements.get(0).name);
        assertEquals(Type.field, elements.get(0).type);
    }

    // ========== XPath Structure Tests ==========

    @Test
    @DisplayName("Should build cumulative XPath for each element")
    void testCumulativeXPath() {
        List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList("dc.title.none");
        
        // Each element should have cumulative xpath
        assertTrue(elements.get(0).xpath.contains("@name='dc'"));
        assertTrue(elements.get(1).xpath.contains("@name='dc'"));
        assertTrue(elements.get(1).xpath.contains("@name='title'"));
        assertTrue(elements.get(2).xpath.contains("@name='dc'"));
        assertTrue(elements.get(2).xpath.contains("@name='title'"));
        assertTrue(elements.get(2).xpath.contains("@name='none'"));
    }

    @Test
    @DisplayName("Should handle complex path with all features")
    void testComplexPath() {
        String xpath = XOAIXPATHHelper.getXPATH("dc.contributor.author:name");
        assertNotNull(xpath);
        assertTrue(xpath.contains(XPATH_ROOT));
        assertTrue(xpath.contains("@name='dc'"));
        assertTrue(xpath.contains("@name='contributor'"));
        assertTrue(xpath.contains("@name='author'"));
        assertTrue(xpath.contains("@name='name'"));
        assertFalse(xpath.contains("@name='value'"));
    }

    // ========== Edge Cases ==========

    @Test
    @DisplayName("Should handle single element path")
    void testSingleElementPath() {
        String xpath = XOAIXPATHHelper.getXPATH("dc");
        assertNotNull(xpath);
        assertTrue(xpath.contains("@name='dc'"));
        assertTrue(xpath.contains("/*[local-name()='field' and @name='value']"));
    }

    @Test
    @DisplayName("Should handle path with only field separator")
    void testOnlyFieldSeparator() {
        String xpath = XOAIXPATHHelper.getXPATH(":custom");
        assertNotNull(xpath);
        assertTrue(xpath.contains("@name='custom'"));
    }

    @Test
    @DisplayName("Should maintain consistent structure across different paths")
    void testConsistentStructure() {
        String xpath1 = XOAIXPATHHelper.getXPATH("dc.title");
        String xpath2 = XOAIXPATHHelper.getXPATH("dc.subject");
        
        // Both should have same structure
        assertTrue(xpath1.contains(XPATH_ROOT));
        assertTrue(xpath2.contains(XPATH_ROOT));
        assertTrue(xpath1.contains("@name='dc'"));
        assertTrue(xpath2.contains("@name='dc'"));
    }
}
