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
import org.lareferencia.core.metadata.OAIMetadataElement.Type;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OAIMetadataElement
 */
@DisplayName("OAIMetadataElement Tests")
class OAIMetadataElementTest {

    private OAIMetadataElement element;

    @BeforeEach
    void setUp() {
        element = new OAIMetadataElement();
    }

    @Test
    @DisplayName("Should create OAIMetadataElement with default constructor")
    void testDefaultConstructor() {
        assertNotNull(element);
        assertNull(element.getName());
        assertNull(element.getType());
        assertNull(element.getXpath());
    }

    @Test
    @DisplayName("Should set and get name")
    void testNameProperty() {
        String name = "dc.title";
        element.setName(name);
        
        assertEquals(name, element.getName());
    }

    @Test
    @DisplayName("Should set and get type as element")
    void testTypeElement() {
        element.setType(Type.element);
        
        assertEquals(Type.element, element.getType());
    }

    @Test
    @DisplayName("Should set and get type as field")
    void testTypeField() {
        element.setType(Type.field);
        
        assertEquals(Type.field, element.getType());
    }

    @Test
    @DisplayName("Should set and get xpath")
    void testXpathProperty() {
        String xpath = "//dc:title";
        element.setXpath(xpath);
        
        assertEquals(xpath, element.getXpath());
    }

    @Test
    @DisplayName("Should handle null name")
    void testNullName() {
        element.setName(null);
        assertNull(element.getName());
    }

    @Test
    @DisplayName("Should handle empty name")
    void testEmptyName() {
        element.setName("");
        assertEquals("", element.getName());
    }

    @Test
    @DisplayName("Should handle null xpath")
    void testNullXpath() {
        element.setXpath(null);
        assertNull(element.getXpath());
    }

    @Test
    @DisplayName("Should handle empty xpath")
    void testEmptyXpath() {
        element.setXpath("");
        assertEquals("", element.getXpath());
    }

    @Test
    @DisplayName("Should handle complex xpath expressions")
    void testComplexXpath() {
        String complexXpath = "//oai_dc:dc/dc:subject[@type='keyword']";
        element.setXpath(complexXpath);
        
        assertEquals(complexXpath, element.getXpath());
    }

    @Test
    @DisplayName("Should set all properties together")
    void testAllProperties() {
        element.setName("dc.subject");
        element.setType(Type.field);
        element.setXpath("//dc:subject");
        
        assertEquals("dc.subject", element.getName());
        assertEquals(Type.field, element.getType());
        assertEquals("//dc:subject", element.getXpath());
    }

    @Test
    @DisplayName("Should handle name with special characters")
    void testNameWithSpecialCharacters() {
        String specialName = "dc.contributor.author";
        element.setName(specialName);
        
        assertEquals(specialName, element.getName());
    }

    @Test
    @DisplayName("Should verify Type enum has element value")
    void testTypeEnumElement() {
        Type type = Type.element;
        assertEquals("element", type.name());
    }

    @Test
    @DisplayName("Should verify Type enum has field value")
    void testTypeEnumField() {
        Type type = Type.field;
        assertEquals("field", type.name());
    }

    @Test
    @DisplayName("Should verify Type enum values")
    void testTypeEnumValues() {
        Type[] types = Type.values();
        assertEquals(2, types.length);
        assertTrue(java.util.Arrays.asList(types).contains(Type.element));
        assertTrue(java.util.Arrays.asList(types).contains(Type.field));
    }

    @Test
    @DisplayName("Should allow changing properties multiple times")
    void testPropertyChanges() {
        element.setName("first");
        element.setName("second");
        assertEquals("second", element.getName());
        
        element.setType(Type.element);
        element.setType(Type.field);
        assertEquals(Type.field, element.getType());
        
        element.setXpath("//first");
        element.setXpath("//second");
        assertEquals("//second", element.getXpath());
    }
}
