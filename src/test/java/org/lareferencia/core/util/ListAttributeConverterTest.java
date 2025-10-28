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

package org.lareferencia.core.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ListAttributeConverter Tests")
class ListAttributeConverterTest {

    private ListAttributeConverter converter;

    @BeforeEach
    void setUp() {
        converter = new ListAttributeConverter();
    }

    @Test
    @DisplayName("Should convert list to JSON string")
    void testConvertToDatabaseColumn() {
        List<String> list = Arrays.asList("item1", "item2", "item3");
        
        String json = converter.convertToDatabaseColumn(list);
        
        assertNotNull(json);
        assertTrue(json.contains("\"item1\""));
        assertTrue(json.contains("\"item2\""));
        assertTrue(json.contains("\"item3\""));
    }

    @Test
    @DisplayName("Should convert empty list to JSON")
    void testConvertEmptyList() {
        List<String> list = new ArrayList<>();
        
        String json = converter.convertToDatabaseColumn(list);
        
        assertNotNull(json);
        assertEquals("[]", json);
    }

    @Test
    @DisplayName("Should handle null list")
    void testConvertNullList() {
        String json = converter.convertToDatabaseColumn(null);
        
        // Implementation returns "null" string instead of null
        assertEquals("null", json);
    }

    @Test
    @DisplayName("Should convert JSON string to list")
    void testConvertToEntityAttribute() {
        String json = "[\"item1\",\"item2\",\"item3\"]";
        
        List<String> list = converter.convertToEntityAttribute(json);
        
        assertNotNull(list);
        assertEquals(3, list.size());
        assertTrue(list.contains("item1"));
        assertTrue(list.contains("item2"));
        assertTrue(list.contains("item3"));
    }

    @Test
    @DisplayName("Should convert null JSON to empty list")
    void testConvertNullJSON() {
        List<String> list = converter.convertToEntityAttribute(null);
        
        assertNotNull(list);
        assertTrue(list.isEmpty());
    }

    @Test
    @DisplayName("Should convert empty JSON to empty list")
    void testConvertEmptyJSON() {
        List<String> list = converter.convertToEntityAttribute("[]");
        
        assertNotNull(list);
        assertTrue(list.isEmpty());
    }

    @Test
    @DisplayName("Should preserve data through round-trip")
    void testRoundTrip() {
        List<String> original = Arrays.asList("alpha", "beta", "gamma");
        
        String json = converter.convertToDatabaseColumn(original);
        List<String> restored = converter.convertToEntityAttribute(json);
        
        assertEquals(original, restored);
    }

    @Test
    @DisplayName("Should handle single item list")
    void testSingleItem() {
        List<String> list = Arrays.asList("single");
        
        String json = converter.convertToDatabaseColumn(list);
        List<String> restored = converter.convertToEntityAttribute(json);
        
        assertEquals(1, restored.size());
        assertEquals("single", restored.get(0));
    }

    @Test
    @DisplayName("Should handle list with special characters")
    void testSpecialCharacters() {
        List<String> list = Arrays.asList("item\"with'quotes", "item\nwith\nnewlines");
        
        String json = converter.convertToDatabaseColumn(list);
        List<String> restored = converter.convertToEntityAttribute(json);
        
        assertEquals(list, restored);
    }

    @Test
    @DisplayName("Should handle invalid JSON gracefully")
    void testInvalidJSON() {
        String invalidJson = "not valid json";
        
        List<String> result = converter.convertToEntityAttribute(invalidJson);
        
        // Should log error and return null
        assertNull(result);
    }

    @Test
    @DisplayName("Should handle large list")
    void testLargeList() {
        List<String> largeList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeList.add("item" + i);
        }
        
        String json = converter.convertToDatabaseColumn(largeList);
        List<String> restored = converter.convertToEntityAttribute(json);
        
        assertEquals(1000, restored.size());
        assertEquals("item0", restored.get(0));
        assertEquals("item999", restored.get(999));
    }

    @Test
    @DisplayName("Should handle UTF-8 characters")
    void testUTF8Characters() {
        List<String> list = Arrays.asList("español", "中文", "العربية");
        
        String json = converter.convertToDatabaseColumn(list);
        List<String> restored = converter.convertToEntityAttribute(json);
        
        assertEquals(list, restored);
    }
}
