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

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MapAttributeConverter Tests")
class MapAttributeConverterTest {

    private MapAttributeConverter converter;

    @BeforeEach
    void setUp() {
        converter = new MapAttributeConverter();
    }

    @Test
    @DisplayName("Should convert map to JSON string")
    void testConvertToDatabaseColumn() {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 42);
        
        String json = converter.convertToDatabaseColumn(map);
        
        assertNotNull(json);
        assertTrue(json.contains("\"key1\""));
        assertTrue(json.contains("\"value1\""));
        assertTrue(json.contains("\"key2\""));
    }

    @Test
    @DisplayName("Should convert empty map to JSON")
    void testConvertEmptyMap() {
        Map<String, Object> map = new HashMap<>();
        
        String json = converter.convertToDatabaseColumn(map);
        
        assertNotNull(json);
        assertEquals("{}", json);
    }

    @Test
    @DisplayName("Should handle null map")
    void testConvertNullMap() {
        String json = converter.convertToDatabaseColumn(null);
        
        // Implementation returns "null" string instead of null
        assertEquals("null", json);
    }

    @Test
    @DisplayName("Should convert JSON string to map")
    void testConvertToEntityAttribute() {
        String json = "{\"key1\":\"value1\",\"key2\":42}";
        
        Map<String, Object> map = converter.convertToEntityAttribute(json);
        
        assertNotNull(map);
        assertEquals("value1", map.get("key1"));
        assertEquals(42, map.get("key2"));
    }

    @Test
    @DisplayName("Should convert null JSON to empty map")
    void testConvertNullJSON() {
        Map<String, Object> map = converter.convertToEntityAttribute(null);
        
        assertNotNull(map);
        assertTrue(map.isEmpty());
    }

    @Test
    @DisplayName("Should convert empty JSON to empty map")
    void testConvertEmptyJSON() {
        Map<String, Object> map = converter.convertToEntityAttribute("{}");
        
        assertNotNull(map);
        assertTrue(map.isEmpty());
    }

    @Test
    @DisplayName("Should preserve data through round-trip")
    void testRoundTrip() {
        Map<String, Object> original = new HashMap<>();
        original.put("name", "test");
        original.put("count", 10);
        original.put("active", true);
        
        String json = converter.convertToDatabaseColumn(original);
        Map<String, Object> restored = converter.convertToEntityAttribute(json);
        
        assertEquals(original.get("name"), restored.get("name"));
        assertEquals(original.get("count"), restored.get("count"));
        assertEquals(original.get("active"), restored.get("active"));
    }

    @Test
    @DisplayName("Should handle nested maps")
    void testNestedMaps() {
        Map<String, Object> inner = new HashMap<>();
        inner.put("nested", "value");
        
        Map<String, Object> outer = new HashMap<>();
        outer.put("key", "value");
        outer.put("inner", inner);
        
        String json = converter.convertToDatabaseColumn(outer);
        Map<String, Object> restored = converter.convertToEntityAttribute(json);
        
        assertNotNull(restored);
        assertEquals("value", restored.get("key"));
        assertTrue(restored.containsKey("inner"));
    }

    @Test
    @DisplayName("Should handle map with list values")
    void testMapWithListValues() {
        Map<String, Object> map = new HashMap<>();
        map.put("items", Arrays.asList("a", "b", "c"));
        
        String json = converter.convertToDatabaseColumn(map);
        Map<String, Object> restored = converter.convertToEntityAttribute(json);
        
        assertNotNull(restored);
        assertTrue(restored.get("items") instanceof List);
    }

    @Test
    @DisplayName("Should handle invalid JSON gracefully")
    void testInvalidJSON() {
        String invalidJson = "not valid json";
        
        Map<String, Object> result = converter.convertToEntityAttribute(invalidJson);
        
        // Should log error and return null
        assertNull(result);
    }

    @Test
    @DisplayName("Should handle various data types")
    void testVariousDataTypes() {
        Map<String, Object> map = new HashMap<>();
        map.put("string", "text");
        map.put("integer", 100);
        map.put("double", 3.14);
        map.put("boolean", true);
        
        String json = converter.convertToDatabaseColumn(map);
        Map<String, Object> restored = converter.convertToEntityAttribute(json);
        
        assertNotNull(restored);
        assertEquals(4, restored.size());
    }
}
