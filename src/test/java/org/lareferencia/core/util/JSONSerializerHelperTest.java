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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("JSONSerializerHelper Tests")
class JSONSerializerHelperTest {

    @TempDir
    Path tempDir;

    // Test POJOs
    static class SimpleObject {
        public String name;
        public int value;
        
        public SimpleObject() {}
        
        public SimpleObject(String name, int value) {
            this.name = name;
            this.value = value;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleObject that = (SimpleObject) o;
            return value == that.value && Objects.equals(name, that.name);
        }
    }
    
    static class ComplexObject {
        public String text;
        public List<String> items;
        public Map<String, Integer> counts;
        public SimpleObject nested;
        
        public ComplexObject() {}
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ComplexObject that = (ComplexObject) o;
            return Objects.equals(text, that.text) &&
                   Objects.equals(items, that.items) &&
                   Objects.equals(counts, that.counts) &&
                   Objects.equals(nested, that.nested);
        }
    }

    // serializeToJsonString tests

    @Test
    @DisplayName("Should serialize simple object to JSON string")
    void testSerializeSimpleObject() throws JsonProcessingException {
        SimpleObject obj = new SimpleObject("test", 42);
        
        String json = JSONSerializerHelper.serializeToJsonString(obj);
        
        assertNotNull(json);
        assertTrue(json.contains("\"name\":\"test\""));
        assertTrue(json.contains("\"value\":42"));
    }

    @Test
    @DisplayName("Should serialize complex nested object")
    void testSerializeComplexObject() throws JsonProcessingException {
        ComplexObject obj = new ComplexObject();
        obj.text = "example";
        obj.items = Arrays.asList("a", "b", "c");
        obj.counts = new HashMap<>();
        obj.counts.put("x", 1);
        obj.counts.put("y", 2);
        obj.nested = new SimpleObject("nested", 99);
        
        String json = JSONSerializerHelper.serializeToJsonString(obj);
        
        assertNotNull(json);
        assertTrue(json.contains("\"text\":\"example\""));
        assertTrue(json.contains("\"items\""));
        assertTrue(json.contains("\"counts\""));
        assertTrue(json.contains("\"nested\""));
    }

    @Test
    @DisplayName("Should serialize empty object")
    void testSerializeEmptyObject() throws JsonProcessingException {
        SimpleObject obj = new SimpleObject();
        
        String json = JSONSerializerHelper.serializeToJsonString(obj);
        
        assertNotNull(json);
        assertTrue(json.contains("\"name\":null"));
        assertTrue(json.contains("\"value\":0"));
    }

    @Test
    @DisplayName("Should serialize list of objects")
    void testSerializeList() throws JsonProcessingException {
        List<SimpleObject> list = Arrays.asList(
            new SimpleObject("first", 1),
            new SimpleObject("second", 2)
        );
        
        String json = JSONSerializerHelper.serializeToJsonString(list);
        
        assertNotNull(json);
        assertTrue(json.startsWith("["));
        assertTrue(json.endsWith("]"));
        assertTrue(json.contains("\"first\""));
        assertTrue(json.contains("\"second\""));
    }

    @Test
    @DisplayName("Should serialize map")
    void testSerializeMap() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 42);
        
        String json = JSONSerializerHelper.serializeToJsonString(map);
        
        assertNotNull(json);
        assertTrue(json.contains("\"key1\":\"value1\""));
        assertTrue(json.contains("\"key2\":42"));
    }

    @Test
    @DisplayName("Should handle special characters in strings")
    void testSerializeSpecialCharacters() throws JsonProcessingException {
        SimpleObject obj = new SimpleObject("test\"with'quotes\nand\ttabs", 1);
        
        String json = JSONSerializerHelper.serializeToJsonString(obj);
        
        assertNotNull(json);
        assertTrue(json.contains("\\\""));
        assertTrue(json.contains("\\n"));
        assertTrue(json.contains("\\t"));
    }

    @Test
    @DisplayName("Should handle UTF-8 characters")
    void testSerializeUTF8() throws JsonProcessingException {
        SimpleObject obj = new SimpleObject("español ñ, 中文, العربية", 1);
        
        String json = JSONSerializerHelper.serializeToJsonString(obj);
        
        assertNotNull(json);
        assertTrue(json.contains("español"));
        assertTrue(json.contains("中文"));
    }

    // deserializeFromJsonString tests

    @Test
    @DisplayName("Should deserialize simple object from JSON string")
    void testDeserializeSimpleObject() throws JsonProcessingException {
        String json = "{\"name\":\"test\",\"value\":42}";
        
        SimpleObject obj = (SimpleObject) JSONSerializerHelper.deserializeFromJsonString(json, SimpleObject.class);
        
        assertNotNull(obj);
        assertEquals("test", obj.name);
        assertEquals(42, obj.value);
    }

    @Test
    @DisplayName("Should deserialize complex object")
    void testDeserializeComplexObject() throws JsonProcessingException {
        String json = "{\"text\":\"example\",\"items\":[\"a\",\"b\"],\"counts\":{\"x\":1},\"nested\":{\"name\":\"n\",\"value\":5}}";
        
        ComplexObject obj = (ComplexObject) JSONSerializerHelper.deserializeFromJsonString(json, ComplexObject.class);
        
        assertNotNull(obj);
        assertEquals("example", obj.text);
        assertEquals(Arrays.asList("a", "b"), obj.items);
        assertEquals(1, obj.counts.get("x"));
        assertEquals("n", obj.nested.name);
        assertEquals(5, obj.nested.value);
    }

    @Test
    @DisplayName("Should handle null values in JSON")
    void testDeserializeWithNulls() throws JsonProcessingException {
        String json = "{\"name\":null,\"value\":0}";
        
        SimpleObject obj = (SimpleObject) JSONSerializerHelper.deserializeFromJsonString(json, SimpleObject.class);
        
        assertNotNull(obj);
        assertNull(obj.name);
        assertEquals(0, obj.value);
    }

    @Test
    @DisplayName("Should throw exception for invalid JSON")
    void testDeserializeInvalidJSON() {
        String invalidJson = "{invalid json}";
        
        assertThrows(JsonProcessingException.class, () ->
            JSONSerializerHelper.deserializeFromJsonString(invalidJson, SimpleObject.class));
    }

    @Test
    @DisplayName("Should throw exception for malformed JSON")
    void testDeserializeMalformedJSON() {
        String malformedJson = "{\"name\":\"test\""; // missing closing brace
        
        assertThrows(JsonProcessingException.class, () ->
            JSONSerializerHelper.deserializeFromJsonString(malformedJson, SimpleObject.class));
    }

    // Round-trip tests

    @Test
    @DisplayName("Should preserve data through serialize-deserialize cycle")
    void testRoundTrip() throws JsonProcessingException {
        SimpleObject original = new SimpleObject("roundtrip", 123);
        
        String json = JSONSerializerHelper.serializeToJsonString(original);
        SimpleObject restored = (SimpleObject) JSONSerializerHelper.deserializeFromJsonString(json, SimpleObject.class);
        
        assertEquals(original, restored);
    }

    @Test
    @DisplayName("Should preserve complex data through round-trip")
    void testComplexRoundTrip() throws JsonProcessingException {
        ComplexObject original = new ComplexObject();
        original.text = "test";
        original.items = Arrays.asList("x", "y", "z");
        original.counts = new HashMap<>();
        original.counts.put("a", 10);
        original.nested = new SimpleObject("inner", 7);
        
        String json = JSONSerializerHelper.serializeToJsonString(original);
        ComplexObject restored = (ComplexObject) JSONSerializerHelper.deserializeFromJsonString(json, ComplexObject.class);
        
        assertEquals(original, restored);
    }

    // deserializeFromFile tests

    @Test
    @DisplayName("Should deserialize from file")
    void testDeserializeFromFile() throws IOException {
        File file = tempDir.resolve("test.json").toFile();
        String json = "{\"name\":\"fromFile\",\"value\":99}";
        Files.writeString(file.toPath(), json);
        
        SimpleObject obj = (SimpleObject) JSONSerializerHelper.deserializeFromFile(file, SimpleObject.class);
        
        assertNotNull(obj);
        assertEquals("fromFile", obj.name);
        assertEquals(99, obj.value);
    }

    @Test
    @DisplayName("Should deserialize complex object from file")
    void testDeserializeComplexFromFile() throws IOException {
        File file = tempDir.resolve("complex.json").toFile();
        ComplexObject original = new ComplexObject();
        original.text = "fileTest";
        original.items = Arrays.asList("1", "2");
        
        String json = JSONSerializerHelper.serializeToJsonString(original);
        Files.writeString(file.toPath(), json);
        
        ComplexObject obj = (ComplexObject) JSONSerializerHelper.deserializeFromFile(file, ComplexObject.class);
        
        assertEquals(original, obj);
    }

    @Test
    @DisplayName("Should throw exception for non-existent file")
    void testDeserializeNonExistentFile() {
        File nonExistent = new File("/non/existent/file.json");
        
        assertThrows(IOException.class, () ->
            JSONSerializerHelper.deserializeFromFile(nonExistent, SimpleObject.class));
    }

    @Test
    @DisplayName("Should throw exception for invalid JSON in file")
    void testDeserializeInvalidJSONFile() throws IOException {
        File file = tempDir.resolve("invalid.json").toFile();
        Files.writeString(file.toPath(), "not valid json");
        
        assertThrows(IOException.class, () ->
            JSONSerializerHelper.deserializeFromFile(file, SimpleObject.class));
    }

    @Test
    @DisplayName("Should handle large JSON file")
    void testDeserializeLargeFile() throws IOException {
        File file = tempDir.resolve("large.json").toFile();
        
        // Create large list
        List<SimpleObject> largeList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeList.add(new SimpleObject("item" + i, i));
        }
        
        String json = JSONSerializerHelper.serializeToJsonString(largeList);
        Files.writeString(file.toPath(), json);
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) 
            JSONSerializerHelper.deserializeFromFile(file, List.class);
        
        assertEquals(1000, result.size());
    }

    @Test
    @DisplayName("Should handle empty JSON object")
    void testDeserializeEmptyJSON() throws JsonProcessingException {
        String json = "{}";
        
        SimpleObject obj = (SimpleObject) JSONSerializerHelper.deserializeFromJsonString(json, SimpleObject.class);
        
        assertNotNull(obj);
        assertNull(obj.name);
        assertEquals(0, obj.value);
    }

    @Test
    @DisplayName("Should handle empty array")
    void testDeserializeEmptyArray() throws JsonProcessingException {
        String json = "[]";
        
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) JSONSerializerHelper.deserializeFromJsonString(json, List.class);
        
        assertNotNull(list);
        assertTrue(list.isEmpty());
    }
}
